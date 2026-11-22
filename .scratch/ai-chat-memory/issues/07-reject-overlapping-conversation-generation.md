# 07: 控制同一会话的并发生成

**What to build:** 让同一会话保持单个活跃生成，不同会话并行，并在进程故障后恢复生成资格。

**Blocked by:** [05 发起和继续同步对话](05-start-and-continue-sync-chat.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 扩展 05 的 generation 阶段入口，为准备、续期、终态和释放加入同一组令牌条件。
- 过期恢复调用共同失败收敛入口，后续 Worker 复用该操作。
- 生成租约保护生成提交资格，09 的任务租约保护任务执行状态，两类资源分别维护。

## 验收条件

- [x] 主动暂停模型并从独立数据库会话发起竞争，第二个不同生成立即返回 409 conversation_generation_conflict。
- [x] 竞争失败请求不追加用户消息或调用模型，不同会话可同时完成。
- [x] PostgreSQL 租约具备期限、唯一所有权令牌及续期，成功、失败和超时释放资格。
- [x] 续期、回复写入、终态与释放均校验令牌、有效期及用户和会话状态，旧执行者失去提交资格。
- [x] 新请求领取租约时若发现旧 `running` 回合租约已过期（`lease_expires_at <= now()`），在同一短事务内先将其收敛为 `failed`（`generation_lease_lost`）后直接接管，不依赖 Worker 存活；Worker 过期恢复器作为无后续新请求时的兜底。
- [x] 同步和流式共用生成入口，数据库事务保持短时，进程崩溃、续期失败及旧执行者恢复有 PostgreSQL 测试。

## Answer

### 实际公共符号与后续切片扩展入口

- **数据库约束与迁移（`migrations/versions/20261122_0006_add_generation_lease_constraints.py`、`src/mem_ai/modules/conversations/models.py`）**：
  - `GENERATION_ACTIVE_CONVERSATION_UNIQUE_INDEX`（`uq_generations_active_conversation`）：`generations(conversation_id)` 在 `WHERE status = 'running'` 上的部分唯一索引，从数据库层兜底单会话至多一个活跃生成。
  - `GENERATION_RUNNING_LEASE_EXPIRES_INDEX`（`ix_generations_running_lease_expires_at`）：`generations(lease_expires_at)` 在 `WHERE status = 'running'` 上的部分索引，供 Worker 过期租约扫描使用。
  - `ck_generations_lease_state_valid` CHECK 约束：`running` 必须持有非空 `lease_token` 与 `lease_expires_at`，终态必须清空两者以释放生成资格。
- **共享租约条件与持久化阶段（`src/mem_ai/modules/conversations/repository.py`、`src/mem_ai/modules/conversations/generation.py`）**：
  - `repository.get_database_now` / `repository.normalize_utc`：未注入测试时间时统一使用 PostgreSQL `SELECT now()` 作为跨副本权威时钟。
  - `repository._active_lease_conditions`：统一 `generation_id`/`conversation_id`/`owner_id`/`status='running'`/`lease_token`/`lease_expires_at > current_time` 令牌与有效期条件，供 `conditional_renew_generation_lease`、`conditional_mark_generation_succeeded` 与活跃执行者 `conditional_mark_generation_terminal_failure` 共同复用。
  - `repository.get_conversation_for_share`、`repository.get_running_generation_for_conversation`、`repository.list_expired_running_generations_for_update`（`FOR UPDATE SKIP LOCKED`）。
  - `generation.converge_generation_failure_in_session`（事务参与共同失败收敛入口）：
    - 活跃执行者路径（`expired_only=False`）按 `users (FOR SHARE) -> conversations (FOR SHARE) -> generations` 顺序校验拥有者、用户/会话有效状态、令牌与有效期并释放租约；当用户或会话进入 `disabled`/`deleting` 时按权威失效原因收敛为 `failed` 并立即释放租约。
    - 过期恢复路径（`expired_only=True`）由 `prepare_generation_phase` 内联接管与 `recover_expired_generations_phase` 共同调用，条件收敛 `lease_expires_at <= now()` 为 `failed`（`generation_lease_lost`）并清空租约列。
  - `generation.prepare_generation_phase`：在短事务 Tx 1 内锁 `users (FOR SHARE)` 与 `conversations (FOR UPDATE)`，存在未过期 `running` 回合时立即抛出 `409 conversation_generation_conflict`（回滚且不写入用户消息）；若旧 `running` 回合租约已过期则同事务调用 `converge_generation_failure_in_session(..., expired_only=True)` 收敛为 `failed (generation_lease_lost)` 后直接接管。
  - `generation.renew_generation_lease_phase`：短事务按 `users (FOR SHARE) -> conversations (FOR SHARE) -> generations` 复查状态、令牌与有效期并续期，失效返回 `409 generation_lease_lost` / `403 user_*` / `409 conversation_deleting`。
  - `generation.commit_generation_success_phase`：短事务 Tx 2 按 `users (FOR SHARE) -> conversations (FOR UPDATE) -> generations` 校验状态、令牌与未过期条件，原子写入完整 Assistant 回复、标记 `succeeded` 并清空租约；失效时整体回滚且不落库回复。
  - `generation.converge_generation_failure_phase`：要求传入 `owner_id` 与 `conversation_id`，委托 `converge_generation_failure_in_session` 执行活跃执行者失败/取消收敛与租约释放（供 Ticket 10/12 复用）。
  - `generation.recover_expired_generations_phase`：批量 `FOR UPDATE SKIP LOCKED` 扫描过期 `running` 回合，逐条调用 `converge_generation_failure_in_session(..., expired_only=True)`（**供 Ticket 09/17 Worker 过期恢复处理器直接复用**）。
- **用户行共享锁协作（`src/mem_ai/modules/users/repository.py`、`src/mem_ai/modules/users/service.py`）**：
  - `users_repository.get_user_by_id(..., for_share=False)` 与 `users_service.ensure_user_active_in_session(..., for_share=False)` 新增可选 `FOR SHARE` 行锁，消除 PostgreSQL `READ COMMITTED` 下并发禁用/删除用户与租约续期/最终提交之间的 TOCTOU 竞态，且不阻塞同一用户的多会话并行。
- **共享续期心跳与编排（`src/mem_ai/modules/conversations/service.py`、`src/mem_ai/core/config.py`）**：
  - `GenerationLeaseHeartbeat`：可复用的异步上下文管理器（**供同步路径与 Ticket 12 SSE 流式路径共用**），在事务外模型执行期间按 `lease_renew_interval_seconds` 定期调用 `renew_generation_lease_phase`；遇到瞬态异常记告警并在租约期内重试，遇到确定性失活或 `generation_lease_lost` 立即触发 `on_lease_lost` 取消在途模型调用并在 Tx 2 前优雅停止。
  - `SyncChatOptions`、`sync_chat_options_from_settings`、`complete_prepared_sync_generation` 与 `run_sync_chat` 接入 `lease_ttl_seconds` 与 `lease_renew_interval_seconds`。
  - `Settings.chat_lease_renew_interval_seconds`（默认 `20.0` 秒）及启动边界校验（严格小于 `chat_lease_ttl_seconds`），同步 `.env.example`。

### 复用、迁移与职责分工

- 直接扩展 05 建立的 `conversations/{models,repository,generation,service}.py`，未引入重复生成入口或额外锁表；生成租约留在 `generations` 表，与 09 的 `jobs` 任务租约严格分离。
- 同步迁移既有调用方 `tests/integration/test_chat_generation_persistence.py` 中 `converge_generation_failure_phase` 的 `owner_id`/`conversation_id` 参数，移除无归属条件的查询路径。
- `conversations` 继续仅通过 `users.service.ensure_user_active_in_session` 校验用户状态，无跨模块持久化层访问。

### 验证记录与剩余限制

- `.venv/bin/alembic upgrade head && .venv/bin/alembic check`：通过（升级至 `20261122_0006`，`No new upgrade operations detected.`）。
- `.venv/bin/pytest -q`：97 passed（新增 `tests/integration/test_generation_lease_concurrency.py`、`tests/api/test_chat_concurrency_api.py`，扩展 `tests/unit/test_config.py` 并更新 `tests/api/test_health_and_foundation.py` 与 `tests/integration/test_chat_generation_persistence.py`）。
- `.venv/bin/ruff check .` 与 `.venv/bin/ruff format --check .`：All checks passed，132 files already formatted。
- `.venv/bin/mypy src`：Success: no issues found in 83 source files（strict 模式）。
- `python3 tools/check_architecture.py`：工程目录、功能索引、文档链接、分层导入、依赖图和已落地源码检查通过。
- 剩余范围：独立 Worker 进程周期调度 `recover_expired_generations_phase` 将在 Worker 执行框架切片（Ticket 09/17）接线；聊天幂等键与结果查询在 Ticket 10 接入；SSE 流式传输在 Ticket 12 复用 `prepare_generation`、`GenerationLeaseHeartbeat`、`commit_generation_success_phase` 与 `converge_generation_failure_phase`。

### 目录与文档同步

- 已按 `backend-design` 技能同步 `docs/backend-scaffold-design.md` 第 3 节当前目录状态、目录树落地说明与 `modules/conversations/{service,generation}.py` 职责表，同步 `.env.example` 与 `tests/api/test_health_and_foundation.py` 迁移头版本断言（`20261122_0006`）。
