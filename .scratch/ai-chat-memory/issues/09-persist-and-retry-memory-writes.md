# 09: 持久化任务并重试记忆写入

**What to build:** 让完整回合的记忆写入可靠落到后台，即使进程重启仍可重试或核验外部结果。

**Blocked by:** [04 查看和显式添加长期记忆](04-list-and-explicitly-create-memories.md), [05 发起和继续同步对话](05-start-and-continue-sync-chat.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 扩展 04 的统一 jobs 记录与执行结果入口，记忆处理器复用既有适配器和范围策略。
- 通过 05 的成功提交阶段在同一 Session 入队，memory_jobs 作为 jobs 的只读视图管理。
- jobs 负责领取、租约、重试和任务状态，memories/jobs.py 负责业务副作用，runner 负责注册与并发调度。

## 验收条件

- [x] 完整回复、生成成功和普通记忆任务在同一短事务提交，普通回合写入范围遵循固定人设快照的 `memoryPolicy.writeScope`（默认 `user`，可设为 `conversation`），入队失败整体回滚，关闭记忆写入时只提交回复及状态。
- [x] 失败和取消回合不创建完整回合任务，上游记忆失败保持已提交回复成功。
- [x] 统一 jobs 表以 job_type 注册业务处理器，memory_jobs 为只读查询视图，执行摘要具有独立保留时间。
- [x] 独立 Worker 使用 SKIP LOCKED、租约和令牌条件提交，支持可配置并发、指数退避、抖动、最大尝试和优雅退出。
- [x] 稳定任务 ID 和去重键防重复入队，上游使用幂等或结果核验，响应丢失进入 reconciling 并留下关联信息。
- [x] 租约过期重新领取时旧执行者无法覆盖状态，未知上游结果保持可核验并告警。
- [x] 任务状态、重试、下一次执行及错误摘要可查询或观测，日志过滤正文、Token 和密钥。
- [x] PostgreSQL 测试验证原子提交、重启、重复领取、失败耗尽、未知结果和退出恢复，框架可供删除及周期处理器复用。

## Answer

### 实际公共符号与后续切片调用入口

- **数据库迁移与公共时间辅助（`migrations/versions/20261123_0008_add_job_attempts_and_memory_jobs_view.py`、`src/mem_ai/db/base.py`）**：
  - `20261123_0008` 迁移新增 `jobs(status, lease_expires_at)` 索引、`job_attempts` 表（含 `attempt_number >= 1`、`status` CHECK 约束与独立保留期索引 `ix_job_attempts_expires_at`），以及受 `INSTEAD OF INSERT OR UPDATE OR DELETE` 触发器保护的只读 PostgreSQL 视图 `memory_jobs`（筛选 `memory_turn_write` 与 `memory_explicit_create` 并投影 `write_id`/`provider_id`/`scope`/`conversation_id`/`generation_id`/`memory_epoch`）。
  - `src/mem_ai/db/base.py` 抽离共享 `normalize_utc` 与 `get_database_now`，供 `jobs/repository.py` 与 `conversations/repository.py` 复用。
- **统一任务框架（`src/mem_ai/modules/jobs/`）**：
  - `models.py`：`Job`（新增 `ix_jobs_status_lease_expires_at`）、`JobAttempt`、`JOB_ATTEMPT_STATUSES`、`MEMORY_WRITE_JOB_TYPES`。
  - `schemas.py`：`JobAttemptStatus`、`JobOutcomeKind`、`JobAttemptRecord`、`ClaimedJob`、`MemoryJobViewRecord`、`JobBackoffPolicy`、`JobExecutionContext`、`JobBeforeCommitHook`、`JobHandlerOutcome`（支持 `before_commit` 同事务原子钩子）、`ResolvedJobTransition`、`JobObservabilitySnapshot`、`JobHandler` 协议。
  - `repository.py`：`enqueue_job_idempotent`（基于 `uq_jobs_dedupe_key` Savepoint 去重）、`claim_due_jobs`（`FOR UPDATE SKIP LOCKED` 领取到期与租约过期任务、`reconciling` 轮询不消耗重试配额、无外部副作用的重复崩溃在 `max_attempts` 边界收敛为 `failed`）、`conditional_renew_job_lease`、`conditional_mark_job_dispatched`、`conditional_update_job_outcome`、`conditional_requeue_failed_job`、基于 SQLAlchemy Core `select()` 的 `list_memory_jobs_view`、`get_job_status_aggregates`。
  - `service.py`：`compute_backoff_delay_seconds`、`resolve_job_transition`、`enqueue_job_in_session`（事务参与入队）、`claim_due_jobs`、`renew_job_lease`、`mark_job_dispatched_in_session`、`record_job_outcome`（令牌+租约条件提交、脱敏摘要、支持 `before_commit_hook` 原子推进关联操作并输出 `job_reconciling`/`job_failed` 告警日志）、`requeue_failed_job`（耗尽失败任务的受控重试运维入口）、`list_job_attempts`、`list_memory_jobs`、`get_job_observability`。
- **异步操作条件推进（`src/mem_ai/modules/operations/{repository,service}.py`）**：
  - `operations_service.transition_operation_in_session`：事务参与条件更新非终态 `operations` 行，不覆盖既有终态，供 `ExplicitMemoryReconcileJobHandler` 及后续 Ticket 13–15 删除处理器通过 `JobHandlerOutcome.before_commit` 与任务状态同事务原子提交。
- **记忆任务入队与业务处理器（`src/mem_ai/modules/memories/{service,jobs,repository}.py`）**：
  - `memories_service.enqueue_turn_memory_write_in_session`：在生成成功短事务（Tx 2）中检查 `memoryPolicy.enabled`/`write_enabled`/`write_scope` 及 `memory_scopes`（`FOR SHARE`）代次与屏障状态，按 `memory_turn_write:<generationId>` 稳定去重键入队且绝不落库原始消息正文；屏障激活或代次推进时抑制旧代次任务同时保持回复提交成功。
  - `src/mem_ai/modules/memories/jobs.py`：定义 `TurnMemoryDispatchState`、`TurnMemoryJobMetadata`、`ExplicitMemoryJobMetadata`、`TurnContentRecord`、`TurnContentLoader` 协议（含 `load_turn_for_memory_write` 与 `is_conversation_active_in_session`，避免 `memories` 反向依赖 `conversations`）、`format_turn_memory_content`、`TurnMemoryWriteJobHandler`（发前在同一短事务按 `users -> conversations (FOR SHARE) -> memory_scopes (FOR UPDATE) -> jobs` 校验并标记 `in_flight`；响应丢失/崩溃恢复先调用 `verify_operation`；`confirmed_during_clear` 保持 `reconciling` 等待清空收敛）与 `ExplicitMemoryReconcileJobHandler`（核验 `memory_explicit_create` 并经 `before_commit` 原子更新 `operations`）。
- **对话成功提交接线与只读回合加载（`src/mem_ai/modules/conversations/{generation,service}.py`）**：
  - `conversations/generation.py` 新增 `is_conversation_active_in_session` 与 `load_turn_for_memory_write`（坚守持久化层所有权，不让 `service.py` 越层依赖 `repository.py`）。
  - `conversations/service.py` 新增 `DatabaseTurnContentLoader`、`build_turn_memory_enqueue_hook`（同步与 Ticket 12 SSE 共用 Tx 2 入队钩子）及 `SyncChatOptions.memory_write_max_attempts`，在 `complete_prepared_sync_generation` 中将普通记忆任务入队纳入同一 Tx 2 原子事务。
- **独立 Worker 执行框架与进程入口（`src/mem_ai/workers/{runner,main}.py`、`src/mem_ai/core/config.py`、`.env.example`、`Makefile`）**：
  - `workers/runner.py`：`WorkerConfig`、`worker_config_from_settings`、`PeriodicJobSpec`（PostgreSQL 窗口去重周期调度）、`WorkerHealthSnapshot`、`JobHandlerRegistry`、`JobLeaseHeartbeat`、`WorkerRunner`（可配置并发、`run_once` / `run_forever`、含 `duration_ms` 与关联字段的结构化 JSON 日志、信号优雅退出与在途任务收敛）。
  - `workers/main.py`：`JOB_TYPE_GENERATION_LEASE_RECOVERY`、`GenerationLeaseRecoveryJobHandler`（复用 `conversations/generation.py:recover_expired_generations_phase`）、`build_worker_registry`、`default_periodic_jobs`、`run_worker` 与 `python -m mem_ai.workers.main`（对应 `make worker`），并登记为 `tools/check_architecture.py` 显式装配例外。

### 复用、迁移与双轴代码审阅修复

- 复用 04 的 `jobs`/`operations`/`memory_scopes` 表与 `MemOSMemoryProvider` 适配器，复用 05/07/08 的共享生成生命周期 `commit_generation_success_phase`、`recover_expired_generations_phase` 与脱敏日志 `JsonLogFormatter`。
- 经双轴 `code-review`（Standards & Spec）修复：
  1. 将 `DatabaseTurnContentLoader` 底层查询迁入 `conversations/generation.py`，消除 `conversations/service.py` 对 `repository.py` 的直接导入。
  2. 为 `JobHandlerOutcome` 与 `record_job_outcome` 增加原子 `before_commit_hook`，使 `ExplicitMemoryReconcileJobHandler` 的 `operations` 状态推进与 `jobs` 租约条件提交同事务执行，杜绝租约失效后旧执行者覆盖 `operations`。
  3. 修复 `confirmed_during_clear` 下次轮询误入发前分支自取消的问题，确保保持 `reconciling` 等待清空处理器收敛。
  4. 区分 `reconciling` 核验轮询与失败重试配额（核验轮询不消耗 `jobs.attempt_count`，同时 `job_attempts.attempt_number` 递增），并限制无外部副作用崩溃任务的无限重领；补充 `requeue_failed_job` 受控重试入口。
  5. 在 `_dispatch_turn_write` 的 `memory_scopes` 短事务及确认后校验中同步复查会话 `active` 状态，将 `list_memory_jobs_view` 改为 SQLAlchemy Core `select()`，并在 Worker 日志补充 `duration_ms`。

### 验证记录与剩余限制

- `uv run alembic upgrade head && uv run alembic check`：通过（升级至 `20261123_0008`，`No new upgrade operations detected.`）。
- `uv run pytest -q`：**123 passed**（新增 `tests/unit/test_jobs_policy.py`、`tests/integration/test_durable_jobs_and_worker.py`、`tests/api/test_chat_memory_write_api.py`，扩展 `tests/unit/test_config.py` 并更新 `tests/api/test_health_and_foundation.py`）。
- `uv run ruff format --check .` 与 `uv run ruff check .`：All checks passed，145 files formatted。
- `uv run mypy src`：Success: no issues found in 88 source files（strict 模式）。
- `python3 tools/check_architecture.py`：工程目录、功能索引、文档链接、分层导入、依赖图和已落地源码检查通过。
- **剩余范围**：`memory_tombstones` 与清空屏障最终清理收敛在 Ticket 13 接入；`job_attempts` 30 天到期批量清理处理器在 Ticket 16 接入 `WorkerRunner` 周期任务框架；`/metrics` 指标导出在 Ticket 17 接入。

### 目录与文档同步

- 已按 `backend-design` 技能同步 `docs/backend-scaffold-design.md` 第 2.3 节装配例外及第 3 节当前目录状态、目录树说明与职责表，同步 `.env.example`、`Makefile`、`tools/check_architecture.py` 与 `tests/api/test_health_and_foundation.py`。
