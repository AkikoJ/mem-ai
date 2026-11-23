# 10: 支持聊天幂等与生成结果查询

**What to build:** 让客户端安全重试聊天并找回原生成，首次响应丢失时也能恢复隐式创建的会话。

**Blocked by:** [07 控制同一会话的并发生成](07-reject-overlapping-conversation-generation.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 扩展 05、07 的生成准备与终态事务，幂等占位和结果更新显式参与该阶段 Session。
- idempotency 统一负责摘要与原结果解析，查询读取已有生成数据，后续 SSE 重试复用相同入口。
- 同键请求先解析已有结果再竞争租约，原请求恢复使用已有生成与固定配置。

## 验收条件

- [x] 聊天必填 Idempotency-Key，唯一范围为 user_id、HTTP 方法、规范路由和键，业务摘要包含 Agent、会话缺省标记、消息、请求模型等参数。
- [x] stream 不参与摘要，首次选定的人设和模型固定保存，配置变化后的重试复用原生成。
- [x] 幂等占位、隐式会话、生成和用户消息原子关联，同键并发只创建一份并只调用一次模型。
- [x] 同键同摘要的 running 返回 202 和原 IDs、查询地址，成功统一返回已保存的 200 JSON，失败及取消返回原终态与稳定错误。
- [x] 同键异摘要返回 409 idempotency_key_conflict，幂等记录读取先于租约争用，失败后新键可再生成。
- [x] 生成查询验证用户归属、会话和用户当前状态，返回运行或终态、成功结果及错误，删除后的资源遵守统一策略。
- [x] 终态幂等自 terminal_at 保留 24 小时，活跃记录持续保留，生成独立按会话保留策略保存。
- [x] 路由及数据库测试覆盖同键并发、丢响应、stream 切换、摘要冲突、跨用户、租约恢复、到期边界和已删除资源。

## Answer

### 实际公共符号与后续切片调用入口

- **数据库表、约束与迁移（`migrations/versions/20261123_0009_add_idempotency_records.py`、`src/mem_ai/modules/conversations/models.py`）**：
  - 新增 `idempotency_records` 表与 `IdempotencyRecord` ORM 模型：唯一约束 `uq_idempotency_records_scope(user_id, method, canonical_route, idempotency_key)` 与 `uq_idempotency_records_generation_id(generation_id)`；CHECK 约束 `ck_idempotency_records_status_valid` 与 `ck_idempotency_records_terminal_expiry_valid`（保证 `running` 记录 `terminal_at`/`expires_at` 必为空，终态必有非空 `terminal_at` 且 `expires_at >= terminal_at`）；索引 `ix_idempotency_records_expires_at`（部分索引，供 Ticket 16 到期清理扫描）、`ix_idempotency_records_conversation_id`（供 Ticket 14 会话清理）、`ix_idempotency_records_user_id`（供 Ticket 15 用户清理）。
- **幂等键、业务摘要与原结果回放（`src/mem_ai/modules/conversations/idempotency.py`）**：
  - `CHAT_HTTP_METHOD`（`"POST"`）、`CHAT_CANONICAL_ROUTE`（`"/v1/chat"`）、`normalize_idempotency_key`、`compute_chat_request_digest`（纳入 `agent_id`、`conversation_id`、`conversation_omitted`、规范化 `message`、`model_id`，显式排除 `stream`）、`build_chat_idempotency_spec`。
  - `is_record_expired`（`running` 永不过期，终态按 `expires_at <= current_time` 判定）、`infer_error_http_status`、`build_replay_error`（失败复原原 HTTP 状态类别并附带 `conversation_id`/`generation_id`/`user_message_id`/`status`/`status_url`，取消统一返回 `409 generation_cancelled`）、`resolve_replay_from_detail`。
  - 事务参与与读查询入口：`purge_expired_or_find_live_in_session`（Tx 1 内命中未过期记录或清理已过期终态旧键以便按新请求复用）、`create_placeholder_in_session`、`finalize_succeeded_in_session`、`finalize_terminal_failure_in_session`、`resolve_chat_idempotency`（在租约竞争、人设/模型解析和 `stream` 校验之前执行）、`get_idempotency_record_for_generation`。
- **共享只读查询与会话访问策略（`src/mem_ai/modules/conversations/queries.py`）**：
  - 抽离并统一管理会话访问策略（`load_accessible_conversation_in_session`、`ensure_conversation_accessible_in_session`、`is_conversation_active_in_session`、`ensure_conversation_accessible`、`load_conversation_for_continuation`）与只读查询（`load_generation_detail_in_session`、`get_generation_detail_for_owner`、`get_generation_for_owner`、`list_conversation_messages_for_owner`、`list_recalled_memories_for_message_for_owner`、`load_turn_for_memory_write`），以及只读协议适配 `DatabaseConversationAccessChecker` 与 `DatabaseTurnContentLoader`（**供 Ticket 14 会话列表/详情/消息查询直接扩展**）。
- **生成持久化阶段与共享编排（`src/mem_ai/modules/conversations/generation.py`、`src/mem_ai/modules/conversations/service.py`、`src/mem_ai/modules/conversations/schemas.py`）**：
  - `prepare_generation_phase` 在 Tx 1 内将幂等占位与隐式会话/生成/用户消息原子提交；对于已有会话在锁会话行后先核对幂等键再竞争租约；唯一约束并发竞争回滚后通过 `IdempotencyScopeRaceError` / `IdempotencyRecordHitError` 统一回读胜出记录。
  - `commit_generation_success_phase` 在 Tx 2 同事务调用 `finalize_succeeded_in_session`；`converge_generation_failure_in_session` 在失败、取消、内联过期接管及 Worker 过期恢复（`recover_expired_generations_phase`）同事务调用 `finalize_terminal_failure_in_session`。
  - `service.prepare_chat_turn` 作为同步与 **Ticket 12 SSE 共用的幂等准备入口**；`service.run_sync_chat` 通过 `@overload` 统一支持显式 `idempotency_key`（返回 `ChatTurnOutcome = CommittedChatResult | ChatAcceptedResult`）与内部无键测试调用。
  - `schemas.py` 新增 `DEFAULT_IDEMPOTENCY_RETENTION_SECONDS`、`generation_status_url`、`IdempotencyKeySpec`、`IdempotencyRecordDTO`、`GenerationDetail`、`ChatAcceptedResult`、`ChatAcceptedResponse`、`GenerationErrorResponse`、`GenerationResponse`。
- **HTTP 路由与配置（`src/mem_ai/modules/conversations/router.py`、`src/mem_ai/core/config.py`、`.env.example`、`src/mem_ai/workers/main.py`）**：
  - `POST /v1/chat` 必填 `Idempotency-Key`（OpenAPI 标为 `required: true`，同时保持未认证请求优先 `401`、认证后缺失返回 `422 idempotency_key_required`、非法值返回 `422 invalid_idempotency_key`）；`running` 重试返回 `202 Accepted` + `Location` + `statusUrl`，`succeeded` 无论 `stream=false/true` 均返回已保存 `200 application/json`。
  - 新增 `GET /v1/conversations/{conversationId}/generations/{generationId}`，校验用户状态（`403`）、会话归属与非 `deleting`（`404`）、生成归属（`404`），成功读取统一返回 `200` 及状态、关联 ID、成功结果或已保存错误对象。
  - `Settings.idempotency_retention_hours`（默认 `24`，范围 `1..720`）同步 `.env.example`、`sync_chat_options_from_settings` 与 `workers/main.py` 过期恢复处理器。

### 复用、迁移与双轴代码审阅修复

- 直接扩展 05/07/08/09 的共享生成生命周期阶段（`prepare_generation_phase`、`commit_generation_success_phase`、`converge_generation_failure_in_session`、`recover_expired_generations_phase`），所有既有 `POST /v1/chat` API 测试调用方均已迁移至必填 `Idempotency-Key`；`application.py`、`workers/main.py` 与测试同步迁移至 `conversations.queries` 下的只读适配器。
- 经双轴 `code-review`（Standards & Spec）修复：
  1. 将 `stream` 开关校验与并发竞争重试环路归位到 `service.py:prepare_chat_turn`，移除 `idempotency.py` 中的编排逻辑与不可达 503 死代码（避免竞态异常泄漏为未处理 500）。
  2. 统一 `service.run_sync_chat` 单一入口（消除并行 `run_idempotent_sync_chat` 重复路径），消除 `service.py` 中 `get_generation_detail` 透传层与 `generation.py` 中 7 个无调用方 re-export。
  3. 强制 `cancelled` 重试回放与终态落库使用 `409 generation_cancelled`，抽离公共 `_resolve_error_http_status` 与 `_ensure_idempotency_slot_available_in_session`，并将内部 ORM 查询函数私有化（`_get_record_in_session`、`_load_readable_conversation_in_session`）。
  4. 通过 `openapi_extra` 在 OpenAPI 规范中将 `POST /v1/chat` 的 `Idempotency-Key` 明确声明为 `required: true` 同时维持认证优先于头参数校验。
  5. 补齐 HTTP 级 24 小时到期边界、`user.status="deleting"` 与真正并发 `asyncio.gather` Tx 1 竞态测试。

### 验证记录与剩余限制

- `.venv/bin/alembic upgrade head` 与 `.venv/bin/alembic check`：通过（升级至 `20261123_0009`，`No new upgrade operations detected.`）。
- `.venv/bin/pytest -q`：**133 passed**（新增 `tests/unit/test_idempotency_policy.py`、`tests/integration/test_chat_idempotency_persistence.py`、`tests/api/test_chat_idempotency_and_generations_api.py`，扩展 `tests/unit/test_config.py` 与 `tests/architecture/test_architecture_rules.py`，并迁移 `tests/api/test_chat_*.py`、`tests/api/test_models_and_chat_selection_api.py` 与 `tests/api/test_health_and_foundation.py`）。
- `.venv/bin/ruff format --check .` 与 `.venv/bin/ruff check .`：All checks passed，151 files formatted。
- `.venv/bin/mypy src`：Success: no issues found in 90 source files（strict 模式）。
- `python3 tools/check_architecture.py`：工程目录、功能索引、文档链接、分层导入、依赖图和已落地源码检查通过。
- **剩余范围**：新请求 `stream=true` 的 SSE 流式响应与断连取消将在 Ticket 12 复用 `prepare_chat_turn`、`GenerationLeaseHeartbeat`、`commit_generation_success_phase` 与 `converge_generation_failure_phase` 接入；过期 `idempotency_records` 后台周期清扫将在 Ticket 16 复用 `ix_idempotency_records_expires_at` 接入；会话/用户删除对关联幂等记录的批量清理将在 Ticket 14/15 接入。

### 目录与文档同步

- 已按 `backend-design` 技能同步 `docs/backend-scaffold-design.md` 第 3 节当前目录状态、功能定位索引、目录树注释与职责表，同步 `.env.example` 与 `tests/api/test_health_and_foundation.py` 迁移头版本断言（`20261123_0009`）。
