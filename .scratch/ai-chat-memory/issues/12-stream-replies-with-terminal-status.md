# 12: 流式返回与确认生成终态

**What to build:** 让聊天通过 SSE 返回增量和确定终态，并可在断连后查询原生成。

**Blocked by:** [06 查询和选择可用模型](06-discover-and-select-models.md), [08 召回相关范围的记忆](08-recall-relevant-scoped-memories.md), [09 持久化任务并重试记忆写入](09-persist-and-retry-memory-writes.md), [10 支持聊天幂等与生成结果查询](10-recover-idempotent-chat-generations.md), [11 更新人设版本和停用 Agent](11-publish-persona-versions-and-retire-agents.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用工程设计第 5.4 节的共同准备、上下文和终态提交，以及已落地的模型、幂等、租约和任务入口。
- 本切片增加 SSE 编码、心跳、模型增量连接与断连通知，取消和提交竞争通过 generation 的共同入口收敛。
- 测试同时经过同步和流式公开入口验证共享规则，并核对同键重试复用原生成。

## 验收条件

- [x] 新流使用 text/event-stream，事件严格遵循设计中的 conversation.started、message.delta、conversation.completed、conversation.failed 和 heartbeat。
- [x] 开始事件在用户消息和 IDs 提交后发出，业务事件 ID 在生成内递增，delta 序号有序，heartbeat 保活且无状态变更。
- [x] 完整回复、生成成功和任务原子提交后才发送 completed，失败时连接仍可写则发送带稳定错误码及 partial_output_persisted=false 的 failed。
- [x] 流开始前错误使用普通 error 信封，流开始后以失败事件收敛，同时复用同步入口的权限、模型、记忆、人设和租约规则。
- [x] 流式断连尝试上游取消，已提交成功保持成功，否则 cancelled，部分 Assistant 文本被丢弃且无完整回合记忆任务（与同步请求断连继续完成提交的契约区分明确）。
- [x] 旧租约或进程崩溃通过恢复器得到可查询 failed，提交与断连竞争只产生一个终态。
- [x] 原键重试的运行请求返回 202，成功返回原 JSON，客户端用查询恢复生成，重复请求复用原模型调用。
- [x] HTTP 和 PostgreSQL 测试覆盖事件 Schema、顺序、心跳、部分失败、提交失败、断连、期限及结果恢复，并同时验证同步与流式调用共同状态转换和提交入口。

## Answer

### 实际公共符号与后续切片调用入口

- **流式配置与跨字段协调校验（`src/mem_ai/core/config.py`、`.env.example`）**：
  - 新增 `Settings.chat_stream_timeout_seconds`（默认 `120.0`，范围 `(0, 3600]`）与 `Settings.chat_stream_heartbeat_interval_seconds`（默认 `15.0`，范围 `(0, 300]`），并在 `validate_production_boundaries` 强制 `chat_stream_heartbeat_interval_seconds < chat_stream_timeout_seconds`（与 `chat_lease_renew_interval_seconds < chat_lease_ttl_seconds` 协调一致）。
- **OpenAI 兼容流式适配器（`src/mem_ai/modules/models/adapters/openai_compatible.py`）**：
  - 实现 `OpenAICompatibleModelProvider.stream_generate(request) -> AsyncIterator[ModelStreamChunk]`：以 `stream=True` 与 `stream_options={"include_usage": True}` 调用 `/chat/completions`，跳过注释与空/角色增量，按单调递增 `sequence` 输出文本增量，在 `[DONE]` 终止时输出含累计 `usage`/`finish_reason` 的最终 `ModelStreamChunk`；未收到 `[DONE]` 的截断流、非 2xx、非法 JSON 与超时统一映射为脱敏 `BadGatewayError("model_upstream_error")` 或 `GatewayTimeoutError("generation_timeout")`；调用方取消或关闭生成器时通过 `async with http_client.stream(...)` 立即关闭上游 HTTP 连接。
  - 抽取 `_build_payload`、`_build_headers`、`_effective_timeout`、`_raise_timeout_error`、`_raise_transport_error`、`_check_response_status` 供 `generate` 与 `stream_generate` 复用。
- **共享生成执行引擎与对话编排（`src/mem_ai/modules/conversations/execution.py`、`src/mem_ai/modules/conversations/service.py`、`src/mem_ai/modules/conversations/generation.py`、`src/mem_ai/modules/conversations/schemas.py`）**：
  - `conversations/execution.py` 抽离同步与流式共用的核心执行引擎：`run_prepared_generation_lifecycle`（统一驱动 `GenerationLeaseHeartbeat`、上下文准备、模型执行、原子 Tx 2 `commit_generation_success_phase` + `build_turn_memory_enqueue_hook` 及 DB 终态收敛）、`execute_model_completion`、`execute_model_stream`（在 `finally` 中以 `anyio.CancelScope(shield=True)` 关闭上游流迭代器）及 `SyncGenerationTaskRegistry`。
  - `conversations/generation.py` 新增 `converge_generation_failure_safely`：普通失败/取消通过条件更新写入终态并返回已确认的 `GenerationRecord`；面对 `generation_lease_lost` 遵循 §5.2 隔离规则不以旧执行者写入 DB，仅查询是否已由接管/Worker 恢复器收敛为终态，防止未收敛时误发 `conversation.failed`。
  - `conversations/service.py` 将 `SyncChatOptions` / `sync_chat_options_from_settings` 统一演进为 `ChatOptions` / `chat_options_from_settings`，新增 `complete_prepared_stream_generation`、`cancel_stream_generation` 与 `execute_prepared_sync_chat`；`prepare_chat_turn` 默认允许流式并在 `stream=True` 新执行时自动开启 `require_streaming=True`（**供 Ticket 13/17 直接扩展代次屏障与指标观测**）。
  - `conversations/schemas.py` 新增 `StreamGenerationFailure` 与 `StreamGenerationOutcome`。
- **SSE 传输编码与路由接入（`src/mem_ai/modules/conversations/stream.py`、`src/mem_ai/modules/conversations/router.py`）**：
  - `stream.py` 落地 §8.4 纯 SSE 线格式编码器（`format_sse_event`、`format_conversation_started`、`format_message_delta`、`format_conversation_completed`、`format_conversation_failed`、`format_sse_heartbeat`、`SSE_MEDIA_TYPE`、`SSE_RESPONSE_HEADERS`），严格保证 `snake_case` 字段、业务事件正整数单调 `id`、`delta.sequence` 从 1 递增、`partial_output_persisted=false` 以及 `heartbeat` 无 `id` 且 `data={}`。
  - `SseStreamingResponse` 继承 `StreamingResponse` 并在 `finally` 中通过 `anyio.CancelScope(shield=True)` 保证断连时关闭 body 异步生成器；`stream_prepared_generation` 与 `build_chat_sse_response` 复用心跳多路复用、仅在 DB 收敛确认后发送 `conversation.failed`，断连时以有界宽限取消 `producer_task` 并调用 `conversations_service.cancel_stream_generation` 收敛为 `cancelled`（已提交 `succeeded` 保持成功）。
  - `router.py` 的 `POST /v1/chat` 统一走 `prepare_chat_turn(..., allow_streaming=True)`：已有幂等记录优先返回 `202` / `200 application/json` / 原终态错误；新执行 `stream=true` 返回 `text/event-stream`（流前错误仍走统一 JSON 错误信封），`stream=false` 走屏蔽断连的 `execute_prepared_sync_chat`。

### 复用、迁移与双轴代码审阅修复

- 直接复用 05–11 落地的 `prepare_generation_phase`、`persist_recalled_memories_phase`、`commit_generation_success_phase`、`converge_generation_failure_phase`、`recover_expired_generations_phase`、`GenerationLeaseHeartbeat`、`idempotency.py`、`queries.py`、`LocalPersonaProvider` 与 `models.service.resolve_model_for_chat`（无需新增数据库迁移）。
- 迁移既有调用方：将 `tests/api/test_chat_api.py` 中此前 05 切片临时拦截 `stream=True` 的 `422` 断言迁移为流前模型不可用 `409 model_unavailable`；将全部 `SyncChatOptions` 引用迁移至 `ChatOptions`；将 `application.py`/`router.py` 迁移至 `conversations.execution.SyncGenerationTaskRegistry`。
- 经双轴 `code-review`（Standards & Spec）修复：
  1. 消除 `stream.py` 对 `generation_store`/`GenerationLeaseHeartbeat` 的直接编排重复，将共享生命周期执行抽离至 `conversations/execution.py` 并由 `conversations/service.py` 统一对外编排，`stream.py` 保持纯 SSE 传输层且全部源码文件严守 ≤ 800 行架构上限。
  2. 严格落实 §8.4 “失败已收敛且连接可写”与 §5.2 “旧执行者隔离”双契约：仅当 DB 确认收敛至 `failed`/`cancelled` 时才发送 `conversation.failed`；`generation_lease_lost` 时旧执行者不写 DB，已由 Worker/接管收敛则发 `conversation.failed`，未收敛则取消上游并静默结束交由恢复器收敛。
  3. 删除未接线的死参数 `is_disconnected`，为断连清理中的 `producer_task` 等待增加有界超时（`_PRODUCER_CANCEL_GRACE_SECONDS = 2.0`），防止悬挂上游 `aclose()` 阻塞 `cancelled` 收敛。
  4. 补充 `chat_stream_heartbeat_interval_seconds < chat_stream_timeout_seconds` 跨字段配置校验、共享 `openai_compatible.py` 异常/日志映射辅助函数及测试公共 `parse_sse_text` 解析器。
  5. 补齐 `stream_prepared_generation` 路径上的“Tx 2 提交后迟到断连保持 `succeeded`”与“流中租约丢失（恢复前/后两种分支）”PostgreSQL 集成测试。

### 验证记录与剩余限制

- `.venv/bin/alembic check`：通过（`No new upgrade operations detected.`，无结构漂移）。
- `.venv/bin/pytest -q`：**155 passed**（新增 `tests/unit/test_sse_stream_codec.py`、`tests/integration/test_chat_streaming_persistence.py`、`tests/api/test_chat_streaming_api.py`，扩展 `tests/unit/test_config.py` 与 `tests/integration/test_openai_compatible_model_adapter.py`，并迁移 `tests/api/test_chat_api.py` 及相关集成测试至 `ChatOptions`）。
- `.venv/bin/ruff format --check .` 与 `.venv/bin/ruff check .`：All checks passed，157 files already formatted。
- `.venv/bin/mypy src`：Success: no issues found in 92 source files（strict 模式）。
- `python3 tools/check_architecture.py`：工程目录、功能索引、文档链接、分层导入、依赖图和已落地源码检查通过。
- **剩余范围**：SSE 并发与连接预算/观测指标接入将在 Ticket 17 扩展；记忆清空屏障对流式与同步生成的统一抑制将在 Ticket 13 接入。

### 目录与文档同步

- 已按 `backend-design` 技能同步 `docs/backend-scaffold-design.md` 第 3 节当前目录状态、功能定位索引、目录树注释与模块职责表（补充 `conversations/execution.py` 与 `conversations/stream.py`，更新 `conversations/service.py`、`conversations/generation.py` 与 `models/adapters/openai_compatible.py`），并同步 `.env.example`。
