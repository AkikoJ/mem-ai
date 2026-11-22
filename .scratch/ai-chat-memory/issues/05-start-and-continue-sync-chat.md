# 05: 发起和继续同步对话

**What to build:** 让用户通过聊天入口隐式创建或继续会话，得到完整同步回复，并可在模型失败后保留真实历史。

**Blocked by:** [03 创建和使用私有 Agent](03-create-and-use-private-agent.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用 03 的生命周期实现和 PersonaProvider，通过 models 解析默认模型并调用项目 ModelProvider。
- 本切片建立工程设计第 5.4 节的共同生成编排与持久化阶段，统一用户消息准备、成功提交和失败收敛。
- 07、08、09、10、12、13 分别扩展相同入口的租约、召回、入队、幂等、SSE 和代次检查，Prompt 编译使用纯项目数据。

## 验收条件

- [x] 省略 conversationId 时创建会话并固定最新发布版本，成功返回会话、生成、回复 ID、实际模型及用量。
- [x] 继续会话使用固定人设，近期成功回合和当前输入进入 Prompt，失败及取消回合的用户消息留存但从后续 Prompt 过滤。
- [x] 无发布版本、Agent 与会话不匹配、跨用户资源及不可用默认模型在外部调用前拒绝。
- [x] 生成记录从 running 转为 succeeded 或 failed，调用模型之前提交原始用户消息。
- [x] 准备、上下文、成功提交及失败收敛具有共同入口，完成记录列出后续租约、幂等和 SSE 切片应扩展的实际符号。
- [x] 完整 Assistant 回复与生成成功在同一短事务提交，普通记忆任务在任务切片接入后加入同一事务。
- [x] 准备阶段在数据库事务外通过只读 `PersonaProvider` 解析人设快照，短事务中固定版本引用与原始用户消息；同步请求进入模型调用后不因客户端 HTTP 断连而中止，保证提交后可通过原幂等键取回完整结果。
- [x] 模型 HTTP 期间释放数据库事务，提交失败、模型超时、上游错误均留下稳定可恢复状态。
- [x] 服务端注册的兼容接口模型适配器通过项目 ModelProvider 实际完成完整生成，受控上游验证响应、用量、超时及失败映射。
- [x] 路由测试和 PostgreSQL 集成测试覆盖新会话、续聊、失败历史过滤、资源隔离及原子提交。

## Answer

### 实际公共符号与后续切片扩展入口

- **模型配置同步、选择与 OpenAI 兼容适配器（`src/mem_ai/modules/models/`）**：
  - `src/mem_ai/modules/models/models.py`：`ModelConfig`（`model_configs` 表，`model_id` 唯一约束、`enabled`/`supports_streaming`/`max_context_tokens`/`max_output_tokens`/`required_permission`/`credential_ref`/`capabilities_json`）。
  - `src/mem_ai/modules/models/schemas.py`：`ModelConfigSeed`、`ModelConfigRecord`、`ResolvedModelSelection`。
  - `src/mem_ai/modules/models/repository.py`：`get_model_config_by_model_id`、`upsert_model_config`（基于 `model_id` 幂等 `ON CONFLICT DO UPDATE`）。
  - `src/mem_ai/modules/models/service.py`：`sync_model_configs`（受控服务端配置幂等同步）与 `resolve_model_for_chat`（**Ticket 06 扩展入口**：复用同一模型选择入口并扩展 `/v1/models` 发现、强制/覆盖策略、权限与能力检查）。
  - `src/mem_ai/modules/models/adapters/openai_compatible.py`：`OpenAICompatibleModelProvider`，实现项目 `ModelProvider` 协议（`generate` 与预留 `stream_generate`），优先使用 PostgreSQL 同步解析出的 `ModelRequest.upstream_model`（并保留 `upstream_model_map` 兜底），封装服务端 `Bearer` 鉴权、用量归一、超时映射 `504 generation_timeout`、上游/畸形响应映射 `502 model_upstream_error` 及密钥/正文脱敏（**Ticket 12 扩展 `stream_generate`**）。
- **共享生成生命周期、持久化阶段与纯 Prompt 编译（`src/mem_ai/modules/conversations/`）**：
  - `src/mem_ai/modules/conversations/models.py`：`Conversation`（`conversations` 表，逻辑引用 `agent_id`/`persona_version_id` 不阻断远程 `PersonaProvider`、`next_message_sequence` 原子序号分配）、`Generation`（`generations` 表，含 `status` CHECK、`user_message_id`/`assistant_message_id` 唯一外键、`model_id`、`memory_epoch`、`lease_token`/`lease_expires_at`、用量与错误列）与 `Message`（`messages` 表，含 `(conversation_id, sequence)` 与 `(generation_id, role)` 唯一约束）。
  - `src/mem_ai/modules/conversations/schemas.py`：不可变应用 DTO（`ConversationRecord`、`MessageRecord`、`GenerationRecord`、`PreparedGeneration`、`PreparedContext`、`CommittedChatResult`）与严格 `extra="forbid"` 的 HTTP Schema（`ChatRequest`、`ChatMessageResponse`、`ChatUsageResponse`、`ChatResponse`）。
  - `src/mem_ai/modules/conversations/prompt.py`：`DEFAULT_PLATFORM_RULES`、`PromptHistoryMessage`、`compile_chat_prompt`（纯项目数据编译：平台规则 → 固定人设 → 召回记忆占位 → 近期 `succeeded` 历史 → 当前输入；**Ticket 08 注入召回记忆**）。
  - `src/mem_ai/modules/conversations/repository.py`：`get_conversation_for_owner`、`get_conversation_for_update`、`create_conversation`、`allocate_message_sequence`、`create_generation`、`create_message`、`link_generation_user_message`、`list_succeeded_history_messages`（按 `Generation.status == 'succeeded'` 过滤失败与取消回合）、`list_all_messages_for_conversation`、`get_generation_for_owner`、`conditional_mark_generation_succeeded`、`conditional_mark_generation_terminal_failure`。
  - `src/mem_ai/modules/conversations/generation.py`（持久化阶段唯一入口）：
    - `ensure_conversation_accessible_in_session` / `ensure_conversation_accessible` / `load_conversation_for_continuation`（统一归属 `404`、删除中 `409 conversation_deleting` 与 Agent 不一致 `409 conversation_agent_mismatch` 策略）。
    - `prepare_generation_phase`（短事务 1：建/锁会话、读取 `succeeded` 历史、写入 `running` 生成与原始用户消息；**Ticket 07 扩展会话租约冲突 `409` 与过期接管；Ticket 10 扩展幂等占位；Ticket 13 扩展 `memory_epoch` 捕获**）。
    - `commit_generation_success_phase`（短事务 2：复查用户/会话状态、分配序号写完整 Assistant 回复、条件更新 `running -> succeeded` 并暴露 `before_commit_hook`；**Ticket 09 注入普通记忆任务原子入队；Ticket 10 注入幂等终态写入；Ticket 13 注入旧代次任务抑制**）。
    - `converge_generation_failure_phase`（条件收敛 `running -> failed/cancelled`，不覆盖已提交 `succeeded`；**Ticket 07 过期恢复、Ticket 10 失败幂等更新与 Ticket 12 断连取消共用**）。
    - `get_generation_for_owner` / `list_conversation_messages_for_owner`（只读查询入口，供 Ticket 10/14 扩展）。
  - `src/mem_ai/modules/conversations/service.py`（Conversation Core 编排唯一入口）：
    - `prepare_generation`（事务外调用只读 `PersonaProvider` 与 `models.service.resolve_model_for_chat`，再调用 `prepare_generation_phase`）。
    - `prepare_context`（调用 `compile_chat_prompt`；**Ticket 08 扩展记忆策略召回与 `message_recalled_memories` 短事务落库**）。
    - `execute_model_completion`（事务外在总生成期限 `asyncio.wait_for` 下调用 `ModelProvider.generate`）。
    - `complete_prepared_sync_generation`（串联上下文、模型调用、成功提交与异常/提交失败收敛，上下文与模型异常统一收敛 `failed`）。
    - `SyncChatOptions`、`run_sync_chat` 与 `SyncGenerationTaskRegistry`（通过 `asyncio.shield` 隔离客户端 HTTP 断连，保障进入模型调用后的同步回合在总期限内继续完成提交并在应用停机时有界排空）。
    - `DatabaseConversationAccessChecker`（实现 `memories` 的 `ConversationAccessChecker` 协议）。
  - `src/mem_ai/modules/conversations/router.py`：`POST /v1/chat`（挂载于 `/v1`，12 切片前以 `422 streaming_not_enabled` 拒绝 `stream=true`，10 切片接入必填 `Idempotency-Key`）。
- **配置、装配与迁移**：
  - `src/mem_ai/core/config.py` 与 `.env.example`：新增 `MODEL_PROVIDER_MODE`、`OPENAI_COMPAT_BASE_URL`、`OPENAI_COMPAT_API_KEY`、`OPENAI_COMPAT_REQUEST_TIMEOUT_SECONDS`、`MODEL_DEFAULT_ID`、`MODEL_REGISTRY_ENTRIES`、`CHAT_GENERATION_TIMEOUT_SECONDS`、`CHAT_HISTORY_MESSAGE_LIMIT`、`CHAT_LEASE_TTL_SECONDS`。
  - `src/mem_ai/application.py`、`src/mem_ai/api/router.py`、`src/mem_ai/modules/memories/router.py`：挂载 `conversations_router`，lifespan 初始化并排空 `SyncGenerationTaskRegistry`、按需幂等同步 `model_registry_entries`、装配默认 `OpenAICompatibleModelProvider`，并将 `DatabaseConversationAccessChecker` 接线到 `/v1/memories`。
  - `src/mem_ai/db/model_registry.py` 与 `migrations/versions/20261122_0005_add_models_and_conversations_tables.py`：注册 ORM 模型并新增 `20261122_0005` 迁移（支持完整 `upgrade`、`alembic check` 与 `downgrade`）。

### 复用、扩展与职责分工

- 复用 01 的 `ModelProvider`/`PersonaProvider` 契约、`ApiResponse`/`ok_response`、`AppError` 体系、`Base` 约束命名、共享 `httpx.AsyncClient` 与 `ProviderRegistry` 装配。
- 复用 02 的 `AuthContextDep`、`users_service.get_active_user` / `ensure_user_active_in_session` 与 JSON 日志关联上下文（`bind_log_context`/`reset_log_context`）。
- 复用 03 的 `LocalPersonaProvider` 与不可变人设版本快照；Conversation Core 仅通过 `PersonaProvider` 只读访问人设，不直接依赖 `agents` 内部表结构。
- 通过 `application.py` 将 `DatabaseConversationAccessChecker` 注入 `app.state.conversation_access_checker`，消除 04 切片遗留的默认会话占空检查，同时保持 `memories` 与 `conversations` 零循环导入。

### 验证记录与剩余限制

- `.venv/bin/alembic upgrade head && .venv/bin/alembic check`：通过（升级至 `20261122_0005`，`No new upgrade operations detected.`）。
- `.venv/bin/pytest -q`：79 passed（含 `tests/unit/test_prompt_compiler.py`、`tests/unit/test_config.py`、`tests/integration/test_openai_compatible_model_adapter.py`、`tests/integration/test_chat_generation_persistence.py`、`tests/api/test_chat_api.py` 及 `tests/integration/test_database_and_migrations.py`）。
- `.venv/bin/ruff check .` 与 `.venv/bin/ruff format --check .`：All checks passed，124 files already formatted。
- `.venv/bin/mypy src`：Success: no issues found in 81 source files（strict 模式）。
- `python3 tools/check_architecture.py`：工程目录、功能索引、文档链接、分层导入、依赖图和已落地源码检查通过。
- 剩余范围：`/v1/models` 发现与请求覆盖策略（Ticket 06）、同会话并发租约冲突与过期接管（Ticket 07）、记忆召回注入（Ticket 08）、普通记忆任务原子入队（Ticket 09）、必填 `Idempotency-Key` 与 `idempotency_records`（Ticket 10）及 SSE 流式传输（Ticket 12）按依赖图在以上共同入口增量扩展。

### 目录与文档同步

- 已按 `backend-design` 技能同步 `docs/backend-scaffold-design.md` 第 3 节当前目录状态、目录树说明与职责表，同步 `.env.example` 与 `tests/api/test_health_and_foundation.py` 迁移头版本断言（`20261122_0005`）及 `tests/architecture/test_architecture_rules.py` 路由唯一性断言。
