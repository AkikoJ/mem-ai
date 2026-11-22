# 06: 查询和选择可用模型

**What to build:** 让用户查询可用模型，并在服务端策略允许时覆盖 Agent 默认模型。

**Blocked by:** [05 发起和继续同步对话](05-start-and-continue-sync-chat.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 扩展 05 已使用的模型选择入口，模型列表与聊天选择使用相同的启用、权限及能力规则。
- 注册配置和供应商连接由 models 管理，生成准备阶段保存实际选择结果。
- 同步和后续流式请求通过同一模型策略决定能力与默认值。

## 验收条件

- [x] 模型查询只公开当前用户可用且启用的注册标识与能力，凭据及供应商内部连接配置保持服务端管理。
- [x] 路由优先级依次为服务端强制策略、被允许的请求模型、Agent 默认模型，省略模型复用 Agent 默认。
- [x] 注册模型、默认模型及能力通过服务端受控配置在启动或显式同步命令中幂等写入 `model_configs`，配置缺失返回稳定配置错误。
- [x] 未注册、停用、越权或不具备请求能力的模型在上游调用前拒绝；旧会话固定人设快照引用的默认模型已停用且请求省略 `modelId` 时返回稳定错误 `model_unavailable`，策略允许时调用方可显式传入其他已启用 `modelId` 继续。
- [x] 选定模型失败得到该次生成失败，测试断言调用的模型、次数和实际保存的 modelId。
- [x] 请求 API Key、Base URL 和任意供应商配置被 Schema 拒绝。

## Answer

### 实际公共符号与后续切片扩展入口

- **模型路由策略、发现与受控同步（`src/mem_ai/modules/models/`）**：
  - `src/mem_ai/modules/models/schemas.py`：`ModelConfigSeed`（严格 `extra="forbid"` 与空白字符校验）、`ModelRoutingPolicy`（`forced_model_id`/`allow_request_override`/`default_model_id`）、`ResolvedModelSelection`（新增 `max_context_tokens`/`capabilities`）、`ModelItemResponse`（仅公开 `modelId`/`displayName`/`supportsStreaming`/`maxContextTokens`/`maxOutputTokens`/`capabilities`，不暴露 `provider_id`/`upstream_model`/`credential_ref`/`required_permission`）与 `ModelListResponse`。
  - `src/mem_ai/modules/models/repository.py`：新增 `list_enabled_model_configs`（利用既有 `ix_model_configs_enabled_model_id` 索引按 `model_id ASC` 稳定输出）与 `has_any_model_config`（区分服务端未同步配置与单模型停用/未注册）。
  - `src/mem_ai/modules/models/service.py`：
    - `model_routing_policy_from_settings`（从 `Settings` 构造共享 `ModelRoutingPolicy`）。
    - `list_available_models`（与聊天选择共用同一启用、权限 `_is_model_permitted`、能力 `_missing_capabilities` 与服务端强制策略规则）。
    - `resolve_model_for_chat`（扩展 05 入口并移除被 `ModelRoutingPolicy.default_model_id` 替代的旧 `server_fallback_model_id` 参数，按 `server_forced > allowed request_override > agent_default > server_default` 优先级解析；未同步模型配置或服务端默认/强制配置缺失返回 `503 model_configuration_missing`，禁止覆盖返回 `403 model_override_not_allowed`，未注册或停用返回 `409 model_unavailable` 且对 Agent 固定默认模型不自动故障转移，缺少权限返回 `403 model_permission_denied`，缺少流式或其他能力返回 `422 model_capability_unsupported`）。
  - `src/mem_ai/modules/models/router.py`：`GET /v1/models`（通过 `AuthContextDep` 认证，支持可选 `?supportsStreaming=` 过滤，仅输出注册标识与能力），已挂载至 `src/mem_ai/api/router.py`。
  - `src/mem_ai/modules/models/sync.py`：`load_model_seeds_from_settings`、`load_model_seeds_from_file`、`run_sync_models` 与显式 CLI 入口 `python -m mem_ai.modules.models.sync`（及 `Makefile` 的 `make sync-models`），缺失配置返回 `503 model_configuration_missing` / 退出码 `1`。
- **对话准备阶段与配置接线**：
  - `src/mem_ai/modules/conversations/service.py`：`SyncChatOptions` 扩展 `model_policy`、`user_permissions`、`require_streaming`、`required_capabilities`，新增 `sync_chat_options_from_settings`；`prepare_generation` 将上述参数透传给 `models.service.resolve_model_for_chat`（供 06 同步与 12 流式共用同一能力与策略入口）。
  - `src/mem_ai/modules/conversations/router.py`：`POST /v1/chat` 通过 `sync_chat_options_from_settings` 注入服务端策略和当前用户权限。
  - `src/mem_ai/core/config.py`、`src/mem_ai/application.py`、`.env.example`：新增 `MODEL_FORCED_ID`、`MODEL_ALLOW_REQUEST_OVERRIDE`、`MODEL_SYNC_ON_STARTUP`，并在配置加载阶段校验 `model_registry_entries` 字段白名单与类型，拒绝凭据/URL 等非受控字段。

### 复用、扩展与职责分工

- 复用 05 落地的 `model_configs` 表、索引（无新数据库迁移）、`upsert_model_config`、`sync_model_configs`、`resolve_model_for_chat` 与 `OpenAICompatibleModelProvider`，以及 `ChatRequest` 的 `extra="forbid"` 严格 Schema。
- 模型发现（`GET /v1/models`）与对话选择（`POST /v1/chat` 及未来 12 SSE 流式准备）共享 `models.service` 内的权限、启用、能力与服务端强制策略规则，避免多处重复判断。
- `conversations/router.py` 仅调用本模块 `conversations.service.sync_chat_options_from_settings`，保持 Router 分层纯净。

### 验证记录与剩余限制

- `.venv/bin/alembic check`：通过（`No new upgrade operations detected.`，无需额外迁移）。
- `.venv/bin/pytest -q`：91 passed（新增 `tests/unit/test_model_policy.py`、`tests/integration/test_models_routing_and_sync.py`、`tests/api/test_models_and_chat_selection_api.py` 与 `tests/unit/test_config.py`、`tests/architecture/test_architecture_rules.py` 扩展用例）。
- `.venv/bin/ruff check .` 与 `.venv/bin/ruff format --check .`：All checks passed，129 files already formatted。
- `.venv/bin/mypy src`：Success: no issues found in 83 source files（strict 模式）。
- `python3 tools/check_architecture.py`：通过。
- 剩余范围：HTTP `POST /v1/chat` 的 `stream=true` 传输仍按 05 约定由 `422 streaming_not_enabled` 保护，待 Ticket 12 接入 SSE 时直接复用 `SyncChatOptions(require_streaming=True)` 与 `resolve_model_for_chat(..., require_streaming=True)`。

### 目录与文档同步

- 已按 `backend-design` 技能同步 `docs/backend-scaffold-design.md` 第 3 节当前目录状态、目录树（新增 `modules/models/sync.py` 与 06 落地说明）及职责表，同步 `.env.example` 与 `Makefile`（`make sync-models`）。
