# 03: 创建和使用私有 Agent

**What to build:** 让认证用户创建私有 Agent、发布首个人设版本，并从自己的列表及详情中取得可用于聊天的 Agent。

**Blocked by:** [02 接入认证上下文和用户主体](02-connect-auth-context-and-user-subject.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用 02 的可信身份和 01 的分页、错误处理与提供方装配。
- agents 统一拥有 Agent、唯一草稿（存 `agents` 行）、不可变人设版本快照（含 `defaultModelId`、`memoryPolicy.writeScope`）和发布事务；personas 的本地适配仅向对话内核提供只读 `PersonaProvider`。
- 本切片建立版本分配和首次发布入口，明确 `PATCH /agents/{id}` 改草稿、`POST /versions` 生成新版本快照、`POST /publish` 发布并切换当前发布引用；11 在该入口扩展草稿更新、并发发布及停用。

## 验收条件

- [x] 创建、首版发布、列表和详情通过公开 HTTP 路由验证，所有权来自认证用户。
- [x] Agent 草稿与不可变人设版本快照在 PostgreSQL 持久化（`persona_versions` 仅存不可变发布快照），重启后仍可读取。
- [x] 只读 `PersonaProvider` 使用项目自己的结构，替换适配器继续满足相同拥有者和版本快照契约。
- [x] 同一 Agent 版本号具有唯一约束，发布使用原子事务和条件更新。
- [x] 列表使用 limit、cursor、items、next_cursor、has_more 及稳定排序，覆盖同时间戳分页。
- [x] 其他用户读取、编辑或发布该 Agent 返回统一 404，客户端用户 ID 无法改变归属。

## Answer

### 实际公共符号与后续调用入口

- **提供方契约同步（`src/mem_ai/contracts/persona_provider.py`）**：
  - `MemoryPolicy` 新增向后兼容默认值 `write_enabled: bool = True`，与 `DESIGN.md` 对齐（`enabled`、`write_enabled`、`write_scope`、`recall_limit`）。
  - `PersonaSnapshot` 与 `PersonaProvider`（`get_latest_published`、`get_version`）保持项目自有只读结构。
- **Agent 与不可变人设版本管理（`src/mem_ai/modules/agents/`）**：
  - `src/mem_ai/modules/agents/models.py`：`Agent`（`agents` 表，含 `draft_*` 唯一草稿列、`status` CHECK 约束、`latest_version_number`/`revision` 内部并发条件更新列、`(current_published_version_id, id) -> persona_versions(id, agent_id)` 复合外键、`ix_agents_owner_id_created_at_id` 稳定分页索引）与 `PersonaVersion`（`persona_versions` 不可变快照表，含 `uq_persona_versions_agent_id_version`、`uq_persona_versions_id_agent_id`、`ck_persona_versions_version_positive` 与首次发布写入的 `published_at`）。
  - `src/mem_ai/modules/agents/schemas.py`：不可变应用 DTO（`AgentDraftRecord`、`PersonaVersionRecord`、`AgentSummaryRecord`、`AgentRecord`）与严格 `camelCase` HTTP 请求/响应 Schema（`AgentCreateRequest`、`AgentUpdateDraftRequest`、`AgentPublishRequest`、`MemoryPolicyPayload`、`MemoryPolicyPatch`、`MemoryPolicyResponse`、`AgentSummaryResponse`、`AgentDetailResponse`、`PersonaVersionResponse`，所有请求模型启用 `extra="forbid"` 并校验非空与 `exclude_unset` 语义）。
  - `src/mem_ai/modules/agents/repository.py`：显式 Session 仓储，含 `(created_at DESC, id DESC)` keyset 游标查询、条件更新（`conditional_update_draft`、`conditional_advance_latest_version`、`conditional_publish_agent`）、`mark_persona_version_published` 与所有权及已发布过滤只读查询。
  - `src/mem_ai/modules/agents/service.py`：
    - 写用例：`create_agent`、`update_agent_draft`、`create_persona_version`、`publish_agent`（全部在事务内复查 `users_service.ensure_user_active_in_session`，发布与版本分配通过 `(agent_id, version)` 唯一约束 + 条件更新保护，复用 `mem_ai.db.base.is_unique_violation` 将并发冲突转换为 `409 agent_publish_conflict` / `agent_version_conflict` / `agent_revision_conflict`）。
    - 读用例：`get_agent`、`list_agents`（复用 `mem_ai.api.pagination` 游标编解码校验）、`get_latest_published_version_for_owner`、`get_persona_version_for_owner`（仅返回已发布版本，供 `personas.provider` 只读委托）。
  - `src/mem_ai/modules/agents/router.py`（挂载于 `src/mem_ai/api/router.py` 的 `/v1` 前缀）：
    - `POST /v1/agents`（`201`）、`GET /v1/agents`（`200`）、`GET /v1/agents/{agentId}`（`200`）、`PATCH /v1/agents/{agentId}`（`200`）、`POST /v1/agents/{agentId}/versions`（`201`）、`POST /v1/agents/{agentId}/publish`（`200`，支持空请求体或 `{"versionId"}`）。
- **共享数据库错误识别（`src/mem_ai/db/base.py`）**：
  - 提取 `is_unique_violation(exc, constraint_name=...)` 并将 `users/service.py` 与 `agents/service.py` 统一迁移到该共享入口。
- **本地只读人设适配与装配（`src/mem_ai/modules/personas/`、`src/mem_ai/application.py`）**：
  - `src/mem_ai/modules/personas/provider.py`：`LocalPersonaProvider(session_factory=...)`，单向委托 `agents.service` 只读入口并转换为 `PersonaSnapshot`（`get_latest_published` 仅返回 `active` 且已发布快照；`get_version` 允许按固定版本读取含已停用 Agent 的历史快照）。
  - `src/mem_ai/application.py`：lifespan 在调用方未显式注入 `persona_provider` 时默认装配 `LocalPersonaProvider(session_factory=session_factory)`，保留 `ProviderRegistry` 注入与 `app.dependency_overrides[get_persona_provider]` 替换接缝。
  - `src/mem_ai/db/model_registry.py` 与 `migrations/versions/20261121_0003_add_agents_and_persona_versions.py`：注册 ORM 模型并新增 `20261121_0003` 迁移（支持完整 `upgrade`、`alembic check` 与 `downgrade`）。

### 复用、扩展与职责分工

- 复用 01 的 `ApiResponse`/`ok_response`、`PaginatedResponse`/`CursorPayload`/`encode_cursor`/`decode_cursor`/`hash_filters`、`AppError` 体系、`Base`/`UUIDPrimaryKeyMixin`/`CreatedAtMixin`/`TimestampMixin` 与 `ProviderRegistry` 装配，未引入重复实现。
- 复用 02 的 `AuthContextDep`、`users_service.ensure_user_active_in_session` 与 `test_auth_adapter`。
- 严格保持 `personas -> agents.service` 单向依赖与 `agents.router -> agents.service` 分层，不跨模块访问 `repository`/`models`。

### 验证记录与剩余限制

- `.venv/bin/alembic upgrade head && .venv/bin/alembic check`：通过（升级至 `20261121_0003`，`No new upgrade operations detected.`）。
- `.venv/bin/pytest -q`：51 passed（含 `tests/integration/test_agents_persistence.py` 重启持久化、唯一约束、并发发布条件更新与同时间戳游标分页；`tests/integration/test_persona_provider_contract.py` 本地与替换适配器一致契约及默认装配/覆盖；`tests/api/test_agents_api.py` 全链路 HTTP 路由、跨用户统一 404 与客户端身份防篡改）。
- `.venv/bin/ruff check .` 与 `.venv/bin/ruff format --check .`：All checks passed，80 files formatted。
- `.venv/bin/mypy src`：Success: no issues found in 46 source files（strict 模式）。
- `python3 tools/check_architecture.py`：工程目录、功能索引、文档链接、分层导入、依赖图和已落地源码检查通过。
- 剩余范围：Agent 停用路由（`DELETE /v1/agents/{agentId}`）以及与真实对话 Prompt/固定会话版本联动的并发测试按依赖图留待 11 切片扩展，已在 03 预留 `status`/`retired_at`/`revision` 字段与条件更新基础。

### 目录与文档同步

- 已按 `backend-design` 技能同步 `docs/backend-scaffold-design.md` 第 3 节当前目录状态、功能定位索引（登记 `src/mem_ai/modules/personas/provider.py` 协作路径）、目录说明与模块职责表，并同步 `tests/api/test_health_and_foundation.py` 中的迁移头版本断言至 `20261121_0003`。
