# 11: 更新人设版本和停用 Agent

**What to build:** 让拥有者更新草稿、发布新版本和停用 Agent，旧会话持续使用原版本。

**Blocked by:** [05 发起和继续同步对话](05-start-and-continue-sync-chat.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 扩展 03 的 Agent 生命周期、版本分配和发布事务，管理路由与 PersonaProvider 适配使用同一实现。
- 复用 05 的版本快照读取，新会话解析最新发布版本，已有会话读取固定版本。
- owner、版本竞争和停用规则集中在 agents，personas 负责协议适配。

## 验收条件

- [x] 草稿修改保留既有发布快照，新会话使用最新发布版本，已有会话使用固定版本。
- [x] 公开更新、版本创建、发布和停用路由具有明确 Schema 与稳定状态码，用户 ID 来自认证上下文。
- [x] 停用 Agent 阻止新会话，已有会话可继续，记录及已发布人设在重启后可读取。
- [x] 并发编辑、版本号、当前发布引用及停用以唯一约束和条件更新保护，状态冲突使用 409。
- [x] 跨用户编辑、发布和停用返回 404，重复停用是幂等成功。
- [x] 可控模型和 PostgreSQL 测试验证发布前后 Prompt、并发状态及重复操作。

## Answer

### 实际公共符号与后续切片调用入口

- **Agent 仓储条件停用与实时状态查询（`src/mem_ai/modules/agents/repository.py`）**：
  - `conditional_retire_agent(session, *, agent_id, owner_id, expected_revision, retired_at)`：按 `(id, owner_id, status='active', revision=expected_revision)` 条件更新将 Agent 迁移为 `retired`，通过 `coalesce(retired_at, :retired_at)` 保留首次停用时间并递增 `revision`、`updated_at`。
  - `get_agent_status_for_owner(session, *, owner_id, agent_id)`：绕过 ORM Identity Map 直接读取 PostgreSQL 当前已提交的 `agents.status`，用于并发条件更新未命中时精确区分并发停用（`409 agent_retired`）、并发修改冲突（`409`）与资源缺失（`404 agent_not_found`）。
  - `get_agent_with_published_version(..., populate_existing=False)`：支持在条件更新未命中后以 `populate_existing=True` 刷新读取已提交行，确保并发重复停用幂等返回胜出方的 `retired_at`。
- **Agent 生命周期服务扩展（`src/mem_ai/modules/agents/service.py`）**：
  - `retire_agent(session_factory, *, owner_id, agent_id) -> AgentRecord`：事务内校验用户启用状态（`users_service.ensure_user_active_in_session`），跨用户或不存在返回统一 `404 agent_not_found`；对已停用 Agent 直接幂等返回且不改写 `retired_at`/`revision`；并发重复停用时重读后按幂等成功返回同一 `retired_at`；并发草稿更新/版本分配/发布先提交导致 `revision` 变化时返回 `409 agent_retire_conflict`（**供 Ticket 15 用户删除清理与内部生命周期调用**）。
  - `_raise_conditional_update_conflict` 与 `_to_agent_record_from_rows`：统一 `update_agent_draft`、`create_persona_version` 与 `publish_agent` 的条件更新冲突判定（并发被停用返回 `409 agent_retired` 并完整回滚未提交的 `persona_versions` 插入，并发版本/编辑冲突返回 `409 agent_revision_conflict`/`agent_version_conflict`/`agent_publish_conflict`，并发删除返回 `404 agent_not_found`）。
- **公开 HTTP 停用路由与 Schema（`src/mem_ai/modules/agents/router.py`、`src/mem_ai/modules/agents/schemas.py`）**：
  - 新增 `DELETE /v1/agents/{agentId}`（`200 OK`，`ApiResponse[AgentDetailResponse]`），返回 `status="retired"`、`retiredAt` 及保留的 `publishedVersion`/`draft`，所有权固定来自 `AuthContextDep`，重复停用幂等成功，跨用户与不存在均返回统一 `404 agent_not_found`。
- **只读人设适配与对话固定版本复用（`src/mem_ai/modules/personas/provider.py`、`src/mem_ai/modules/conversations/service.py`）**：
  - 复用 03/05 已落地的 `LocalPersonaProvider.get_latest_published`（仅返回 `active` 且已发布快照，停用后自动拦截新会话返回 `404 agent_persona_not_found` 且不触发模型调用）与 `LocalPersonaProvider.get_version`（允许已停用 Agent 的既有会话继续按固定快照读取人设、默认模型与记忆策略；**Ticket 12 流式聊天直接复用同一解析入口**）。

### 复用、迁移与双轴代码审阅修复

- 复用 03 的 `agents`/`persona_versions` 表结构（无需新增迁移，已有 `status`、`retired_at`、`revision`、复合外键与 `(agent_id, version)` 唯一约束）及 `update_agent_draft`/`create_persona_version`/`publish_agent`；迁移 `tests/integration/test_persona_provider_contract.py` 中的 `_retire_in_db` 从直接 SQL `UPDATE` 改用公开入口 `agents_service.retire_agent`。
- 经双轴 `code-review`（Standards & Spec）修复：
  1. 同步 `docs/backend-scaffold-design.md` §8.1 与 §8.2，明确 `DELETE /v1/agents/{agentId}` 属于 `active -> retired` 状态迁移并返回 `200 OK` 与 `AgentDetailResponse`。
  2. 在 `agents/service.py` 抽离 `_to_agent_record_from_rows` 消除 5 处重复的行转换逻辑，并统一 `_raise_conditional_update_conflict` 与 `retire_agent` 对并发缺失行返回 `404 agent_not_found` 的语义。
  3. 在 `tests/integration/test_agents_persistence.py` 抽离 `_race_with_get_with_published_barrier` 与 `_race_pausing_before_repo_call`，消除重复并发测试脚手架。

### 验证记录与剩余限制

- `.venv/bin/alembic check`：通过（`No new upgrade operations detected.`，无结构漂移）。
- `.venv/bin/pytest -q`：**137 passed**（新增 `tests/api/test_agent_versioning_and_retirement_api.py` 验证发布前后与停用后新/旧会话 Prompt、默认模型切换、幂等回放及重启持久化；扩展 `tests/api/test_agents_api.py` 验证 `DELETE /v1/agents/{agentId}` Schema、重复停用幂等、跨用户统一 404、防伪造身份、停用后 409 拒绝变更与重启可读；扩展 `tests/integration/test_agents_persistence.py` 验证确定性并发 `PATCH`、`POST /versions`、`publish` 与 `retire` 双向竞争的 409 冲突、事务回滚与并发重复停用幂等）。
- `.venv/bin/ruff check .` 与 `.venv/bin/ruff format --check .`：All checks passed，152 files already formatted。
- `.venv/bin/mypy src`：Success: no issues found in 90 source files（strict 模式）。
- `python3 tools/check_architecture.py`：工程目录、功能索引、文档链接、分层导入、依赖图和已落地源码检查通过。
- **剩余范围**：流式对话（SSE）对固定人设与停用规则的复用将在 Ticket 12 验证；用户删除时对私有 Agent 与人设版本的物理分批清理将在 Ticket 15 接入。

### 目录与文档同步

- 已按 `backend-design` 技能同步 `docs/backend-scaffold-design.md` 第 3 节当前目录状态、目录树说明与模块职责表，并同步 §8.1/§8.2 中 `DELETE /v1/agents/{agentId}` 返回 `200 OK` 与 `AgentDetailResponse` 的接口契约。
