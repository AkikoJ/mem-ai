# 01: 将人设只读快照收口至 agents 模块并移除过渡适配层

**What to build:** 将不可变人设版本快照类型与按拥有者/版本查询的公开只读服务完全归口到 `agents` 模块，让对话核心直接从 `agents` 获取人设快照；移除过渡 `personas` 模块、`PersonaProvider` 协议与对应测试替身，使私有 Agent 的草稿隔离、版本发布、停用及旧会话续聊在同一领域拥有者下保持一致。

**Blocked by:** None (can start immediately)

**Status:** resolved

**Parent:** [一体化单进程 Python 后端体系收敛规格](../spec.md)

## 实现边界

- 复用 `agents` 既有的私有 Agent 归属策略、不可变人设版本快照持久化与对话核心准备阶段。
- 在 `agents` 公开服务与数据模型中承接最新已发布快照和固定版本快照的只读解析，并在写事务外供 `conversations` 调用。
- 移除 `personas` 模块、`PersonaProvider` 协议、应用装配中对应提供方槽位以及人设测试替身；保持跨模块持久化边界不变（`conversations` 仍不得访问 `agents` 的仓储或 ORM 模型）。

## 验收条件

- [x] `agents` 模块提供完整的不可变人设快照类型与只读快照查询入口（按拥有者获取最新已发布版本及按版本 ID 获取固定版本），且跨用户查询仍隔离返回未找到。
- [x] 对话核心直接调用 `agents` 公开只读入口解析人设，同步与流式聊天、幂等回放、草稿编辑不影响已有会话、新版本发布仅作用于新会话以及停用 Agent 后旧会话继续续聊继续使用同一查询路径。
- [x] `personas` 过渡模块、`PersonaProvider` 协议、`ProviderRegistry` 中的人设槽位及 `FakePersonaProvider` 已彻底移除，应用装配与测试夹具仅保留面向外部引擎的 `ModelProvider` 与 `MemoryProvider`。
- [x] 工程目录树、当前目录状态、功能定位索引、模块职责表和 ADR-0007 同步更新；架构检查、Ruff、mypy 与全量 pytest（含 PostgreSQL 集成测试）全部通过。

## Answer

- `mem_ai.modules.agents.schemas` 现在拥有不可变 `PersonaSnapshot`、`MemoryPolicy` 和 `MemoryWriteScope`（`PersonaSnapshot` 与 `PersonaVersionRecord` 共享 `_PersonaVersionBase` 字段定义并支持 `from_attributes=True`）；`mem_ai.modules.agents.service.get_latest_published_version_for_owner` 与 `get_published_persona_version_for_owner` 分别按 Agent 所有者读取最新已发布和固定已发布版本快照。对话核心在写事务外直接调用这两个公开入口；快照查询仍按所有者与 `agent_id` 双重过滤，跨用户或错 `agent_id` 访问均返回未找到。
- 移除了 `src/mem_ai/modules/personas/`、`src/mem_ai/contracts/persona_provider.py`、`ProviderRegistry` 人设槽位、依赖函数、应用装配适配器、Fake 测试替身及旧契约测试。现有 API/集成测试改为创建并发布真实 Agent 后通过 `agents.service` 读取快照，并在 `tests/integration/test_agents_persistence.py` 补齐 `PersonaSnapshot` 类型校验与错 `agent_id` 查已有 `version_id` 返回 `None` 的负例；同时修正 `tests/conftest.py` 的 `empty_pg_settings` 隔离本地 `.env` 外部提供方与模型同步配置；跨模块 Repository/ORM 隔离保持不变。
- 同步了工程目录与功能索引、模块所有权、调用职责、对话事务步骤、收敛阶段、ADR-0007、`DESIGN.md`、`spec.md` 的 `Implementation Plan` 及 `README.md` 合并索引状态，并清理了 `§3.1` 残留 `personas` 说明、字段名冲突与禁用术语。
- 验证：`python3 tools/check_architecture.py` 通过；`uv run ruff check .` 与 `uv run ruff format --check .` 通过；`uv run mypy src` 通过；全量 `uv run pytest -q` 在本地 PostgreSQL 下 `237 passed`（0 failed、0 errors）。未提交代码。
