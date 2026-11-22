# 08: 召回相关范围的记忆

**What to build:** 让模型在生成前使用当前用户和会话范围内的相关记忆，供应商故障时仍能完成聊天。

**Blocked by:** [04 查看和显式添加长期记忆](04-list-and-explicitly-create-memories.md), [05 发起和继续同步对话](05-start-and-continue-sync-chat.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用 04 的 memories 范围策略和 MemoryProvider，以及 05 的共同上下文准备与 Prompt 编译入口。
- memories 统一筛选相关记忆，对话持久化阶段保存实际注入的引用。
- 13 在同一记忆策略中补充 epoch、屏障和删除标记校验，同步和后续 SSE 共同使用。

## 验收条件

- [x] 用户记忆（`scope="user"`）可跨自己的会话，当前会话记忆（`scope="conversation"`）通过适配器过滤条件（如 MemOS `tags`/`info`）仅注入所属会话，其他会话与其他用户记忆始终被过滤。
- [x] 相关度、数量、去重和上下文预算规则确定可测，当前输入优先，冲突或低相关记忆被排除。
- [x] 召回失败降级为固定人设、成功历史和当前输入，错误日志脱敏。
- [x] 只为实际注入的记忆保存提供方 ID、记忆 ID、范围、相关度、位置和用户消息关联，引用只含元数据。
- [x] MemoryProvider 适配器封装供应商结构，注入内容使用统一格式，Prompt 测试和数据库关联测试覆盖降级和空结果。
- [x] 召回及引用写入可接入后续代次和 tombstone 校验，供删除切片过滤正在删除的记忆。

## Answer

### 实际公共符号与后续切片扩展入口

- **统一范围召回策略与代次/屏障/Tombstone 接缝（`src/mem_ai/modules/memories/recall.py`，经 `src/mem_ai/modules/memories/service.py` 公开再导出）**：
  - `MemoryBarrierState`、`MemoryRecallGuard`、`TombstoneLookup`、`MemoryRecallPolicy`、`MemoryRecallOutcome`。
  - `get_memory_epoch_in_session`：在短事务内只读获取 `(user_id, provider_id)` 当前 `memory_scopes.epoch`（缺省为 `0`），由 `conversations.generation.prepare_generation_phase` 在 Tx 1 捕获到 `generations.memory_epoch`。
  - `load_recall_guard_in_session`：加载 `memory_scopes`（`epoch`、`barrier_state`）并预留 `tombstone_lookup` 接缝（**Ticket 13 直接在此接入 `memory_tombstones` 查询，无需改动 Conversation Core 或适配器契约**）。
  - `filter_injectable_memories_in_session`：供 Tx 1.5 在保存实际注入引用前复查清空屏障、代次推进与 tombstone 状态，防召回在途发生清空/删除的 TOCTOU 竞态。
  - `select_memories_for_prompt`：纯确定性筛选管道，依次执行空白/范围归属防御校验、屏障/代次/tombstone 过滤、最低相关度阈值、`(-relevance, conversation 优先于 user, -memory_epoch, provider_id, memory_id)` 排序、外键与规范化正文去重、当前输入冗余与 `key: value` 冲突排除、数量上限与字符预算裁剪。
  - `recall_relevant_memories`：在数据库事务外按 `user` 与当前 `conversation` 两个范围调用 `MemoryProvider.recall`（受单次召回超时约束），遇到清空屏障、超时或供应商异常时记录脱敏日志（仅输出 `provider`/`scope`/`error_code`，绝不记录用户正文、查询文本或密钥）并安全降级为空记忆。
- **MemOS Cloud 范围过滤召回适配（`src/mem_ai/modules/memories/adapters/memos.py`）**：
  - 强化 `MemOSMemoryProvider.recall`：`user` 范围发送 `tags`（`memai:scope:user`）与 `info` 提升字段 `scene="scope:user"` 且省略 `conversation_id`；`conversation` 范围发送 `tags`（`memai:scope:conversation` + `memai:conv:<id>`）、`scene="scope:conversation"`、`business_type="conv:<id>"` 与 `conversation_id`；截断前先按相关度/代次/ID 排序（避免 `memory_detail_list` 挤掉更高相关度的 `preference_detail_list` 条目），将 `memory_limit_number`/`preference_limit_number` 限制在 MemOS 上限 25 内，兼容平铺与嵌套 `info` 字段，客户端二次过滤跨用户/错范围/低相关项。
- **元数据引用表、迁移与共同上下文阶段（`src/mem_ai/modules/conversations/`、`migrations/versions/20261122_0007_add_message_recalled_memories.py`）**：
  - `models.py`：`MessageRecalledMemory`（`message_recalled_memories` 表，仅存 `user_message_id`、`provider_id`、`memory_id`、`scope`、`relevance`、`prompt_position`、`memory_epoch`、`recalled_at`、`deleted_at`、`created_at`，**严禁保存记忆正文**；包含 `(user_message_id, provider_id, memory_id, scope)` 唯一约束 `uq_message_recalled_memories_msg_provider_mem_scope`、CHECK 约束及 `(user_message_id, prompt_position)` / `(provider_id, memory_id)` 索引）。
  - `schemas.py`：`RecalledMemoryReferenceRecord` 与扩展 `PreparedContext`（`memory_recall_degraded`、`memory_degradation_reason`）。
  - `prompt.py`：`CompiledPrompt` 与 `compile_chat_context`（预算优先级严格保证平台规则 + 固定人设 + 当前用户输入最先保留，其次近期成功历史，最后按序注入统一格式 `- [<scope>] <content>` 记忆并返回实际注入子集；`compile_chat_prompt` 委托同一实现）。
  - `repository.py`：`get_active_generation_by_lease`、`get_message_for_conversation`、`create_recalled_memory_references`、`list_recalled_memories_for_user_message`。
  - `generation.py`：
    - `prepare_generation_phase` 扩展可选 `memory_provider_id` 在 Tx 1 捕获 `memory_epoch`。
    - `persist_recalled_memories_phase`（短事务 Tx 1.5）：按 `users (FOR SHARE) -> conversations (FOR SHARE) -> generations` 复查用户/会话状态与未过期生成租约令牌、校验用户消息归属/生成/角色、执行 `filter_in_session` 守卫回调、落库 1-based `prompt_position` 元数据引用，并将唯一约束冲突转换为 `409 recalled_memory_conflict`。
    - `list_recalled_memories_for_message_for_owner`：拥有者只读查询接口（供测试、Ticket 13 核验及 Ticket 14 复用）。
  - `service.py`、`router.py`、`api/dependencies.py`、`core/config.py`、`core/logging.py`：
    - `api/dependencies.py` 新增 `get_optional_memory_provider` / `OptionalMemoryProviderDep`（记忆提供方未配置时聊天自动降级，不阻断 `POST /v1/chat`）。
    - `conversations/service.py` 扩展 `SyncChatOptions`、`sync_chat_options_from_settings`、`prepare_generation`、`prepare_context`（**同步与 Ticket 12 SSE 共用**）、`complete_prepared_sync_generation`（将上下文准备意外异常区分映射为 `503 generation_context_failed`，不误报为 `502 model_upstream_error`）与 `run_sync_chat`。
    - `core/config.py` 与 `.env.example` 新增 `CHAT_MEMORY_RECALL_TIMEOUT_SECONDS`、`CHAT_MEMORY_MIN_RELEVANCE`、`CHAT_MEMORY_MAX_ITEMS`、`CHAT_MEMORY_CONTEXT_MAX_CHARS`、`CHAT_PROMPT_MAX_CHARS`；`core/logging.py` 增补 `recall_query` 脱敏标记。

### 复用、迁移与职责分工

- 复用 04 的 `MemoryProvider` 契约、`memory_scopes` 表及 `MemOSMemoryProvider` 适配器，以及 05/07 的 `conversations/{prompt,generation,service}.py` 共享生成生命周期与租约校验条件。
- 严格遵循单向依赖：`conversations -> memories.service`（经公开服务入口协作，零跨模块持久化层访问与零内部深层导入），`memories` 与 `conversations` 保持零循环依赖。
- 经双轴 `code-review`（Standards & Spec）修复：消除 `conversations/service.py` 对 `memories.recall` 的深层导入、为 `persist_recalled_memories_phase` 补充唯一约束转换、移除未使用的 `conflict_checker` 投机参数、修复 MemOS 适配器截断前未按相关度排序与 `min_relevance=0.0` 不一致问题、统一多词空格属性键冲突匹配，并拆分上下文准备与模型调用的意外异常分类。

### 验证记录与剩余限制

- `.venv/bin/alembic upgrade head && .venv/bin/alembic check`：通过（升级至 `20261122_0007`，`No new upgrade operations detected.`）。
- `.venv/bin/pytest -q`：111 passed（新增 `tests/unit/test_memory_recall_policy.py`、`tests/integration/test_memory_recall_persistence.py`、`tests/api/test_chat_memory_recall_api.py`，扩展 `tests/unit/test_prompt_compiler.py`、`tests/unit/test_config.py`、`tests/integration/test_memos_adapter_contract.py` 并更新 `tests/api/test_health_and_foundation.py`）。
- `.venv/bin/ruff check .` 与 `.venv/bin/ruff format --check .`：All checks passed，137 files already formatted。
- `.venv/bin/mypy src`：Success: no issues found in 84 source files（strict 模式）。
- `python3 tools/check_architecture.py`：工程目录、功能索引、文档链接、分层导入、依赖图和已落地源码检查通过。
- **剩余范围**：`memory_tombstones` 实体表与真实单条/清空删除流程在 Ticket 13 接入 `load_recall_guard_in_session` 既有接缝；普通对话回合异步记忆写入在 Ticket 09 接入；SSE 流式路径在 Ticket 12 直接复用 `prepare_context`。

### 目录与文档同步

- 已按 `backend-design` 技能同步 `docs/backend-scaffold-design.md` 第 3 节当前目录状态、功能定位索引、目录树（新增 `modules/memories/recall.py`）与职责表，同步 `.env.example` 与 `tests/api/test_health_and_foundation.py` 迁移头版本断言（`20261122_0007`）。
