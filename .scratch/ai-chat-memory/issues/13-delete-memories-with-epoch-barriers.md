# 13: 删除与清空记忆并阻止旧任务回写

**What to build:** 让用户删除单条或清空记忆，立即停止相关召回，并通过可查询操作确保旧写入最终收敛。

**Blocked by:** [04 查看和显式添加长期记忆](04-list-and-explicitly-create-memories.md), [08 召回相关范围的记忆](08-recall-relevant-scoped-memories.md), [09 持久化任务并重试记忆写入](09-persist-and-retry-memory-writes.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 扩展 04、08 的 memories 策略和 09 的记忆处理器，统一拥有代次、屏障、删除标记及写入资格检查。
- operations 提供操作创建、进度和终态入口，jobs 承担任务执行规则，资源处理器通过这些入口报告结果。
- 对话成功提交通过同一策略检查旧代次入队资格，14、15 继续复用该策略完成范围清理。

## 验收条件

- [x] 单条删除与 confirm=true 清空先保存删除操作，确认完成返回 204，执行中或结果待确认返回 202、operationId 和查询地址。
- [x] 操作查询返回 pending、running、reconciling、succeeded 或 failed，确定失败映射稳定错误，其他用户读取返回 404。
- [x] 单条删除建立 tombstone，之后召回过滤该记忆，历史引用保留并标为删除或无法解析。
- [x] 清空以用户和提供方范围原子递增 epoch、建立屏障并取消旧代次排队任务，生成和写入捕获代次，在途写入登记与清空使用相同范围锁串行。
- [x] 屏障中召回降级为空，显式创建返回 409 memory_scope_clearing，新普通写入暂停，旧生成完成时抑制其旧代次任务。
- [x] 显式创建和普通任务的在途外部写入均登记与核验；具备单次状态查询能力时等待结果稳定后最终清空并解除屏障，缺乏单次查询能力时等待完成或达到 `memory_inflight_settle_timeout` 后保留 `reconciling` 告警并执行最终清空与延迟二次补偿清空，避免单次超时永久锁死屏障。最终清空本身结果未知时保持屏障待确认与告警。
- [x] 删除操作与代次递增遵循后端工程设计第 11.3.1 节显式状态机，并接入可选的独立删除审计归档导出接缝；删除处理器复用统一任务框架，使用稳定操作关联和提供方结果核验，重复执行可恢复。
- [x] 受控上游及 PostgreSQL 测试覆盖排队、已发送写入、丢响应、迟到完成、显式创建竞争、旧生成收尾和重启恢复。

## Answer

### 1. 实际公共符号与文件路径（供 14、15 复用）

- **配置与数据库迁移：**
  - `src/mem_ai/core/config.py` / `.env.example`：新增 `memory_inflight_settle_timeout`（默认 `60.0`）、`memory_clear_compensation_delay_seconds`（默认 `30.0`）与 `deletion_audit_archive_path`（默认 `None`，可选独立审计归档路径）。
  - `src/mem_ai/modules/memories/models.py` / `migrations/versions/20261123_0010_add_memory_tombstones.py`：新增 `MemoryTombstone`（`memory_tombstones` 表，含 `status`/`target_kind`/`memory_epoch >= 0` 检查约束、`(user_id, provider_id, memory_id)` 与 `(user_id, provider_id, conversation_id)` 部分唯一索引及查询索引）。
- **提供方协议与 MemOS 适配器：**
  - `src/mem_ai/contracts/memory_provider.py`：新增 `DELETE_MEMORY_REF_PREFIX`、`CLEAR_MEMORIES_REF_PREFIX`、`build_delete_memory_external_ref`、`build_clear_memories_external_ref` 与 `provider_supports_write_status_verification(provider)` 能力判定辅助函数。
  - `src/mem_ai/modules/memories/adapters/memos.py`：新增 `supports_write_status_verification` 声明；`delete_memory` 在调用 `/delete/memory` 前执行归属校验（遇超时或超出 `list_max_items` 上限时失败关闭，防止跨用户删除与未发送即进入 `reconciling`）；`verify_operation` 扩展支持单条删除与用户清空的外部状态核验（遇 `list_max_items` 截断保持 `reconciling`）。
- **操作状态机与独立删除审计归档接缝：**
  - `src/mem_ai/modules/operations/service.py` / `repository.py`：落地 §11.3.1 显式状态机 `ALLOWED_OPERATION_TRANSITIONS`、`is_valid_operation_transition`、`allowed_operation_source_statuses`、`get_operation_in_session`、`attach_job_in_session`，并在 SQL 条件更新中强制来源状态约束。
  - `src/mem_ai/modules/operations/audit.py`：定义 `DeletionAuditEvent`、`DeletionAuditArchiver`、`NoopDeletionAuditArchiver`、`JsonlDeletionAuditArchiver`（追加写 + fsync + `sanitize_log_value` 脱敏）、`build_deletion_audit_archiver` 与 `export_deletion_audit_events_safely`。
- **通用任务与会话引用接缝：**
  - `src/mem_ai/modules/jobs/{schemas,repository,service}.py`：扩展 `JobAfterCommitHook`（事务提交后执行）、`list_user_jobs_in_session`、`cancel_matching_jobs_in_session` 与 `update_reconciling_job_in_session`（支持审计延期）。
  - `src/mem_ai/modules/conversations/{repository,queries,generation}.py`：新增 `mark_recalled_memories_deleted_for_memory`、`mark_recalled_memories_deleted_for_user_epoch`、`DatabaseRecalledMemoryReferenceMarker`（单向依赖实现 `RecalledMemoryReferenceMarker` 协议），并在 `prepare_generation_phase` 中以 `for_share=True` 捕获 `memory_epoch`。
- **记忆删除、代次屏障与 Worker 处理器：**
  - `src/mem_ai/modules/memories/barrier.py`：集中实现 §11.3.1 屏障与 tombstone 状态机（`ALLOWED_BARRIER_TRANSITIONS`、`ALLOWED_TOMBSTONE_TRANSITIONS`、`transition_scope_barrier_in_session`、`transition_tombstone_in_session`）、唯一错误映射（`map_memory_provider_error`、`is_retryable_memory_provider_error`）、`RecalledMemoryReferenceMarker` 协议、在途旧代次写入取消与收敛判断（`cancel_queued_and_list_unsettled_writes_in_session`、`is_write_job_settled_for_clear`）及原子屏障释放入口（`finalize_clear_barrier_in_session`，14/15 可继续复用）。
  - `src/mem_ai/modules/memories/deletions.py`：`delete_single_memory`（tombstone 先行、历史引用标记、同目标操作去重、幂等 204 / 异步 202）与 `MemoryDeleteJobHandler`（`JOB_TYPE_MEMORY_DELETE = "memory_delete"`）。
  - `src/mem_ai/modules/memories/clearing.py`：`clear_user_memories`（校验 `confirm=true` 否则返回 `422 clear_confirmation_required`、`memory_scopes FOR UPDATE` 原子递增 epoch 并建立屏障、取消旧代次排队任务、无在途写入时走同步快速路径、遇瞬时错误转入 `retry_wait` 自动恢复、有在途写入转交 Worker）。
  - `src/mem_ai/modules/memories/clear_jobs.py`：`MemoryClearJobHandler`（`JOB_TYPE_MEMORY_CLEAR = "memory_clear"`），按提供方是否支持单次写入状态查询分别执行 `verify_operation` 稳定收敛或 `memory_inflight_settle_timeout` 告警 + 延迟二次补偿清空（`compensation_clear`），最终清空失败/未知时保持屏障 `reconciling` 并在重试耗尽时推进 `operations` 至 `failed` 避免死锁。
  - `src/mem_ai/modules/memories/recall.py` / `service.py` / `jobs.py` / `router.py`：`load_recall_guard_in_session` 默认接入真实 `memory_tombstones` 查询；`list_memories` 实时过滤 tombstone 与旧代次记录；`TurnMemoryWriteJobHandler` 与 `ExplicitMemoryReconcileJobHandler` 接入 `confirmed_during_clear` 自取消与审计保留保护；路由新增 `DELETE /v1/memories/{memoryId}` 与 `DELETE /v1/memories?confirm=true`。
  - `src/mem_ai/application.py` / `src/mem_ai/workers/main.py`：装配 `DatabaseRecalledMemoryReferenceMarker`、`build_deletion_audit_archiver` 并在 `build_worker_registry` 注册 `MemoryDeleteJobHandler` 与 `MemoryClearJobHandler`。

### 2. 复用与代码评审收敛

- 复用 04/08 的 `memory_scopes`、`operations`、`GET /v1/operations/{operationId}`、`load_recall_guard_in_session` 与 09 的统一 PostgreSQL `jobs` / `WorkerRunner` 框架，并遵循严格锁序（`users FOR SHARE -> conversations -> memory_scopes FOR UPDATE/SHARE -> jobs FOR UPDATE`）。
- 同步与流式生成继续共用 `run_prepared_generation_lifecycle` 与 `build_turn_memory_enqueue_hook`，在清空屏障或旧代次完成后统一抑制旧任务入队。
- 通过双轴代码评审（Standards & Spec）并修复了发现项：合并重复错误映射为唯一 `map_memory_provider_error`；将删除/清空外部引用约定收敛至 `contracts/memory_provider.py`；修复 MemOS 适配器在 `list_max_items` 截断及删除前查询超时时的失败关闭逻辑；修复验证重试耗尽时 `operations` 未推进至 `failed` 的死锁风险；修复同步清空快速路径遇瞬时错误未转交 Worker 重试以及显式创建迟到提交在屏障已释放时可能遗留孤儿 `reconciling` 操作的问题；补充流式旧代次抑制回归测试。

### 3. 验证结果与剩余限制

- `uv run alembic upgrade head`：已升级至 `20261123_0010`。
- `uv run alembic check`：`No new upgrade operations detected.`
- `uv run ruff format --check . && uv run ruff check .`：166 files already formatted，0 errors。
- `uv run mypy src`：`Success: no issues found in 97 source files`。
- `python3 tools/check_architecture.py`：工程目录、功能索引、文档链接、分层导入、依赖图与文件规模（`<= 800` 行）全部通过。
- `uv run pytest -q`：**166 passed**（覆盖 `tests/unit/test_memory_deletion_policy.py`、`tests/integration/test_memory_deletion_barriers.py`、`tests/api/test_memory_deletion_api.py`、`tests/integration/test_memos_adapter_contract.py` 及全部已有测试）。
- 剩余边界：会话级删除清理（`DELETE /v1/conversations/{conversationId}`）将在 Ticket 14 落地，用户删除与跨资源分批清理将在 Ticket 15 落地，周期保留与到期清理将在 Ticket 16 落地。

### 4. 文档同步情况

- 已按 `backend-design` 技能同步更新 `docs/backend-scaffold-design.md` 第 3.0.1 节当前目录状态、第 3.0.2 节功能定位索引、第 3 节目录树（新增 `operations/audit.py`、`memories/{barrier,deletions,clearing,clear_jobs}.py`）与第 3.2 节职责表。
