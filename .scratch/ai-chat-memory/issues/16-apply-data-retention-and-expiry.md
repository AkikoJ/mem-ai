# 16: 执行数据保留与到期清理

**What to build:** 让过期幂等和任务执行日志自动清理，同时保留活跃工作及删除恢复必需的数据。

**Blocked by:** [10 支持聊天幂等与生成结果查询](10-recover-idempotent-chat-generations.md), [15 删除用户及关联数据](15-delete-user-and-associated-data.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用 jobs 的周期任务去重和执行框架，资源清理由所属模块提供可恢复批量入口。
- retention 负责期限、扫描和批次选择，幂等到期语义、删除完成判断分别沿用 idempotency 与 operations。
- 清理测试沿用既有状态规则，覆盖活动生成、待核验操作和外部去重记录的保护。

## 验收条件

- [x] 服务端配置规定会话和生成默认长期、任务执行日志 30 天、终态幂等 24 小时、分页快照（`memory_list_snapshots`）按配置短期过期，已删除记忆历史引用随会话保留。
- [x] 终态幂等从终态提交时间计算，running 记录和活跃生成保留，过期键重新使用按新请求处理。
- [x] 周期任务复用统一框架，按稳定游标有限批次扫描、幂等删除，多个 Worker 同周期去重。
- [x] 待确认外部结果、未完成操作、远程映射及提供方去重信息在仍需要时保留，回执和操作摘要按配置最小化清理。
- [x] 显式会话与用户删除优先于长期保留，清理结果只记录脱敏计数、耗时和失败摘要。
- [x] PostgreSQL 测试覆盖 24 小时与 30 天边界、迟到终态、分批中断、重复执行、活跃工作和待确认操作保护。

## Answer

### 实际公共符号与后续切片调用入口

- **配置与保留策略（`src/mem_ai/core/config.py`、`.env.example`、`src/mem_ai/modules/retention/schemas.py`）**：
  - `Settings` 新增 `conversation_retention_policy: Literal["indefinite"] = "indefinite"`、`operation_summary_retention_days = 30`、`retention_cleanup_interval_seconds = 900`、`retention_batch_size = 100`、`retention_max_batches_per_run = 50`（沿用已有 `job_attempt_retention_days = 30`、`idempotency_retention_hours = 24`、`memory_list_snapshot_ttl_seconds = 600` 与 `deletion_receipt_ttl_seconds = 2592000`）。
  - `RetentionPolicy`、`retention_policy_from_settings`、`RETENTION_STAGES`、`RetentionCursor`、`RetentionJobMetadata`、`RetentionBatchResult`、`RetentionSweepSummary`。
- **资源拥有者可恢复有限批次清理与保护入口**：
  - `src/mem_ai/modules/conversations/idempotency.py`：`IdempotencyPurgeBatch`、`purge_expired_idempotency_batch_in_session`（按 `(expires_at, id)` 游标 + `SKIP LOCKED` 清理已过期的 `succeeded`/`failed`/`cancelled` 幂等记录，排除 `running` 及活跃生成，保留父会话/生成/消息/已删除记忆引用）。
  - `src/mem_ai/modules/jobs/cleanup_repository.py`、`src/mem_ai/modules/jobs/cleanup.py`、`src/mem_ai/modules/jobs/repository.py`、`src/mem_ai/modules/jobs/service.py`：`checkpoint_job_metadata_in_session`（租约条件元数据检查点）、`find_active_or_unsettled_job_ids_in_session`、`purge_expired_job_attempts_batch_in_session`（清理 30 天到期执行日志，保留 `running`/未完成/非终态任务及非终态操作关联尝试，保留承载 `dedupe_key`/`external_ref` 的业务 `jobs` 行）、`purge_expired_periodic_jobs_batch_in_session`（仅清理 30 天前已完成的系统周期任务）；`finish_job_attempt` 与 `finish_running_attempts_for_lease` 在尝试完成时按 `greatest(expires_at, finished_at + attempt_retention_days)` 保护迟到终态尝试，并同步从 `memories/router.py` 透传 `settings.job_attempt_retention_days` 到同步记忆任务。
  - `src/mem_ai/modules/memories/snapshots.py`、`src/mem_ai/modules/memories/barrier.py`、`src/mem_ai/modules/memories/repository.py`：`MemorySnapshotPurgeBatch`、`purge_expired_list_snapshots_batch_in_session`（按 `(expires_at, id)` 清理过期 `memory_list_snapshots`，永不清理 `memory_scopes`/`memory_tombstones`）与 `find_protected_operation_ids_in_session`（保护活跃屏障或未核验 tombstone 关联的操作）。
  - `src/mem_ai/modules/operations/models.py`、`repository.py`、`service.py` 与迁移 `migrations/versions/20261124_0012_add_operations_retention_indexes.py`：新增部分组合索引 `ix_operations_expires_at_id`、`ix_operations_completed_at_id`，提供 `find_non_terminal_operation_links_in_session`、`purge_expired_user_delete_operations_batch_in_session`（仅清理已 `succeeded` 且 `owner_id IS NULL` 的到期用户删除回执操作）和 `purge_expired_operation_summaries_batch_in_session`（仅清理已收敛且未受活跃屏障/未核验 tombstone/非终态任务保护的到期操作摘要）。
- **周期保留清理编排与 Worker 装配（`src/mem_ai/modules/retention/service.py`、`src/mem_ai/workers/main.py`、`src/mem_ai/workers/runner.py`）**：
  - `JOB_TYPE_RETENTION_CLEANUP = "retention_cleanup"`、`run_retention_batch_in_session`、`run_retention_sweep`、`RetentionCleanupJobHandler`（每批独立短事务同步落盘 `(stage_index, cursor, totals)` 检查点，单次触达 `max_batches_per_run` 上限时以 `reconciling` 让出并续扫避免后续阶段饿死，失败保留检查点可重试，结果与 JSON 日志仅输出脱敏计数、耗时与安全失败摘要）。
  - `workers/main.py` 在 `build_worker_registry` 注册 `RetentionCleanupJobHandler` 并在 `default_periodic_jobs` 启用窗口去重的 `retention_cleanup` 周期任务。

### 复用、迁移与双轴代码审阅修复

- 直接复用 `workers/runner.py` 周期窗口去重（`periodic:retention_cleanup:<bucket>`）、`SKIP LOCKED` 领取、租约心跳与退避框架，以及 `conversations/idempotency`、`jobs`、`memories`、`operations` 既有状态契约。
- 经双轴 `code-review`（Standards & Spec）修复：
  1. 索引命名统一为 `ix_operations_expires_at_id` / `ix_operations_completed_at_id`；将 `jobs/cleanup.py` 中的 SQLAlchemy 查询与行锁抽离到 `jobs/cleanup_repository.py`。
  2. 将 `conversation_retention_days` 收敛为显式 `conversation_retention_policy: Literal["indefinite"] = "indefinite"`，避免配置有限天数被静默忽略；为同步记忆任务透传 `settings.job_attempt_retention_days`。
  3. 消除 `RetentionCleanupJobHandler.handle` 与 `run_retention_sweep` 的循环重复，复用 `db.base.normalize_utc`，由 `workers/main.py` 注入可清理的周期任务类型，并修复单次批次预算用尽时返回 `succeeded` 丢弃游标的问题（改为 `reconciling` 续扫并补充 PostgreSQL 回归测试）。

### 验证记录与剩余限制

- `.venv/bin/alembic upgrade head && .venv/bin/alembic check`：通过（升级至 `20261124_0012`，`No new upgrade operations detected.`）。
- `.venv/bin/pytest -q`：**210 passed**（新增 `tests/unit/test_retention_policy.py` 与 `tests/integration/test_retention_and_expiry.py`，扩展 `tests/unit/test_config.py`，更新 `tests/api/test_health_and_foundation.py` 迁移版本断言）。
- `.venv/bin/ruff format --check src tests migrations tools`、`.venv/bin/ruff check src tests migrations tools`、`.venv/bin/mypy src`（109 source files）、`python3 tools/check_architecture.py`、`git diff --check` 均通过。
- 剩余范围：并发锁冲突行经 `SKIP LOCKED` 跳过不阻塞在线请求，并在下一周期窗口幂等补扫；未调用真实外部 MemOS 服务。目录树、状态表、功能定位索引与职责表已同步至 `docs/backend-scaffold-design.md`。
