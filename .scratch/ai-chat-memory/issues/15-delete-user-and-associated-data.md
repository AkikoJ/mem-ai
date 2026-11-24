# 15: 删除用户及关联数据

**What to build:** 让用户删除自身及关联数据，立即停止业务访问，并通过专用回执持续查询清理进度。

**Blocked by:** [11 更新人设版本和停用 Agent](11-publish-persona-versions-and-retire-agents.md), [13 删除与清空记忆并阻止旧任务回写](13-delete-memories-with-epoch-barriers.md), [14 查询与删除自己的会话](14-query-and-delete-owned-conversations.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用 02 的用户状态入口、03/11 的生命周期实现、14 的会话清理及 13 的记忆删除策略。
- users 协调资源模块清理，operations 统一处理删除状态和回执，资源模块通过公开入口报告阶段结果。
- 远程副作用核验和通用任务恢复继续使用既有 memories 与 jobs 实现，清理顺序保护未完成操作所需数据。

## 验收条件

- [x] 用户删除原子设置 `users.status = deleting` 并保存操作（同步写入可选的独立删除审计归档接缝），期间保持 `user_identities` 绑定使普通凭证持续返回 403 且阻止静默重建新账号。
- [x] 202 返回随机 deletionReceipt、操作 ID、状态地址及有效期，服务端仅存受保护摘要，日志脱敏。
- [x] 回执通过 Authorization 的 DeletionReceipt 方案仅查询指定操作，错误操作、过期和伪造回执被拒绝。
- [x] 清理涵盖 Agent、人设版本、会话、消息、引用、生成、幂等、分页快照及用户与会话记忆，分阶段状态可查询；待本地与远程清理全部确认完成后，最终阶段再移除 `user_identities` 与用户业务记录（此后同一外部主体可重建全新 `user_id`）。
- [x] 本地用户关联数据清理后仍保留操作摘要及最小远程映射直至完成，任务不会因用户外键级联被提前删除。
- [x] 在途生成、普通或显式记忆写入、既有删除操作汇入最终屏障，本地完成与远程待确认分别显示。
- [x] 全部远程及本地确认完成才为 succeeded，未知结果保留待核验和告警，重启与重复执行可恢复。
- [x] API、任务及 PostgreSQL 测试覆盖停用即时生效、回执权限、清理顺序、外键生存、重复请求和远程部分失败。

## Answer

- `src/mem_ai/modules/users/cleanup.py` 提供 `delete_user_for_owner` 和 `UserCleanupJobHandler`，按生成租约、全提供方记忆屏障、本地业务数据（会话、Agent、分页快照 `memory_list_snapshots`）清理（设置 `local_complete=true`）、远程记忆确认（汇总 `awaiting_retry` / `provider_unavailable`、告警与自适应退避轮询）、既有会话删除操作确认、终态任务与记忆映射清理、子操作与最终身份移除的阶段顺序推进；提供方子操作映射持久化在父任务 `jobs.metadata_json`（不向回执暴露内部子操作 ID），`finalizing_identity` 完成时在单事务内原子移除身份并将操作与父任务同步置为 `succeeded` 并导出终态审计事件。
- `src/mem_ai/modules/auth/dependencies.py`、`src/mem_ai/modules/operations/service.py`、`repository.py`、`router.py` 和迁移 `migrations/versions/20261123_0011_add_user_deletion_receipts.py` 提供操作受理、HMAC-SHA256 回执摘要、过期/撤销/固定时间校验及基于 `OperationAuthContextDep`（`DeletionReceiptAccess | UserOperationAccess`）的统一回执/用户认证（支持 `dependency_overrides`）。
- `src/mem_ai/modules/memories/user_cleanup.py`、`repository.py`、`clear_state.py`、`clear_jobs.py`、`service.py` 和 `jobs.py` 复用共享屏障，枚举历史提供方、在本地阶段有界清理 `memory_list_snapshots`、严格等待有效写入租约、分批核验未知写入（超上限窗口输出告警并持久化 `unsettled_audit_job_ids`）、为删除中已同步确认的显式写入标记 `confirmed_during_clear`；失败子清空或缺失历史提供方保留 `reconciling` / `awaiting_retry` / `provider_unavailable` 并可通过既有受控重排恢复。
- `src/mem_ai/modules/conversations/cleanup.py`、`agents/service.py`、`jobs/cleanup.py` 和 `operations/repository.py` 分别提供会话图、Agent/人设、终态任务和子操作的有界清理入口，共享 `JobRecord.has_active_lease` 与 `_settle_running_generation_in_session` 过期生成收敛辅助逻辑；被用户删除接管的 `conversation_delete` 操作在远程确认前保持 `reconciling`，远程完成后由 `settle_user_conversation_operations_in_session` 收敛为 `succeeded`。
- 测试入口为 `tests/api/test_user_deletion.py`、`tests/integration/test_user_deletion.py` 和 `tests/integration/test_user_deletion_provider_mapping.py`（覆盖 `conversation_delete` + `user_delete` 竞态与分页快照在 `local_complete=true` 前清理）；旧记忆屏障回归固定 clear-only Worker 以保持核验时序契约。为避免两个同名测试模块冲突，`pyproject.toml` 固定 `--import-mode=importlib`。
- 验证结果：`.venv/bin/pytest -q` **199 passed**；`.venv/bin/ruff check src tests migrations tools`、`.venv/bin/ruff format --check src tests migrations tools`、`.venv/bin/mypy src`、`python3 tools/check_architecture.py` 和 `git diff --check` 均通过。未执行构建命令，未调用真实 MemOS 服务。历史提供方不可用时，身份、scope 和子操作映射按设计保留，物理过期清理由 ticket 16 承接。
