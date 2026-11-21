# 15: 删除用户及关联数据

**What to build:** 让用户删除自身及关联数据，立即停止业务访问，并通过专用回执持续查询清理进度。

**Blocked by:** [11 更新人设版本和停用 Agent](11-publish-persona-versions-and-retire-agents.md), [13 删除与清空记忆并阻止旧任务回写](13-delete-memories-with-epoch-barriers.md), [14 查询与删除自己的会话](14-query-and-delete-owned-conversations.md)

**Status:** ready-for-agent

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用 02 的用户状态入口、03/11 的生命周期实现、14 的会话清理及 13 的记忆删除策略。
- users 协调资源模块清理，operations 统一处理删除状态和回执，资源模块通过公开入口报告阶段结果。
- 远程副作用核验和通用任务恢复继续使用既有 memories 与 jobs 实现，清理顺序保护未完成操作所需数据。

## 验收条件

- [ ] 用户删除原子设置 `users.status = deleting` 并保存操作（同步写入可选的独立删除审计归档接缝），期间保持 `user_identities` 绑定使普通凭证持续返回 403 且阻止静默重建新账号。
- [ ] 202 返回随机 deletionReceipt、操作 ID、状态地址及有效期，服务端仅存受保护摘要，日志脱敏。
- [ ] 回执通过 Authorization 的 DeletionReceipt 方案仅查询指定操作，错误操作、过期和伪造回执被拒绝。
- [ ] 清理涵盖 Agent、人设版本、会话、消息、引用、生成、幂等、分页快照及用户与会话记忆，分阶段状态可查询；待本地与远程清理全部确认完成后，最终阶段再移除 `user_identities` 与用户业务记录（此后同一外部主体可重建全新 `user_id`）。
- [ ] 本地用户关联数据清理后仍保留操作摘要及最小远程映射直至完成，任务不会因用户外键级联被提前删除。
- [ ] 在途生成、普通或显式记忆写入、既有删除操作汇入最终屏障，本地完成与远程待确认分别显示。
- [ ] 全部远程及本地确认完成才为 succeeded，未知结果保留待核验和告警，重启与重复执行可恢复。
- [ ] API、任务及 PostgreSQL 测试覆盖停用即时生效、回执权限、清理顺序、外键生存、重复请求和远程部分失败。
