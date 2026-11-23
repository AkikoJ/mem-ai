# 14: 查询与删除自己的会话

**What to build:** 让用户查看自己的会话及真实消息历史，并删除会话与相关会话记忆。

**Blocked by:** [07 控制同一会话的并发生成](07-reject-overlapping-conversation-generation.md), [09 持久化任务并重试记忆写入](09-persist-and-retry-memory-writes.md), [10 支持聊天幂等与生成结果查询](10-recover-idempotent-chat-generations.md), [13 删除与清空记忆并阻止旧任务回写](13-delete-memories-with-epoch-barriers.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用 conversation 的归属规则、07 的生成条件提交、09 的任务框架和 13 的删除操作与记忆策略。
- conversations/cleanup.py 负责会话数据分批清理，远程会话记忆通过 memories 的范围清理入口处理。
- 会话 deleting 状态与范围删除标记参与写入校验，用户级代次与用户级记忆遵循既有清空契约。

## 验收条件

- [x] 会话列表、详情和消息历史采用统一游标，包含生成状态，可区分失败或取消回合与完整回复。
- [x] 删除立即将会话置为 deleting，列表隐藏、详情/消息/生成查询返回 404、续聊返回 409 conversation_deleting，返回可查询操作，其他用户操作统一 404。
- [x] 后台分批清理消息、召回引用、生成、相关幂等及该会话范围记忆（提供方不支持按会话批量删除时按筛选出的目标 ID 分批删除并叠加本地删除标记兜底隔离），用户记忆按用户级清空规则保留。
- [x] 在途生成续期与最终提交复查会话 deleting 状态并失败收敛，失联租约待其过期收敛后再清理数据，既有任务在执行前复查，已发外部写入核验并在最终会话清空前收敛。
- [x] 会话清理使用统一任务、去重和租约，重复删除关联同一有效操作，重启后继续，全部确认才完成。
- [x] HTTP 和 PostgreSQL 测试覆盖分页、用户隔离、生成竞争、任务竞争、远程未知结果、重复请求和分批恢复。


## Answer

- `conversations/queries.py`、`history_repository.py`、`schemas.py` 和 `router.py` 提供自己的会话列表、详情与消息历史游标查询，生成状态与失败信息来自既有生成记录。
- `conversations/cleanup.py` 和 `cleanup_repository.py` 提供删除入口、任务处理及本地数据有界清理；`memories/conversation_cleanup.py` 提供范围 tombstone、在途任务收敛和远程记忆逐条清理。`workers/main.py` 注册处理器，调用方通过会话删除 API 与操作查询 API 跟踪进度。
- 复用会话访问、生成租约、统一任务与操作状态机、既有单条记忆删除/核验和用户级清空规则；显式会话记忆写入在登记及确认事务复查会话，清理时跳过锁定的写入任务并继续等待其收敛。工程目录、导航索引及职责表已同步。
- 验证：`python3 tools/check_architecture.py`、`.venv/bin/ruff check src tests`、`.venv/bin/mypy src/mem_ai` 均通过；`.venv/bin/pytest -q` 全量 171 项通过，含 PostgreSQL HTTP/Worker 测试，覆盖用户隔离、分页、重复删除、分批重启、在途生成及写入竞争、远程结果未知与核验。
- 独立子代理审查因任务内容未送达未能完成；已对规格、并发锁顺序及项目约束执行人工检查。未自动构建或提交。
