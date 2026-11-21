# 12: 流式返回与确认生成终态

**What to build:** 让聊天通过 SSE 返回增量和确定终态，并可在断连后查询原生成。

**Blocked by:** [06 查询和选择可用模型](06-discover-and-select-models.md), [08 召回相关范围的记忆](08-recall-relevant-scoped-memories.md), [09 持久化任务并重试记忆写入](09-persist-and-retry-memory-writes.md), [10 支持聊天幂等与生成结果查询](10-recover-idempotent-chat-generations.md), [11 更新人设版本和停用 Agent](11-publish-persona-versions-and-retire-agents.md)

**Status:** ready-for-agent

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用工程设计第 5.4 节的共同准备、上下文和终态提交，以及已落地的模型、幂等、租约和任务入口。
- 本切片增加 SSE 编码、心跳、模型增量连接与断连通知，取消和提交竞争通过 generation 的共同入口收敛。
- 测试同时经过同步和流式公开入口验证共享规则，并核对同键重试复用原生成。

## 验收条件

- [ ] 新流使用 text/event-stream，事件严格遵循设计中的 conversation.started、message.delta、conversation.completed、conversation.failed 和 heartbeat。
- [ ] 开始事件在用户消息和 IDs 提交后发出，业务事件 ID 在生成内递增，delta 序号有序，heartbeat 保活且无状态变更。
- [ ] 完整回复、生成成功和任务原子提交后才发送 completed，失败时连接仍可写则发送带稳定错误码及 partial_output_persisted=false 的 failed。
- [ ] 流开始前错误使用普通 error 信封，流开始后以失败事件收敛，同时复用同步入口的权限、模型、记忆、人设和租约规则。
- [ ] 流式断连尝试上游取消，已提交成功保持成功，否则 cancelled，部分 Assistant 文本被丢弃且无完整回合记忆任务（与同步请求断连继续完成提交的契约区分明确）。
- [ ] 旧租约或进程崩溃通过恢复器得到可查询 failed，提交与断连竞争只产生一个终态。
- [ ] 原键重试的运行请求返回 202，成功返回原 JSON，客户端用查询恢复生成，重复请求复用原模型调用。
- [ ] HTTP 和 PostgreSQL 测试覆盖事件 Schema、顺序、心跳、部分失败、提交失败、断连、期限及结果恢复，并同时验证同步与流式调用共同状态转换和提交入口。
