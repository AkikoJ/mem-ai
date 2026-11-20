# 03: 创建和使用私有 Agent

**What to build:** 让认证用户创建私有 Agent、发布首个人设版本，并从自己的列表及详情中取得可用于聊天的 Agent。

**Blocked by:** [02 接入认证上下文和用户主体](02-connect-auth-context-and-user-subject.md)

**Status:** ready-for-agent

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用 02 的可信身份和 01 的分页、错误处理与提供方装配。
- agents 统一拥有 Agent、人设版本和发布事务，personas 的本地适配委托同一实现满足 PersonaProvider。
- 本切片建立版本分配和首次发布入口，11 在该入口扩展草稿更新、并发发布及停用。

## 验收条件

- [ ] 创建、首版发布、列表和详情通过公开 HTTP 路由验证，所有权来自认证用户。
- [ ] Agent 与人设版本在 PostgreSQL 持久化，已发布版本不可变，重启后仍可读取。
- [ ] PersonaProvider 使用项目自己的结构，替换适配器继续满足相同拥有者和版本契约。
- [ ] 同一 Agent 版本号具有唯一约束，发布使用原子事务和条件更新。
- [ ] 列表使用 limit、cursor、items、next_cursor、has_more 及稳定排序，覆盖同时间戳分页。
- [ ] 其他用户读取、编辑或发布该 Agent 返回统一 404，客户端用户 ID 无法改变归属。
