# 02: 接入认证上下文和用户主体

**What to build:** 让请求获得可信身份并映射稳定本地用户，后续模块据此隔离资源，生产接入实际身份验证。

**Blocked by:** [01 建立 FastAPI 与 PostgreSQL 基础](01-establish-fastapi-postgres-foundation.md)

**Status:** ready-for-agent

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用 01 的依赖注入、错误响应和测试应用。
- 由 auth 验证凭证，users 唯一负责本地主体映射与用户状态查询，业务模块复用可信上下文和用户策略。
- 后续删除与提交前检查继续调用 users 的状态规则，业务 Schema 中的用户字段服从认证身份。

## 验收条件

- [ ] 凭证验证与本地用户解析分层，统一 AuthenticationContext 含 actor_id、user_id、auth_method 和 permissions。
- [ ] 已验证 issuer 与 subject 唯一映射本地用户，并发首次请求复用同一用户，映射在重启后保留。
- [ ] `GET /v1/users/me` 返回当前用户，身份缺失或失效返回 401，已停用或删除中用户返回 403。
- [ ] 请求体、查询参数和未受信任身份 Header 无法覆盖当前身份，跨主体注入有 HTTP 验证。
- [ ] 开发与测试适配器可替换，生产配置拒绝开发适配器，真实凭证适配点可接入部署验证协议。
- [ ] 可信网关模式显式配置来源和代理边界，伪造身份 Header 被拒绝。
- [ ] 用户主体持久化、禁用状态与测试身份注入通过 PostgreSQL 和路由测试验证。
