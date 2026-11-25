# 04: 扩展本地用户凭据与登录会话并交付内置认证接口（Expand）

**What to build:** 为本地用户扩展唯一邮箱与强哈希密码存储，新增可撤销的持久化登录会话表并上线完整的内置认证接口（注册、登录、刷新轮换、注销与改密），同时扩展到期保留清理自动回收过期登录会话，且在扩展阶段保持既有外部认证适配与全量现有测试继续通过。

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

**Parent:** [一体化单进程 Python 后端体系收敛规格](../spec.md)

## 实现边界

- 本切片为宽重构的 **Expand** 阶段：在 `users` 新增规范化小写唯一 `email` 与 `password_hash` 列（兼容已有历史行升级），在 `auth` 新增 `auth_sessions` 持久化登录会话表与索引，不破坏现有 `user_identities` 与既有认证测试。
- 在 `auth` 提供内置密码哈希与校验、Access Token（JWT）签发/校验、高熵 Refresh Token 摘要持久化与轮换（检测重放立即吊销整个会话家族），以及 `/v1/auth/register`、`/v1/auth/login`、`/v1/auth/refresh`、`/v1/auth/logout`、`/v1/auth/password` 路由；自助注册受服务端配置开关控制。
- 让内置签发的 Access Token 在普通业务认证依赖中直接解析到本地活跃用户，同时保留对停用/删除中用户拒绝（`403`）与删除回执隔离；扩展 `retention` 周期清理按有限批次游标清理已过期或长期已吊销的 `auth_sessions`。

## 验收条件

- [ ] Alembic 迁移同时通过空库升级、V1 历史库升级与元数据漂移检查，`users` 支持大小写不敏感唯一邮箱与密码哈希，`auth_sessions` 仅存刷新令牌摘要并具备查询与保留索引。
- [ ] `POST /v1/auth/register` 在开启自助注册时返回 `201` 与 Access/Refresh Token；关闭注册时返回稳定 `403`；重复邮箱（含大小写变体）返回稳定 `409`；弱密码或非法邮箱返回 `422`；日志与响应绝不泄露明文密码或刷新令牌摘要以外的敏感信息。
- [ ] `POST /v1/auth/login` 对未知邮箱与错误密码返回一致的 `401`（防止用户枚举），对停用或删除中用户返回 `403`，成功后创建持久化登录会话并返回令牌对。
- [ ] `POST /v1/auth/refresh` 原子轮换新旧登录会话并返回新令牌对；重放已轮换或已吊销的旧 Refresh Token 会返回 `401` 并吊销对应会话家族。
- [ ] `POST /v1/auth/logout` 支持吊销当前登录会话或按用户吊销全部活跃登录会话；`POST /v1/auth/password` 校验旧密码并更新哈希，同时立即吊销其余活跃登录会话。
- [ ] 内置登录获得的 Access Token 可直接调用既有受保护业务接口（如用户、Agent、会话、对话与记忆）；过期/吊销会话被周期保留清理分批回收且不影响活跃会话。
- [ ] 既有全部 V1 单元、API、集成与架构测试保持全绿，Ruff、mypy 与 `python3 tools/check_architecture.py` 全部通过。
