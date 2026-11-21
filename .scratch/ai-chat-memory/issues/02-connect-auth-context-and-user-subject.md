# 02: 接入认证上下文和用户主体

**What to build:** 让请求获得可信身份并映射稳定本地用户，后续模块据此隔离资源，生产接入实际身份验证。

**Blocked by:** [01 建立 FastAPI 与 PostgreSQL 基础](01-establish-fastapi-postgres-foundation.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用 01 的依赖注入、错误响应和测试应用。
- 由 auth 验证凭证，users 唯一负责本地主体映射与用户状态查询，业务模块复用可信上下文和用户策略。
- 后续删除与提交前检查继续调用 users 的状态规则，业务 Schema 中的用户字段服从认证身份。

## 验收条件

- [x] 凭证验证与本地用户解析分层，统一 AuthenticationContext 含 actor_id、user_id、auth_method 和 permissions。
- [x] 已验证 issuer 与 subject 唯一映射本地用户，并发首次请求复用同一用户，映射在重启后保留。
- [x] `GET /v1/users/me` 返回当前用户，身份缺失或失效返回 401，已停用或删除中用户返回 403。
- [x] 请求体、查询参数和未受信任身份 Header 无法覆盖当前身份，跨主体注入有 HTTP 验证。
- [x] 开发与测试适配器可替换，生产配置拒绝开发适配器，真实凭证适配点可接入部署验证协议。
- [x] 可信网关模式显式配置来源和代理边界，伪造身份 Header 被拒绝。
- [x] 用户主体持久化、禁用状态与测试身份注入通过 PostgreSQL 和路由测试验证。

## Answer

### 实际公共符号与后续调用入口

- **认证上下文与层一凭证验证（`src/mem_ai/modules/auth/`）**：
  - `src/mem_ai/modules/auth/context.py`：`VerifiedPrincipal(issuer, subject, actor_id, auth_method, permissions)` 与 `AuthenticationContext(actor_id, user_id, auth_method, permissions)`（含 `has_permission()`）。
  - `src/mem_ai/modules/auth/adapters.py`：`AuthRequest`、`CredentialVerifier` 协议、`DevelopmentAuthAdapter`、`StaticTokenAuthAdapter`、`JwtHs256AuthAdapter`（校验签名、算法、`exp`、`nbf`、`iss`、`aud`、`sub`）、`TrustedGatewayAuthAdapter`（校验直连代理 CIDR、共享网关密钥与可信签发方）及 `build_credential_verifier(settings)`。
  - `src/mem_ai/modules/auth/dependencies.py`：`get_credential_verifier`、`CredentialVerifierDep`、`get_auth_context`、`AuthContextDep`（后续业务路由直接声明 `auth: AuthContextDep` 获取可信 `user_id`，并自动绑定结构化日志 `user_id`；测试可通过 `create_app(..., credential_verifier=...)` 或 `app.dependency_overrides[get_credential_verifier]` 替换适配器）。
- **用户主体映射、共享状态策略与路由（`src/mem_ai/modules/users/`）**：
  - `src/mem_ai/modules/users/models.py`：`User`（`users`，含 `ck_users_status_valid` 约束与 `ix_users_status` 索引）与 `UserIdentity`（`user_identities`，含 `uq_user_identities_issuer_subject` 唯一约束、`ix_user_identities_user_id` 与 `ondelete="RESTRICT"` 外键）。
  - `src/mem_ai/modules/users/schemas.py`：`UserStatus`、`UserRecord`（应用层只读快照，不向外泄露 ORM 对象）、`UserMeResponse`（显式 `camelCase` 别名）。
  - `src/mem_ai/modules/users/repository.py`：`get_user_by_id`、`get_user_by_identity`、`create_user_with_identity`、`update_user_status`。
  - `src/mem_ai/modules/users/service.py`：
    - `assert_user_active(user)`：统一拒绝 `disabled`（`403 user_disabled`）与 `deleting`（`403 user_deleting`）。
    - `ensure_user_active_in_session(session, user_id=...)`：事务参与式状态复查入口，供后续对话提交、后台任务及删除协调复用。
    - `resolve_or_create_user(session_factory, issuer=..., subject=...)`：使用短事务 + savepoint 冲突回滚重查，保证并发首次请求复用同一本地用户且无孤立 `users` 行。
    - `resolve_active_user`、`get_active_user`、`set_user_status`。
  - `src/mem_ai/modules/users/router.py`：`GET /v1/users/me`（挂载于 `src/mem_ai/api/router.py` 的 `/v1` 前缀下，返回统一 `ApiResponse[UserMeResponse]`）。
- **配置、中间件、装配与迁移**：
  - `src/mem_ai/core/config.py`：新增 `auth_mode`（`development` / `jwt_hs256` / `trusted_gateway`）、`GatewayHeaders`、`is_ip_in_networks` 及启动边界校验，生产环境拒绝 `auth_mode="development"`、弱密钥及通配符代理 CIDR（`0.0.0.0/0`、`::/0`）。
  - `src/mem_ai/api/middleware.py`：新增 `IdentityHeaderSanitizerMiddleware` 清理外来/非可信网关来源的身份 Header，并扩展 `RequestContextMiddleware` 重置 `user_id` 日志上下文。
  - `src/mem_ai/application.py`：`create_app(..., credential_verifier=...)` 装配凭证验证器并在生产环境拒绝 `DevelopmentAuthAdapter` 与 `StaticTokenAuthAdapter`。
  - `src/mem_ai/db/model_registry.py` 与 `migrations/versions/20261121_0002_add_users_and_user_identities.py`：注册 ORM 模型并新增 `users`/`user_identities` 迁移（`20261121_0002`）。
  - `tests/conftest.py`：新增 `sign_hs256_jwt()`、`make_test_principal()` 与 `test_auth_adapter` 供当前及后续切片复用。

### 复用、扩展与职责分工

- 复用 01 的 `ApiResponse`/`ok_response`、`AppError` 异常信封、`Base`/`UUIDPrimaryKeyMixin`/`TimestampMixin`、`SessionFactoryDep`、`create_app` 生命周期和隔离 PostgreSQL 测试 fixture，未引入重复实现。
- `auth` 与 `users` 保持单向依赖：`users/{models,schemas,repository,service}` 不依赖 `auth`，`auth/dependencies` 委托 `users/service` 完成主体映射与状态校验，`users/router` 仅通过 `AuthContextDep` 与 `users/service` 交互。

### 验证记录与剩余限制

- `.venv/bin/alembic upgrade head && .venv/bin/alembic check`：通过（升级至 `20261121_0002`，`No new upgrade operations detected.`）。
- `.venv/bin/pytest`：42 passed（含配置边界、认证适配器单元、PostgreSQL 并发主体映射/重启持久化/状态策略集成、`GET /v1/users/me`/跨主体注入/可信网关代理边界 API 与架构规则测试）。
- `.venv/bin/ruff check .` 与 `.venv/bin/ruff format --check .`：All checks passed。
- `.venv/bin/mypy src`：Success: no issues found in 38 source files（strict 模式）。
- `python3 tools/check_architecture.py`：工程目录、功能索引、文档链接、分层导入、依赖图和已落地源码检查通过。
- 剩余范围：用户删除协调（`DELETE /v1/users/me` 与 `users/cleanup.py`）和删除回执权限属于 15 切片；非对称 OIDC/JWKS 协议可在部署时通过实现 `CredentialVerifier` 协议无侵入接入。

### 目录与文档同步

- 已按 `backend-design` 技能同步 `docs/backend-scaffold-design.md` 第 3 节当前目录状态、目录说明与模块职责表，并同步 `.env.example` 的 `AUTH_*` 环境变量模板。
