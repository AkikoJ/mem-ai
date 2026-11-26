# 07: 移除外部认证适配器、权限字段与遗留表结构并完成全量验收（Contract）

**What to build:** 彻底移除外部认证适配器、可信网关认证配置、身份上下文中的权限集合以及遗留的 `user_identities` 表与 `model_configs.required_permission` 列，完成空库及 V1 历史库迁移升级验证，并同步架构检查与工程文档完成一体化单进程后端的最终验收。

**Blocked by:** [01 将人设只读快照收口至 agents 模块并移除过渡适配层](01-consolidate-persona-snapshots-into-agents.md)、[03 移除独立 Worker 进程入口并统一单进程运维与容器配置](03-retire-standalone-worker-and-unify-ops.md)、[06 将全量测试基线迁移至纯内置认证接缝（Migrate 测试套件）](06-migrate-test-suite-to-builtin-auth-seam.md)

**Status:** resolved

**Parent:** [一体化单进程 Python 后端体系收敛规格](../spec.md)

## 实现边界

- 本切片为宽重构的 **Contract** 与总收敛阶段：移除 `DevelopmentAuthAdapter`、`StaticTokenAuthAdapter`、`JwtHs256AuthAdapter` 外部签发方模式、`TrustedGatewayAuthAdapter` 及相关配置项和网关认证头分支（保留 Host/CORS/可信代理 IP 解析、`X-Request-Id` 白名单与删除回执隔离）。
- 精简 `AuthenticationContext`，移除 `VerifiedPrincipal`、`actor_id` 与 `permissions`；同步清理模型配置与同步种子中的 `required_permission`。
- 新增收缩阶段 Alembic 迁移删除 `user_identities` 表与 `model_configs.required_permission` 列，补全 `users.email` / `password_hash` 非空约束收口，并验证空库升级、从 V1 基线版本一路升级到最新头版本以及元数据漂移检查。
- 同步更新工程设计文档的目录树、当前目录状态表（清除全部过渡标注）、功能定位索引、配置模板与架构检查脚本，完成单进程一体化后端的端到端验收。

## 验收条件

- [x] 所有外部认证适配器、外部 `issuer`/`audience` 校验、可信网关认证头模式、`VerifiedPrincipal`、`AuthenticationContext.permissions` 与 `required_permission` 代码和配置已彻底清除。
- [x] Alembic 收缩迁移在空 PostgreSQL 库和已填充数据的 V1 历史库上均能顺利升级到最新 head，且 `alembic check` 无元数据漂移；升级后既有 Agent、人设版本、会话、消息、记忆引用、任务与操作记录保持完整可用。
- [x] 单进程端到端验收通过：仅启动一个服务实例加一个 PostgreSQL，完成注册登录 → 创建并发布私有 Agent → 同步与流式聊天 → 异步记忆写入与召回 → 刷新轮换与改密 → 清空屏障与删除用户（含防同邮箱重注册及回执查询）→ 优雅停机与恢复重放全闭环。
- [x] 工程设计文档、环境变量模板、架构检查脚本与架构测试完全同步，无残留过渡标注，Ruff、mypy、Alembic 校验、全量 pytest 与 `python3 tools/check_architecture.py` 全部通过。


## Answer

已于 2026-11-26 在基线 `42839a7` 上完成实现。全部直接阻塞项（01、03、06）均已落地；本切片完成 ADR-0007 宽重构的 Contract 收缩与一体化单进程 V2 全量验收。

### 实际入口与收缩变更

- **移除外部认证适配与遗留配置**：
  - 删除 `src/mem_ai/modules/auth/adapters.py`（`DevelopmentAuthAdapter`、`StaticTokenAuthAdapter`、`JwtHs256AuthAdapter`、`TrustedGatewayAuthAdapter`、`AuthRequest`、`CredentialVerifier`、`build_credential_verifier`）。
  - `src/mem_ai/core/config.py`：移除 `AuthMode`、`GatewayHeaders`、全部 `auth_mode`/`auth_dev_*`/`auth_trusted_issuers`/`auth_trusted_audience`/`auth_jwt_hs256_secret`/`auth_gateway_*` 字段与校验，以及模型注册表允许键中的 `required_permission`；保留 `auth_trusted_proxies`、`auth_jwt_leeway_seconds`、`allowed_hosts`、`cors_allowed_origins` 与可信请求 ID 规则；`deletion_receipt_signing_key()` 优先使用 `deletion_receipt_secret`，未配置时回退至 `auth_jwt_secret`。
  - `src/mem_ai/api/middleware.py`：`IdentityHeaderSanitizerMiddleware` 移除 `trusted_gateway` 分支，始终剥离伪造身份及历史网关头（`X-User-Id`、`X-Actor-Id`、`X-Forwarded-User`、`X-Remote-User`、`X-Gateway-Auth`、`X-Auth-*`），非可信代理来源继续剥离代理转发头。
  - `src/mem_ai/application.py` 与 `src/mem_ai/ops.py`：移除 `credential_verifier` 参数与外部分派分支；`verify_providers` 仅经 `auth.service.extract_bearer_token` 与 `authenticate_builtin_access_token` 核验内置令牌，移除 `ProviderVerificationReport.auth_mode`，`--check-memory` 使用数据库会话时要求提供 `--probe-user-id` 或 `--check-auth`。
- **精简身份上下文、用户与模型契约**：
  - `src/mem_ai/modules/auth/context.py`：移除 `VerifiedPrincipal`、`actor_id`、`permissions`、`has_permission`，`AuthenticationContext` 收口为 `(user_id, email, auth_method, session_id)`。
  - `src/mem_ai/modules/auth/tokens.py`、`service.py`、`dependencies.py`：移除 `try_peek_jwt_issuer`、`is_builtin_access_token` 与外部回退路径，抽取 `_resolve_auth_context` 供可选与操作认证依赖共享，直接验签内置 Bearer JWT。
  - `src/mem_ai/modules/users/{models,repository,service,schemas,router,cleanup}.py`：移除 `UserIdentity` 模型与 `resolve_or_create_user`/`resolve_active_user`，将 `delete_user_and_identities` 收口为 `delete_deleting_user`；`User.email` 与 `User.password_hash` 收紧为 `NOT NULL`；`UserMeResponse` 移除 `actorId` 与 `permissions`。
  - `src/mem_ai/modules/models/{models,schemas,repository}.py`：移除 `ModelConfig.required_permission` 列及 `ModelConfigSeed`/`ModelConfigRecord`/upsert 中的 `required_permission`。
- **Alembic Contract 迁移**：
  - 新增 `migrations/versions/20261126_0014_contract_external_auth_and_permissions.py`（`down_revision = "20261126_0013"`）：对历史空值或空串 `users.email`/`password_hash` 执行确定性占位回填（`legacy-<uuid>@migrated.invalid` 与 `!unusable-legacy-migrated`），收紧非空约束与全量唯一索引 `ix_users_email_unique`、`ck_users_email_lowercase`，删除 `user_identities` 表与 `model_configs.required_permission` 列，并提供可逆 `downgrade()`。
- **架构守卫与端到端验收**：
  - `tools/check_architecture.py` 与 `tests/architecture/test_architecture_rules.py`：新增 `RETIRED_PATHS` 与 `check_retired_paths` 防止已退役路径（`modules/personas`、`contracts/persona_provider.py`、`workers/main.py`、`modules/auth/adapters.py`）回流，同时纳入 `.scratch/unified-single-process-backend/spec.md` 链接与 DAG 依赖校验。
  - `tests/integration/test_database_and_migrations.py`：抽取共享 V1 历史数据种子，验证空库升级/降级/漂移检查、历史 `20261122_0005` 及 V1 终版 `20261124_0012` 升级至 `20261126_0014` 保持全部 Agent/人设版本/会话/消息/召回引用/模型/任务/操作/范围完整可用，并验证 `user_identities` 与 `required_permission` 已删除。
  - `tests/integration/test_production_deployment_and_recovery.py`：新增 `test_v2_unified_single_process_end_to_end_acceptance`，以单个应用实例 + 单个 PostgreSQL 完成注册登录 → 创建发布私有 Agent → 同步与跨会话 SSE 聊天（含内嵌后台任务异步记忆写入与跨会话召回及引用落库）→ 刷新轮换/重放吊销/改密 → 清空屏障收敛 → 删除用户（含删除中 `403`/`401`/`409` 防同邮箱重注册、回执隔离与进度查询、物理清理后同邮箱重新注册）→ 优雅停机与审计归档重放全闭环。

### 验证证据

- 红→绿：新增收缩契约与架构单测在收缩前按预期失败（4 failed），收缩后全部转绿。
- 静态、依赖与架构检查：`uv lock --check`、`.venv/bin/ruff check .`、`.venv/bin/ruff format --check .`（206 files）、`.venv/bin/mypy src`（118 source files）、`python3 tools/check_architecture.py` 与 `git diff --check` 全部通过。
- 全量测试：`.venv/bin/pytest -q --tb=short --show-capture=no` 全部通过（291 passed，覆盖单元、API、PostgreSQL 迁移/历史升级、内嵌任务、删除屏障、保留清理、安全边界、单进程 V2 全闭环与架构测试）。
- 代码评审：通过 `code-review` 双轴并行评审（Standards 与 Spec），已全部修复评审指出的项（跨会话召回非空断言与 `message_recalled_memories` 校验、`0014` 空串回填守卫、显式 `DELETION_RECEIPT_SECRET` 原始字节保留、清理冗余 `parsed_proxies` 与 `_resolve_auth_context` 去重、补齐可信/不可信代理网关头剥离测试及历史测试种子去重）。
- 剩余限制：全部测试在本地隔离 PostgreSQL 与受控模型/记忆提供方上运行；真实第三方供应商（OpenAI 兼容接口与 MemOS Cloud）连通性及平台级归档卷/PITR 需按 `docs/operations/deployment-and-recovery.md` 在拥有真实凭据的 Staging 环境完成核验。

### 文档与职责同步

已同步 `.env.example`、`docs/backend-scaffold-design.md`（清除全部 V1 过渡标注并更新目录树、职责表、主数据表、认证/删除/测试/收敛章节）、`DESIGN.md`、`docs/adr/0007-unified-single-process-backend.md` 与 `docs/operations/deployment-and-recovery.md`。模块所有权完全收敛至 ADR-0007 目标形态。
