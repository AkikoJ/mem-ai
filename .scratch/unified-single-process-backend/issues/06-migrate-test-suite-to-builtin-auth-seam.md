# 06: 将全量测试基线迁移至纯内置认证接缝（Migrate 测试套件）

**What to build:** 将共享测试夹具与全量 API、集成和单元测试从静态令牌与外部主体替身迁移至内置注册/登录会话接缝，使整个测试套件直接通过最高层内置认证路径验证用户隔离、会话生命周期、对话、记忆与删除契约。

**Blocked by:** [05 切换用户删除防重注册与模型无权限路由至新契约（Migrate 核心业务）](05-migrate-user-deletion-and-permissionless-models.md)

**Status:** resolved

**Parent:** [一体化单进程 Python 后端体系收敛规格](../spec.md)

## 实现边界

- 本切片为宽重构的测试迁移阶段：在共享测试夹具中提供基于内置注册/登录（或内置令牌签发辅助）的双用户认证夹具，替代 `StaticTokenAuthAdapter` 与外部主体构造器。
- 逐批迁移所有业务 API 测试、PostgreSQL 集成测试与模型策略单元测试，使其不再引用外部认证适配器、可信网关头、`user_identities` 外部映射或 `permissions` / `required_permission` 测试夹具。
- 验证所有跨用户 `404` 隔离、停用/删除中 `403` 拦截、删除回执越权拦截及多用户并发行为在内置认证接缝下全部通过。

## 验收条件

- [x] 共享测试夹具默认使用内置认证模式，提供可复用的多用户注册登录辅助并移除业务测试对静态令牌与外部主体替身的依赖。
- [x] 全量业务 API 测试（用户、Agent、人设版本、模型、聊天同步/流式/并发/幂等/记忆召回与写入、会话、记忆与删除、限流与观测）全部通过真实内置认证接缝运行并绿灯。
- [x] 全量集成测试与单元测试不再依赖 `user_identities` 映射构造用户或 `required_permission` 权限断言（仅保留将在 07 随历史库升级测试处理的迁移兼容用例）。
- [x] 全量 pytest、Ruff、mypy 与 `python3 tools/check_architecture.py` 全部通过。


## Answer

已于 2026-11-26 在基线 `8c4a4f3` 上完成实现。直接依赖 05 已落地；本切片迁移测试接缝，生产外部认证与遗留表结构仍由 07 收缩。

### 实际入口与迁移

- `tests/conftest.py`：`BuiltinUser` / `BuiltinUsers` 保存真实用户和令牌，`register_builtin_user` / `login_builtin_user` 调用公开 HTTP 接口；`builtin_users` 为每个隔离数据库注册 Alice/Bob，登录会话可跨应用重启复用。`create_builtin_user` 通过公开 `auth.service.register_user` 为服务层集成测试创建本地凭据及登录会话，返回 `UserRecord`。删除 `make_test_principal` 与 `test_auth_adapter`，业务请求显式使用内置令牌，不覆盖认证依赖。
- `tests/api/`：用户、Agent、人设、模型、同步/SSE/并发/幂等聊天、召回/写入、会话、记忆、删除、资源预算与观测测试全部迁至真实内置认证。保留跨用户 404、停用/删除中 403、回执越权拒绝、重启与并发断言；用户删除后显式重新注册邮箱获得新用户，旧令牌不再隐式重建身份。
- `tests/integration/`：种子用户经公开内置注册创建；用户持久化改为规范化邮箱并发注册、登录及重启验证；删除资源图以 `auth_sessions`、邮箱占位和最终级联删除为断言，恢复重放与生产生命周期也使用内置用户。仅 `test_database_and_migrations.py` 保留历史 `user_identities` 插入作为 07 历史库升级兼容基线。
- `tests/unit/`：移除外部适配器专用的 `test_auth_adapters.py` 和旧认证配置断言，将适用的非有限过期时间、非法用户/会话/邮箱声明校验迁入 `test_builtin_auth.py`；移除模型权限夹具与断言，保留模型注册、启用、强制选择、覆盖与能力约束。
- `src/mem_ai/modules/auth/service.py` 与 `src/mem_ai/modules/auth/dependencies.py`：提取公开入口 `extract_bearer_token`、`is_builtin_access_token` 与 `authenticate_builtin_access_token`，统一 Bearer 解析、内置 issuer 判断、JWT 验签与当前 `active` 用户状态校验。
- `src/mem_ai/ops.py::verify_providers`（为解阻 `test_production_deployment_and_recovery.py` 迁移内置令牌而补齐的最小前置修正）：复用 `auth.service` 公开入口核验内置令牌并记录 `verified_user_id`；当同时执行 `check_auth` 与 `check_memory` 且未显式传入 `probe_user_id` 时自动复用已验签的内置用户 ID，不再回退写入 `user_identities`（集成测试已断言 `user_identities` 为 0 行且快照归属该内置用户）。保留旧外部分派分支至 07，未新增外部适配器。

### 验证证据

- 运维探针红→绿：原实现拒绝注册接口签发的内置令牌且 `check_memory` 回退写入 `user_identities`；修复后通过，且篡改签名、停用用户、缺失数据库接缝均被拒绝，同时 `user_identities` 保持 0 行。
- 分批与全量验证：`python3 tools/check_architecture.py`、`.venv/bin/ruff check .`、`.venv/bin/ruff format --check .`、`.venv/bin/mypy src`（119 个源文件）与 `git diff --check` 全部通过。
- 最终 `.venv/bin/pytest -q --tb=short --show-capture=no` 全量通过：**287 passed in 98.53s**。
- 测试使用本机 PostgreSQL 的隔离临时数据库及受控模型/记忆提供方；未执行构建或真实外部供应商请求。

### 文档与后续边界

已同步 `docs/backend-scaffold-design.md` 的目录状态、功能索引、测试夹具说明、测试契约和运维装配依赖，以及 `docs/operations/deployment-and-recovery.md` 的内置认证探针与 `check_memory` 用户 ID 复用说明（术语已按 `GLOSSARY.md` 统一为“用户”）。资源与事务所有权不变。

当前生产配置仍只提供旧 `auth_mode` 选项，因此测试配置暂保留严格 JWT 回退所需的 issuer/audience/secret，以拒绝匿名请求；没有有效业务请求使用该外部签发路径，所有有效令牌均由内置认证签发。07 应移除该过渡配置及生产外部认证/权限结构，并继续扩展保留的历史迁移兼容测试。
