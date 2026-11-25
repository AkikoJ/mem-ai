# 06: 将全量测试基线迁移至纯内置认证接缝（Migrate 测试套件）

**What to build:** 将共享测试夹具与全量 API、集成和单元测试从静态令牌与外部主体替身迁移至内置注册/登录会话接缝，使整个测试套件直接通过最高层内置认证路径验证用户隔离、会话生命周期、对话、记忆与删除契约。

**Blocked by:** [05 切换用户删除防重注册与模型无权限路由至新契约（Migrate 核心业务）](05-migrate-user-deletion-and-permissionless-models.md)

**Status:** ready-for-agent

**Parent:** [一体化单进程 Python 后端体系收敛规格](../spec.md)

## 实现边界

- 本切片为宽重构的测试迁移阶段：在共享测试夹具中提供基于内置注册/登录（或内置令牌签发辅助）的双用户认证夹具，替代 `StaticTokenAuthAdapter` 与外部主体构造器。
- 逐批迁移所有业务 API 测试、PostgreSQL 集成测试与模型策略单元测试，使其不再引用外部认证适配器、可信网关头、`user_identities` 外部映射或 `permissions` / `required_permission` 测试夹具。
- 验证所有跨用户 `404` 隔离、停用/删除中 `403` 拦截、删除回执越权拦截及多用户并发行为在内置认证接缝下全部通过。

## 验收条件

- [ ] 共享测试夹具默认使用内置认证模式，提供可复用的多用户注册登录辅助并移除业务测试对静态令牌与外部主体替身的依赖。
- [ ] 全量业务 API 测试（用户、Agent、人设版本、模型、聊天同步/流式/并发/幂等/记忆召回与写入、会话、记忆与删除、限流与观测）全部通过真实内置认证接缝运行并绿灯。
- [ ] 全量集成测试与单元测试不再依赖 `user_identities` 映射构造用户或 `required_permission` 权限断言（仅保留将在 07 随历史库升级测试处理的迁移兼容用例）。
- [ ] 全量 pytest、Ruff、mypy 与 `python3 tools/check_architecture.py` 全部通过。
