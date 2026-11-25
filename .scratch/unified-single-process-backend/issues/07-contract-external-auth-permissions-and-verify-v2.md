# 07: 移除外部认证适配器、权限字段与遗留表结构并完成全量验收（Contract）

**What to build:** 彻底移除外部认证适配器、可信网关认证配置、身份上下文中的权限集合以及遗留的 `user_identities` 表与 `model_configs.required_permission` 列，完成空库及 V1 历史库迁移升级验证，并同步架构检查与工程文档完成一体化单进程后端的最终验收。

**Blocked by:** [01 将人设只读快照收口至 agents 模块并移除过渡适配层](01-consolidate-persona-snapshots-into-agents.md)、[03 移除独立 Worker 进程入口并统一单进程运维与容器配置](03-retire-standalone-worker-and-unify-ops.md)、[06 将全量测试基线迁移至纯内置认证接缝（Migrate 测试套件）](06-migrate-test-suite-to-builtin-auth-seam.md)

**Status:** ready-for-agent

**Parent:** [一体化单进程 Python 后端体系收敛规格](../spec.md)

## 实现边界

- 本切片为宽重构的 **Contract** 与总收敛阶段：移除 `DevelopmentAuthAdapter`、`StaticTokenAuthAdapter`、`JwtHs256AuthAdapter` 外部签发方模式、`TrustedGatewayAuthAdapter` 及相关配置项和网关认证头分支（保留 Host/CORS/可信代理 IP 解析、`X-Request-Id` 白名单与删除回执隔离）。
- 精简 `AuthenticationContext`，移除 `VerifiedPrincipal`、`actor_id` 与 `permissions`；同步清理模型配置与同步种子中的 `required_permission`。
- 新增收缩阶段 Alembic 迁移删除 `user_identities` 表与 `model_configs.required_permission` 列，补全 `users.email` / `password_hash` 非空约束收口，并验证空库升级、从 V1 基线版本一路升级到最新头版本以及元数据漂移检查。
- 同步更新工程设计文档的目录树、当前目录状态表（清除全部过渡标注）、功能定位索引、配置模板与架构检查脚本，完成单进程一体化后端的端到端验收。

## 验收条件

- [ ] 所有外部认证适配器、外部 `issuer`/`audience` 校验、可信网关认证头模式、`VerifiedPrincipal`、`AuthenticationContext.permissions` 与 `required_permission` 代码和配置已彻底清除。
- [ ] Alembic 收缩迁移在空 PostgreSQL 库和已填充数据的 V1 历史库上均能顺利升级到最新 head，且 `alembic check` 无元数据漂移；升级后既有 Agent、人设版本、会话、消息、记忆引用、任务与操作记录保持完整可用。
- [ ] 单进程端到端验收通过：仅启动一个服务实例加一个 PostgreSQL，完成注册登录 → 创建并发布私有 Agent → 同步与流式聊天 → 异步记忆写入与召回 → 刷新轮换与改密 → 清空屏障与删除用户（含防同邮箱重注册及回执查询）→ 优雅停机与恢复重放全闭环。
- [ ] 工程设计文档、环境变量模板、架构检查脚本与架构测试完全同步，无残留过渡标注，Ruff、mypy、Alembic 校验、全量 pytest 与 `python3 tools/check_architecture.py` 全部通过。
