# 定位为一体化单进程 Python 后端服务

状态为 accepted。本决策取代 [ADR-0001](0001-modular-monolith-and-provider-seams.md)、[ADR-0002](0002-defer-cordis-runtime.md) 和 [ADR-0006](0006-shared-postgres-jobs.md)。

Mem-AI 不再定位为“为未来拆分独立记忆服务、独立人设服务或接入插件运行时预留接缝的无头对话组件”，而是定位为**自包含、一体化的 AI 对话应用 Python 后端服务**。项目以单一代码库、单一服务进程和 PostgreSQL 这一唯一必选有状态基础设施，提供从内置认证、Agent/人设、同步与流式对话、长期记忆到持久化后台任务、可观测性与恢复运维的完整后端体系。

## 核心决策

1. **放弃内部服务拆分与 Cordis 插件运行时**：不再规划独立记忆服务、独立人设服务、远程内部适配器或 Cordis 插件运行时。人设的草稿、版本发布与只读快照解析统一归口 `agents` 模块，不再维护独立的 `PersonaProvider` / `personas` 拆分接缝。内部保持清晰的模块归属与分层纪律，防止演化为大泥球；仅对真正的外部第三方系统（大语言模型接口、MemOS 等记忆引擎）保留适配边界，以隔离外部协议并便于测试替换。
2. **收拢为单进程运行与内嵌持久任务循环**：移除独立 Worker 进程及其独立探针/指标端口，将基于 PostgreSQL `jobs` 表（`FOR UPDATE SKIP LOCKED`、租约、指数退避、外部核验与周期窗口去重）的异步任务执行器直接内嵌到 FastAPI `lifespan` 中统一启停与优雅排空。横向扩展时部署同构单进程副本即可安全并发消费（按配置开关控制是否启用本副本的任务循环）。
3. **内置极简用户认证，去除外部认证与角色权限复杂度**：取消外部 OAuth/SSO、可信网关头与 `user_identities` 外部身份映射，统一为内置 `email` + 密码哈希 + JWT（短期 Access Token 与落库可撤销 Refresh Token `auth_sessions`）完整认证闭环（注册、登录、刷新、注销、改密），是否开放自助注册由配置控制。暂不引入角色权限（RBAC）与管理端 HTTP API，移除 `permissions` 与模型 `required_permission` 过滤；所有活跃用户平等使用服务端已启用模型，并严格执行本人私有资源归属隔离。模型配置同步与系统运维统一走配置文件/环境变量、受控 `/metrics` 与运维 CLI。
4. **PostgreSQL 单栈基础设施基线**：不把 Redis、Celery 或独立消息队列作为必选依赖，业务主数据、持久任务、刷新令牌会话、分页快照与审计元数据默认全部基于 PostgreSQL 即可完整运行。

## 权衡与备选方案

- **保留 API + Worker 双进程 vs 纯内存协程 vs 单进程内嵌持久任务**：保留双进程会增加部署拓扑、探针与运维心智，不符合“整体作为一个单一 Python 后端服务”的定位；改为纯内存 `BackgroundTasks` 虽最轻，但会在重启时丢失记忆写入与删除进度并破坏 [ADR-0005](0005-memory-epochs-and-deletion-barriers.md) 的删除屏障与 `reconciling` 核验契约。单进程内嵌 PostgreSQL 持久任务循环在运维简洁度与崩溃可恢复性之间取得平衡。
- **保留外部认证与两级 RBAC vs 纯内部认证且无角色权限**：当前产品形态聚焦独立 AI 对话应用后端，过早维护多套外部认证适配器、权限点过滤与后台管理 API 会显著增加状态面和心智负担；去掉它们后认证和授权模型收缩为“登录态 + 本人资源归属”，更贴合完整而克制的单体体系。

## 后续影响

- HTTP 请求与内嵌后台任务共享同一进程的事件循环、HTTPX 连接池与 PostgreSQL 连接池，因此必须通过独立的并发预算（请求并发、生成并发、后台任务并发）与连接池总预算隔离资源竞争，并在停机时统一排空同步生成与在途后台任务。
- 用户删除期间改为保留 `users` 行处于 `deleting` 状态（占用唯一 `email` 防同邮箱重复注册）并立即吊销全部 `auth_sessions`，待本地与远程记忆清理确认完成后再物理移除用户记录。
- `workers/main.py` 独立入口、`auth` 外部适配器及 `permissions` / `required_permission` 在对应重构切片完成前作为 V1 过渡实现保留。人设收敛切片已完成：只读快照和类型归入 `agents`，不再保留 `personas` 模块或 `PersonaProvider` 协议。
