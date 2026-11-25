# 02: 将持久后台任务执行器内嵌至主服务生命周期

**What to build:** 在主 FastAPI 应用生命周期中直接装配、启停并优雅排空基于 PostgreSQL 持久任务表的后台任务执行器，让单个 Python 后端进程即可同时承载在线 HTTP/SSE 请求与异步记忆写入、删除清理、租约恢复和周期保留任务，并将任务心跳与指标并入主服务观测体系。

**Blocked by:** None (can start immediately)

**Status:** resolved

**Parent:** [一体化单进程 Python 后端体系收敛规格](../spec.md)

## 实现边界

- 复用既有的 `jobs` 持久化表、只读领域视图、行锁跳过领取、租约续期、退避抖动、`reconciling` 核验与业务任务处理器装配逻辑，以及 `WorkerRunner` 的周期窗口去重与心跳记录能力。
- 在应用工厂与配置中增加内嵌后台任务执行器的启停开关、独立并发预算与停机排空协同：启动时随应用生命周期拉起消费循环，停机时先置未就绪、停止领取新任务并在有界期限内并行排空在途同步生成与在途后台任务，再关闭共享 HTTP 客户端与数据库连接池。
- 将内嵌执行器与主应用共享同一 `TelemetryRegistry` 与追踪器，使心跳、活跃任务数、任务积压与待核验计数直接汇入主服务日志与受控 `/metrics`；本切片暂不删除旧独立入口，确保过渡期间全量既有测试绿灯。

## 验收条件

- [x] 仅启动主 FastAPI 服务（不启动独立 Worker 进程）即可端到端完成普通对话后的异步记忆写入、单条/清空记忆删除、会话删除、用户删除、过期生成租约兜底恢复及周期保留清理。
- [x] 通过配置开关关闭自动轮询循环时，应用实例上的内嵌后台任务执行器支持单步确定性执行（`run_once`），并可在维护或恢复窗口安全暂停后台消费。
- [x] 同时运行两个同构主服务实例连接同一 PostgreSQL 时，后台任务通过 `FOR UPDATE SKIP LOCKED` 与周期窗口去重安全并发消费，无重复副作用。
- [x] 进程内 HTTP 请求并发、生成并发与内嵌后台任务并发相互独立受控，后台任务突发负载不会饿死在线健康检查或对话请求。
- [x] 应用收到停机信号时将 `/health/ready` 置为未就绪，停止领取新任务并在有限宽限内排空在途同步生成与后台任务，超时未完成任务保留可恢复租约状态。
- [x] 内嵌后台任务执行器的心跳与执行指标通过主服务日志和受控 `/metrics` 正确暴露，Ruff、mypy、全量 pytest 与 `python3 tools/check_architecture.py` 全部通过。

## Answer

- `src/mem_ai/application.py` 在 FastAPI `lifespan` 中通过 `workers/assembly.py` 装配 `WorkerRunner`、处理器与周期任务，并共享应用的数据库 Session 工厂、HTTP 客户端、`TelemetryRegistry` 和 tracer。`WORKER_LOOP_ENABLED` 默认开启自动消费；关闭后仍可通过 `app.state.worker_runner.run_once()` 确定性步进。实例通过 `app.state.worker_runner` 与 `app.state.worker_task` 暴露执行状态；停机时先置未就绪、通过 `request_shutdown()` 立即中断轮询循环而不打断排空阶段，并与 `SyncGenerationTaskRegistry.drain()` 并行完成有界排空后再释放共享资源。
- 将业务处理器注册和周期任务装配从 `workers/main.py` 迁入 `workers/assembly.py`，把长任务租约续期移至 `workers/lease.py`，把纯配置/健康快照模型与 `JobHandlerRegistry` 拆至 `workers/schemas.py`（`runner.py` 保持兼容重导出），并提取 `core/tasks.py`（`drain_tasks_bounded`）供同步生成注册表、内嵌任务执行器与应用停机复用单一总宽限排空。`WorkerRunner` 通过实例级信号量与领取槽位预留使 `run_forever` 与重叠 `run_once` 严格受限于 `worker_concurrency`；取消任务时先停止 `JobLeaseHeartbeat` 再等待处理器取消，防止关库后续租；启动与停机心跳前均刷新积压与待核验计数写入结构化日志，`/metrics` 复用单一实时口径 `mem_ai_jobs_backlog` / `mem_ai_jobs_reconciling`（无重复缓存指标）。连接池预留校验（`db_pool_size + db_max_overflow >= worker_concurrency + 1`）仅在 `worker_loop_enabled=True` 时生效。
- `tests/api/test_worker_lifespan.py` 使用真实迁移 PostgreSQL 和主应用 lifespan 覆盖异步记忆写入、单条与范围删除、会话和用户清理、两个同构应用实例的周期任务去重、关闭轮询后的手动步进、过期生成恢复、保留期清理、就绪状态、指标、并发饱和在线健康/对话隔离及 `run_once` 不超领、同步生成与后台任务并行排空、以及停机超时保留可恢复 `running` 租约与停止续租。测试中的模型与记忆提供方作为可控外部边界替身。
- 验证通过：全量 pytest `249 passed`；lifespan 测试 `11 passed`；`ruff check .`、`ruff format --check .`、`mypy src`、`python3 tools/check_architecture.py` 与 `git diff --check` 均通过。未运行构建命令。
- 同步更新了 `docs/backend-scaffold-design.md` 的 §3.0.1 落地表、§3.0.2 功能定位索引、目录树、§3.2 职责说明（含 `core/tasks.py`、`workers/{assembly,lease,schemas}.py` 与 `test_worker_lifespan.py`），以及 `.env.example` 和架构检查入口。`workers/main.py` 仍保留 V1 独立过渡入口，本 ticket 后续由 03 移除；代码未提交。
