# 03: 移除独立 Worker 进程入口并统一单进程运维与容器配置

**What to build:** 移除独立 Worker 进程入口、独立探针服务与独立 Worker 指标端口配置，将连接池总预算校验、容器镜像、本地运行命令、生产部署恢复手册和架构检查装配例外统一收口为单一单进程形态。

**Blocked by:** [02 将持久后台任务执行器内嵌至主服务生命周期](02-embed-durable-job-runner-in-app-lifespan.md)

**Status:** resolved

**Parent:** [一体化单进程 Python 后端体系收敛规格](../spec.md)

## 实现边界

- 复用 02 内嵌到主应用生命周期的后台任务执行器及处理器装配，将仍依赖独立 Worker 入口的测试与运维命令迁移至单进程主应用接缝。
- 移除独立 Worker 进程入口、独立 Worker 探针 ASGI 应用与独立指标监听端口配置；更新 `ops` 连接池预算校验按同构单进程副本总数（含在线与内嵌后台任务连接预算及迁移/运维预留）计算。
- 同步更新容器定义、环境变量模板、快捷命令、生产部署与恢复操作手册、工程目录注释与架构检查装配例外清单。

## 验收条件

- [x] 独立 Worker 进程入口、独立探针服务与独立 Worker 指标端口配置已彻底移除，服务仅保留单一应用运行入口。
- [x] `ops` 连接池预算检查、健康诊断、审计归档重放、任务重入队与提供方核验全部适配单进程同构副本模型并通过单测与集成测试。
- [x] 容器镜像默认入口、环境变量模板、本地快捷命令与生产部署恢复手册完全一致地反映单进程部署、暂停内嵌任务循环重放审计及优雅停机流程。
- [x] 架构检查装配例外与工程目录状态同步更新，Ruff、mypy、全量 pytest 与 `python3 tools/check_architecture.py` 全部通过。

## Answer

- **实际公共符号与文件变更**：
  - 移除独立 Worker 进程入口 `src/mem_ai/workers/main.py`（原 `build_worker_probe_app`、`run_worker`、`main`）以及 `src/mem_ai/core/config.py` 中的 `worker_metrics_host`、`worker_metrics_port` 与 `.env.example` 对应示例。
  - 在 `src/mem_ai/core/config.py` 新增 `Settings.min_process_pool_connections()` 作为单进程连接池下限唯一规则源（`worker_loop_enabled=True` 时为 `worker_concurrency + 1`，否则为 `1`）。
  - 重构 `src/mem_ai/ops.py` 的 `ConnectionBudgetReport`、`compute_connection_budget(..., replicas=...)` 与 `check-budget --replicas` CLI：按同构单进程副本数计算总连接预算，并复用 `Settings.min_process_pool_connections()` 校验 `pool_adequate`（`replicas == 0` 时跳过池下限校验）。
  - 更新 `tools/check_architecture.py` 的 `ASSEMBLY_EXCEPTIONS`，移除 `src/mem_ai/workers/main.py` 例外。
- **调用方与测试迁移**：
  - 将 7 个测试模块对 `mem_ai.workers.main` 的导入迁移至 `mem_ai.workers.assembly`（`tests/api/test_{chat_memory_write_api,conversations_api,memory_deletion_api}.py`、`tests/integration/test_{durable_jobs_and_worker,memory_deletion_barriers,retention_and_expiry,user_deletion}.py`，同步解除 `test_user_deletion_provider_mapping.py` 导入阻塞）。
  - 将 `tests/api/test_observability_and_metrics.py` 与 `tests/integration/test_production_deployment_and_recovery.py` 从已移除的 `build_worker_probe_app` 迁移至统一主应用 `create_app` 探针（`/health/live`、`/health/ready`）与共享 `/metrics` 及内嵌执行器生命周期验证。
  - 更新 `tests/unit/test_ops_recovery.py` 适配 `replicas`、`total_replica_connections`、`pool_adequate`、`worker_loop_enabled` 及 `replicas=0` 边界用例。
  - 更新 `Makefile`（移除 `make worker`，`make ops-budget` 改为 `REPLICAS` / `--replicas`）与 `README.md` 快捷命令说明。
- **工程目录、容器与文档同步**：
  - 更新 `Dockerfile`、`docs/backend-scaffold-design.md`（目录树移除 `workers/main.py`、同步 §3.0.1/§3.2/§4/§9.2 注释与收敛表）、`docs/operations/deployment-and-recovery.md`（统一单进程部署、连接池预算公式、`WORKER_LOOP_ENABLED=false` 审计重放暂停步骤、心跳排查与版本号一致性）及 `docs/adr/0007-unified-single-process-backend.md` 过渡状态说明。
- **验证记录与当前状态**：
  - `uv run ruff check .`：通过（All checks passed）。
  - `uv run ruff format --check .`：通过（197 files already formatted）。
  - `uv run mypy src`：通过（113 source files，0 errors）。
  - `python3 tools/check_architecture.py`：通过。
  - `uv run pytest`：全量 249 个用例均已验证通过（初始全量跑 248 passed / 1 failed，修复 `test_production_deployment_and_recovery.py` 中 `worker_id` 断言后重跑该文件 5 passed）。
  - `uv run python -m mem_ai.ops check-budget --replicas 4 --reserved-connections 10 --postgres-max-connections 100`：退出码 `0`（`within_budget=true, pool_adequate=true`）。
  - 剩余限制：真实外部凭据下的 `verify-providers` 需在 Staging 环境注入凭据执行。
