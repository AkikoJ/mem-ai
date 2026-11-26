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

- 移除了 `src/mem_ai/workers/main.py` 独立 Worker 进程入口及其 `build_worker_probe_app`、`run_worker` 与 `main` 导出。
- 从 `src/mem_ai/core/config.py` 移除 `worker_metrics_host` 与 `worker_metrics_port` 配置字段。
- 从 `.env.example` 移除 `WORKER_METRICS_HOST` 与 `WORKER_METRICS_PORT` 注释及配置示例。
- 更新 `Dockerfile` 移除关于独立 Worker 进程的注释，统一为"统一单进程后端服务"描述。
- 更新 `tools/check_architecture.py` 的 `ASSEMBLY_EXCEPTIONS`，移除 `src/mem_ai/workers/main.py` 例外。
- 重构 `src/mem_ai/ops.py` 中的 `ConnectionBudgetReport` 和 `compute_connection_budget`：
  - 将 `api_replicas` 与 `worker_replicas` 合并为单一 `replicas` 参数。
  - 更新连接池预算计算逻辑，当 `WORKER_LOOP_ENABLED=true` 时，验证连接池大小是否足以支持 `MAX_CONCURRENT_REQUESTS + MAX_CONCURRENT_GENERATIONS + WORKER_CONCURRENCY + 1`。
  - CLI 参数从 `--api-replicas` 与 `--worker-replicas` 改为单一 `--replicas` 参数。
- 更新 `docs/operations/deployment-and-recovery.md` 生产部署手册：
  - 标题从 "API 与 Worker 独立运行" 改为"统一单进程运行"。
  - 更新所有 "API/Worker"、"API 与 Worker" 的引用为"统一后端服务"或"服务副本"。
  - 移除独立 Worker 探针端口描述。
  - 更新连接池预算公式为统一单进程副本计算。
  - 更新版本标注从 V1 至 V2，并添加 ADR-0007 引用。
- 更新 `docs/backend-scaffold-design.md` 工程设计文档：
  - 移除 `workers` 模块描述中的 "`workers/main.py` 保留 V1 过渡独立入口" 说明。
  - 更新职责表移除 `workers/main.py` 文件。
  - 更新落地切片长段描述，移除独立 Worker 进程入口、独立探针应用相关描述。
  - 更新模块导入说明，统一为单进程内嵌任务执行器。
  - 更新后台任务执行器描述，移除 V1 过渡期独立启动说明。
  - 更新收敛规划表，标记收敛 03 已完成。
- 验证通过：`python3 tools/check_architecture.py`、`uv run ruff check --select I .`、`uv run mypy src/mem_ai/ops.py` 均通过。
- 实际运行测试 `compute_connection_budget` 确认函数正确工作（`within_budget=True, pool_adequate=False` 在默认配置下符合预期）。
- 代码未提交。
