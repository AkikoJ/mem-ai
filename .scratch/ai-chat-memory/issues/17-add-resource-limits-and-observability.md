# 17: 接入资源限制与运行观测

**What to build:** 让部署可限制资源并定位生成、记忆和任务故障，提供可运营的 API 与 Worker。

**Blocked by:** [09 持久化任务并重试记忆写入](09-persist-and-retry-memory-writes.md), [12 流式返回与确认生成终态](12-stream-replies-with-terminal-status.md), [13 删除与清空记忆并阻止旧任务回写](13-delete-memories-with-epoch-barriers.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 扩展 01 的配置、日志、错误及应用生命周期入口，复用已落地的生成、记忆和任务状态来源。
- 资源预算和遥测由 core 公共能力承接，API、SSE 与 Worker 在既有状态转换处记录事件。
- 预算超限、超时与退出通过既有生成和任务收敛入口处理。

## 验收条件

- [x] 入口分布式限流与应用可替换限制边界明确，应用限制进程并发、请求体、输入、上下文、生成及流时长和 Worker 并发。
- [x] 预算超限使用稳定 429 及适用重试提示，运行中的生成按状态机收敛，心跳与代理超时配置协调。
- [x] 生产使用 CORS、Host、可信代理和网关白名单，伪造身份与请求 ID 测试覆盖边界。
- [x] JSON 日志关联请求、会话、生成和任务，过滤正文、Token、回执和密钥，低基数指标与业务标识分离。
- [x] 指标覆盖请求和模型延迟、活跃及失败生成、租约、记忆降级、积压、最老等待、待核验、最近成功及屏障时长。
- [x] 就绪检查包含数据库与迁移兼容版本，Worker 暴露心跳，指标访问受控并预留追踪接缝。
- [x] 针对性测试验证资源超限、敏感字段过滤、指标状态、依赖故障与优雅退出。

## Answer

### 实际公共符号与后续切片调用入口

- **资源限制边界与配置（`src/mem_ai/core/limits.py`、`src/mem_ai/core/config.py`、`src/mem_ai/core/exceptions.py`、`src/mem_ai/core/exception_handlers.py`、`.env.example`）**：
  - `RateLimitRequest`、`RateLimitDecision`、`RateLimiter`（可替换限流协议，入口网关负责分布式限流时默认装配 `NoopRateLimiter`；单节点或测试可装配有界 `FixedWindowRateLimiter`）、`compute_retry_after_seconds`、`ConcurrencyPermit` 与 `ConcurrencyLimiter`。
  - `Settings` 新增 `allowed_hosts`（生产必填且禁止 `*`）、`trust_request_id_from_proxies_only`（生产默认 `True`）、`max_request_body_bytes`、`max_concurrent_requests`、`max_concurrent_generations`、`rate_limit_retry_after_seconds`、`chat_message_max_chars`、`proxy_read_timeout_seconds`（强制 `chat_stream_heartbeat_interval_seconds < proxy_read_timeout_seconds`）、`compatible_alembic_revisions`、`metrics_enabled`、`metrics_allowed_networks`（生产禁止 `0.0.0.0/0` 与 `::/0`）、`metrics_bearer_token`、`worker_heartbeat_interval_seconds`、`worker_heartbeat_file`、`worker_metrics_host` 与 `worker_metrics_port`。
  - `AppError` 支持响应头透传，`RateLimitError` 支持 `retry_after_seconds` 自动附带 `Retry-After` 头与 `details.retry_after_seconds`，新增 `PayloadTooLargeError`（`413 payload_too_large`）。
- **低基数指标、日志脱敏与追踪接缝（`src/mem_ai/core/telemetry.py`、`src/mem_ai/core/logging.py`）**：
  - `TelemetryRegistry`（通过 `FORBIDDEN_METRIC_LABEL_KEYS` 与 `validate_metric_labels` 强制禁止业务 ID 进入指标标签）、`GaugeSample`、`HistogramSnapshot`、`render_prometheus`、`bind_telemetry`/`reset_telemetry`/`current_telemetry`/`current_tracer`，以及 `Span`/`Tracer`/`NoopTracer`/`InMemoryTracer`/`OtelTracerAdapter` OpenTelemetry 追踪接缝。
  - `core/logging.py` 扩展消息正文键（`content`/`user_message`/`assistant_message`/`reply`/`delta`/`text`/`body`）及内联 `Bearer <token>` / `DeletionReceipt <receipt>` 正则脱敏（`redact_inline_secrets`），同时保留安全关联字段（`request_id`/`user_id`/`conversation_id`/`generation_id`/`provider`/`job_id`）及用量统计（`input_tokens`/`output_tokens`/`total_tokens`）。
- **API 中间件、受控 `/metrics` 与只读观测快照（`src/mem_ai/api/middleware.py`、`src/mem_ai/api/dependencies.py`、`src/mem_ai/application.py`、`src/mem_ai/modules/health/{service,router}.py`）**：
  - `AllowedHostMiddleware`（校验单一且匹配的 `Host`，拒缺省/重复/非法 Host 为 `400 invalid_host`）、`IdentityHeaderSanitizerMiddleware`（非可信代理清理伪造身份与转发头）、`ResourceBudgetMiddleware`（可替换限流、UUID 路径归一化键、进程请求并发 `429`、`Content-Length` 与分块流 `413`）及 `RequestContextMiddleware`（可信代理 `X-Request-Id`、`effective_route_context` 低基数路由模板指标与 HTTP 追踪 Span）。
  - `create_app(..., rate_limiter=..., telemetry=..., tracer=...)` 维护 `app.state.ready` 启动/退出就绪状态；`modules/health/service.py` 提供 `resolve_compatible_migration_revisions`、支持多兼容迁移版本的 `check_readiness`、`authorize_metrics_access`、`collect_prometheus_metrics` 与共享 `build_metrics_response`，`modules/health/router.py` 暴露受控 `GET /metrics`。
  - 只读观测聚合快照：`conversations/queries.py:get_generation_observability`、`memories/barrier.py:get_memory_barrier_observability`（结合 `operations/service.py:get_oldest_operation_created_at_in_session` 从操作创建时间计算活跃屏障真实时长）及 `jobs/service.py:get_job_observability`（补充 `last_success_at`）。
- **生成、记忆与 Worker 运行埋点（`src/mem_ai/modules/conversations/{execution,generation,service,stream,router,schemas}.py`、`src/mem_ai/modules/memories/{recall,barrier}.py`、`src/mem_ai/workers/{runner,main}.py`）**：
  - 共享生成执行引擎记录 `mem_ai_generations_active`、`mem_ai_generations_total`、`mem_ai_model_requests_total`、`mem_ai_model_request_duration_seconds`、`mem_ai_model_first_token_duration_seconds`（TTFT）与 `mem_ai_generation_lease_events_total`；`conversations/service.py` 与 `SseStreamingResponse` 统一执行 `max_concurrent_generations` 与 `chat_message_max_chars` 预算并保证幂等重试不误触 429、断连不泄漏并发许可。
  - `memories/recall.py` 绑定 `provider` 日志上下文并记录召回/降级指标与 `mem_ai_memory_provider_degraded` 独立状态（普通记忆故障降级不破坏 `/health/ready`）；`memories/barrier.py` 记录 `mem_ai_memory_barrier_duration_seconds`。
  - `WorkerRunner` 绑定 `telemetry`/`tracer` 与任务关联日志，输出周期 JSON 心跳日志与原子心跳文件（`worker_heartbeat_file`），`workers/main.py` 提供 `build_worker_probe_app`（暴露带请求 ID 的 `/health/live`、`/health/ready` 及受控 `/metrics`）并在配置 `worker_metrics_port` 时随 Worker 优雅启停。

### 复用、迁移与双轴代码审阅修复

- 直接复用既有 `conversations/generation.py` 状态机与过期收敛、`memories/recall.py` 降级与 `memories/barrier.py` 屏障释放、`jobs/service.py` 与 `workers/runner.py` 租约和优雅退出框架（无需新增数据库迁移）。
- 经双轴 `code-review`（Standards & Spec）修复：
  1. 修复 `conversations/execution.py` 在外层整体超时取消时将模型超时误记为 `cancelled` 的问题，统一抽离 `_run_observed_model_call` 并绑定 `provider` 日志上下文；将 `conversations/schemas.py` 硬编码 32k 上限放宽至与 `Settings.chat_message_max_chars` 一致的 200k 安全上界。
  2. 修复 `SseStreamingResponse` 在迭代开始前断连可能泄漏生成并发许可的问题，并将 `generation_limiter` 同步覆盖无幂等键的 `run_sync_chat`；修复用户主动清空记忆（`memory_scope_clearing`）误触发全局 `mem_ai_memory_provider_degraded=1` 告警的问题。
  3. 修复活跃记忆屏障时长在 `clearing -> reconciling` 转换时因读 `updated_at` 而归零的问题（改为经 `operations_service.get_oldest_operation_created_at_in_session` 读取屏障操作 `created_at`）。
  4. 修复 `AllowedHostMiddleware` 仅校验首个 `Host` 头的重复头绕过风险，归一化 `ResourceBudgetMiddleware` 限流键中的 UUID 路径段；消除 `workers/runner.py` 租约丢失重复记录、抽取共享 `build_metrics_response`、为 `build_worker_probe_app` 挂载 `RequestContextMiddleware`、在 `health/router.py` 改用 `Depends` 注入，并同步补齐 `docs/backend-scaffold-design.md` §3.2 职责表。

### 验证记录与剩余限制

- `.venv/bin/pytest -q`：**227 passed**（新增 `tests/unit/test_limits_and_telemetry.py`、`tests/api/test_limits_and_security_boundaries.py`、`tests/api/test_observability_and_metrics.py`，扩展 `tests/unit/test_config.py`、`tests/unit/test_logging_and_errors.py` 与 `tests/architecture/test_architecture_rules.py`）。
- `.venv/bin/ruff format --check src tests migrations tools`、`.venv/bin/ruff check src tests migrations tools`、`.venv/bin/mypy src`（111 source files）、`python3 tools/check_architecture.py`、`git diff --check` 均通过。
- 剩余范围：入口网关分布式限流规则、生产 TLS/反向代理、容器构建、备份恢复与故障演练随 Ticket 18 落地。目录树、状态表、功能定位索引与 §3.2 职责表已同步至 `docs/backend-scaffold-design.md`。
