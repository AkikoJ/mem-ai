# 01: 建立 FastAPI 与 PostgreSQL 基础

**What to build:** 让应用使用测试配置启动，连接 PostgreSQL、执行迁移并通过公开健康接口验证，为业务切片提供可替换的提供方和测试入口。

**Blocked by:** None (can start immediately)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 建立应用装配、项目协议、公共分页与错误处理，后续业务切片复用这些入口。
- 本切片建立 Ruff、mypy、pytest 和 CI 的最小执行链，先接入 `python3 tools/check_architecture.py`，再按工程设计第 13.4 节将检查扩展到已落地模块。
- 业务实现按后续切片逐步增加，计划目录随实际文件更新。

## 验收条件

- [x] `create_app()`、生命周期及 API 进程入口可用，资源在停机时释放，配置在启动时校验。
- [x] SQLAlchemy 异步 Session 与 psycopg、连接池和 Alembic 同步迁移可用，外部调用在事务外执行。
- [x] 空 PostgreSQL 数据库能够升级，元数据漂移检查能够发现缺失迁移。
- [x] 存活和数据库就绪接口、请求 ID、JSON 日志及嵌套 error 信封可通过 HTTP 测试验证，错误码为稳定小写下划线。
- [x] 项目拥有 ModelProvider、MemoryProvider 和 PersonaProvider 的类型化装配边界，依赖覆盖可替换提供方。
- [x] fixture 可以创建应用、PostgreSQL、HTTP 客户端和受控提供方，隔离测试数据并清理资源。
- [x] Ruff、mypy、pytest、架构与文档检查具备最小执行入口，CI 对当前已落地范围运行这些检查，后续切片扩展同一流水线。
- [x] 目录注释、文件职责与实际新增文件同步。

## Answer

### 实际公共符号与后续调用入口

- **应用装配与 ASGI 入口**：
  - `src/mem_ai/application.py`：`create_app(settings: Settings | None = None, *, providers: ProviderRegistry | None = None) -> FastAPI`，在 `lifespan` 中创建并释放 `AsyncEngine`、`async_sessionmaker[AsyncSession]` 与 `httpx.AsyncClient`。
  - `src/mem_ai/main.py`：暴露 `app = create_app()` 作为 Uvicorn ASGI 入口。
- **配置、日志与错误**：
  - `src/mem_ai/core/config.py`：`Settings`（`pydantic-settings` 启动校验、生产弱密码与 `*` CORS 拒绝、`database_url()` 通过 `URL.create("postgresql+psycopg", ...)` 组装）。
  - `src/mem_ai/core/logging.py`：`JsonLogFormatter`、`configure_logging`、`bind_log_context`、`reset_log_context`、`sanitize_log_value`。
  - `src/mem_ai/core/exceptions.py`：`AppError`（校验稳定小写下划线错误码）及 `UnauthorizedError`、`ForbiddenError`、`NotFoundError`、`ConflictError`、`UnprocessableEntityError`、`RateLimitError`、`BadGatewayError`、`ServiceUnavailableError`、`GatewayTimeoutError`。
  - `src/mem_ai/core/exception_handlers.py`：`register_exception_handlers`、`build_error_envelope`（统一 `{success, code, message, data, request_id, details}` 响应信封，422 校验错误过滤原始 `input`）。
- **数据库、迁移与外部 HTTP 基础设施**：
  - `src/mem_ai/db/base.py`：`Base`、`NAMING_CONVENTION`、`UUIDPrimaryKeyMixin`、`CreatedAtMixin`、`TimestampMixin`。
  - `src/mem_ai/db/engine.py`：`create_async_engine_from_settings`、`create_session_factory`、`create_sync_engine_from_settings`。
  - `src/mem_ai/db/model_registry.py`：`load_all_models()`，供后续切片集中显式导入 ORM 模型。
  - `src/mem_ai/integrations/http.py`：`create_http_client(settings)`。
  - `alembic.ini`、`migrations/env.py`、`migrations/script.py.mako`、`migrations/versions/20261121_0001_initial_foundation.py`（基线版本 `20261121_0001`）。
- **项目提供方协议与依赖装配**：
  - `src/mem_ai/contracts/model_provider.py`：`ModelProvider`、`ModelRequest`、`ModelResponse`、`ModelStreamChunk`、`ModelUsage`、`ModelMessage`。
  - `src/mem_ai/contracts/memory_provider.py`：`MemoryProvider`、`MemoryRecord`、`MemoryRecallQuery`、`MemoryRecallItem`、`MemoryWriteInput`、`MemoryWriteResult`、`MemoryListQuery`、`MemoryListPage`、`MemoryDeleteResult`、`MemoryOperationStatus`。
  - `src/mem_ai/contracts/persona_provider.py`：`PersonaProvider`、`PersonaSnapshot`、`MemoryPolicy`。
  - `src/mem_ai/api/dependencies.py`：`ProviderRegistry`、`SettingsDep`、`SessionFactoryDep`、`HttpClientDep`、`ModelProviderDep`、`MemoryProviderDep`、`PersonaProviderDep`（均支持 `app.dependency_overrides` 替换）。
- **公共 HTTP、统一响应、分页与健康检查**：
  - `src/mem_ai/api/responses.py`：`ApiResponse`、`ok_response`（统一 `{success, code, message, data, request_id}` 成功响应外壳）。
  - `src/mem_ai/api/pagination.py`：`PaginationParams`、`CursorPayload`、`PaginatedResponse`、`encode_cursor`、`decode_cursor`、`hash_filters`。
  - `src/mem_ai/api/middleware.py`：`RequestContextMiddleware`、`register_middleware`。
  - `src/mem_ai/api/router.py`：`build_api_router()`（挂载健康检查路由与 `/v1` 业务路由前缀）。
  - `src/mem_ai/modules/health/service.py`、`src/mem_ai/modules/health/router.py`：`GET /health/live` 与 `GET /health/ready`（有界检查 PostgreSQL 连通及 Alembic 迁移版本）。
- **测试 Fixture 与 CI**：
  - `tests/conftest.py`：`empty_pg_settings`、`migrated_pg_settings`、`FakeModelProvider`、`FakeMemoryProvider`、`FakePersonaProvider`、`app`、`async_client`。
  - `.github/workflows/checks.yml`：按工程设计第 13.2 节顺序执行锁文件同步、Ruff、mypy、Alembic 升级与漂移检查、pytest 与架构检查。

### 复用、扩展与架构检查同步

- 复用并扩展原有 `compose.yaml`（支持 `${POSTGRES_DATA_DIR:-postgres_data}` 宿主机绑定挂载或默认命名卷）、`.env.example` 与 `tools/check_architecture.py`（新增分层导入方向检查、显式装配例外清单 `ASSEMBLY_EXCEPTIONS` 与 `spec.md`/ticket 依赖图一致性及无环检查），未引入重复实现。
- 在 `modules/health/` 中拆分 `service.py` 与 `router.py`，保持 Router 不直接依赖 SQLAlchemy。

### 验证记录与剩余限制

- 本地 Docker PostgreSQL 17 已通过 `docker compose up -d --wait` 启动且健康，数据卷已绑定持久化至 `/Users/mac/Documents/docker-volumn/mem-ai-postgres`。
- `.venv/bin/alembic upgrade head && .venv/bin/alembic check`：通过（升级至 `20261121_0001`，无元数据漂移）。
- `.venv/bin/pytest`：24 passed（覆盖 unit、api、PostgreSQL integration 与 architecture 测试）。
- `.venv/bin/ruff check .` 与 `.venv/bin/ruff format --check .`：All checks passed。
- `.venv/bin/mypy src`：Success: no issues found in 27 source files（strict 模式）。
- `python3 tools/check_architecture.py`：工程目录、功能索引、文档链接、分层导入、依赖图和已落地源码检查通过。
- 剩余依赖与后续切片范围：具体业务表、认证上下文（02）、私有 Agent（03）、MemOS 适配器（04）与模型适配器（05/06）将在对应切片按已确立的协议与装配边界接入；GitHub Actions 流水线 `.github/workflows/checks.yml` 需在推送至远程仓库后由 GitHub Runner 首次实际触发运行。

### 目录与文档同步

- 已按 `backend-design` 技能同步 `docs/backend-scaffold-design.md` 第 3 节当前目录状态、目录树（补充 `modules/health/service.py` 及工具职责注释）与 `README.md` 本地 PostgreSQL 数据卷和检查命令说明。
