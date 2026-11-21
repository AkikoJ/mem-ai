# 04: 查看和显式添加长期记忆

**What to build:** 让认证用户查看并同步显式添加自己的长期记忆，通过可替换 MemOS 适配器获得确定的创建结果。

**Blocked by:** [02 接入认证上下文和用户主体](02-connect-auth-context-and-user-subject.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 复用认证、分页及项目 MemoryProvider 契约，MemOS 协议集中在 memories 适配器。开始本切片实现前先完成一次 MemOS Cloud 真实接口最小能力验证（同步模式返回、状态查询、`tags`/`info` 过滤、删除收敛延迟）并记录结论。
- 稳定写入登记从统一 jobs 建立最小持久化与执行结果入口，09 在原表和入口扩展 Worker、租约与重试。
- 范围解析由 memories 提供（显式创建默认 `user`，可选 `conversation` 并校验会话归属），08 的召回、09 的普通回合写入和 13 的删除屏障扩展同一策略。

## 验收条件

- [x] 列表和显式创建通过公开路由验证，范围由认证用户确定，显式创建默认为用户级记忆，指定 `conversation` 时校验会话归属。
- [x] 提供方同步确认并返回稳定单条 ID 时返回 201；若提供方（如异步抽取型适配器）仅受理而尚未产出单条 ID，返回 202 与可查询 `operationId`；已知失败映射稳定应用错误。
- [x] 显式创建通过统一 jobs 的最小持久化入口保存稳定写入标识、目标范围和外部结果关联，请求路径同步领取执行，超时未知保留可核验信息供共享 Worker 和删除屏障接入。
- [x] 列表统一游标契约，默认 20、上限 100；对仅支持页码分页的供应商，有界分页快照持久化到 PostgreSQL `memory_list_snapshots` 表以支持多副本部署，适配器契约测试验证稳定 ID、排序、跨页、过期游标及用户隔离。
- [x] MemOS HTTP、鉴权及响应映射封装在 MemoryProvider 实现中，受控上游覆盖成功、超时和失败，凭据来自服务端。
- [x] 其他用户记忆无法列出或新增到其名下，错误和日志过滤供应商凭据及用户正文。

## Answer

### 实际公共符号与后续调用入口

- **统一任务最小持久化入口（`src/mem_ai/modules/jobs/`）**：
  - `src/mem_ai/modules/jobs/models.py`：`Job` ORM 模型（`jobs` 表，通用字段含 `job_type`、`business_id`、`user_id`、`dedupe_key`、`status` CHECK 约束、`attempt_count`/`max_attempts`、`lease_token`/`lease_expires_at`、`next_run_at`/`started_at`/`finished_at`、`external_ref`、`last_error_code`/`last_error_summary`、`metadata_json`，不含记忆领域专属列）。
  - `src/mem_ai/modules/jobs/schemas.py`：`JobStatus`、`JobRecord`。
  - `src/mem_ai/modules/jobs/repository.py`：`create_job`、`get_job_by_id`、`list_jobs_for_user`、`conditional_update_job_outcome`（带 `lease_token` 与 `lease_expires_at > current_time` 条件保护及 `populate_existing=True`）。
  - `src/mem_ai/modules/jobs/service.py`：事务参与入口 `register_and_claim_sync_job_in_session`、`complete_job_in_session`，以及查询入口 `get_job`、`list_jobs_for_user`（供 09 扩展 `SKIP LOCKED` 领取、退避重试与 `job_attempts`/`memory_jobs` 视图）。
- **异步操作追踪入口（`src/mem_ai/modules/operations/`）**：
  - `src/mem_ai/modules/operations/models.py`：`Operation` ORM 模型（`operations` 表）。
  - `src/mem_ai/modules/operations/schemas.py`：`OperationStatus`、`OperationRecord`、`OperationAcceptedResponse`、`OperationResponse`、`operation_status_url`。
  - `src/mem_ai/modules/operations/service.py`：事务参与入口 `create_operation_in_session` 与拥有者查询入口 `get_operation_for_owner`（复查活跃用户，跨用户统一返回 404）。
  - `src/mem_ai/modules/operations/router.py`：`GET /v1/operations/{operationId}`（挂载于 `/v1`，供 13–15 复用并扩展删除进度与回执校验）。
- **记忆策略、范围/分页快照与公共路由（`src/mem_ai/modules/memories/`）**：
  - `src/mem_ai/modules/memories/models.py`：`MemoryScopeState`（`memory_scopes` 表，`(user_id, provider_id)` 唯一、`epoch`、`barrier_state`）与 `MemoryListSnapshot`（`memory_list_snapshots` 表）。
  - `src/mem_ai/modules/memories/schemas.py`：`MemoryCreateRequest`（`extra="forbid"`、校验 `scope`/`conversationId` 绑定）、`MemoryItemResponse` 与判别联合 `MemoryCreateOutcome = CreatedMemoryOutcome | AcceptedMemoryOutcome`。
  - `src/mem_ai/modules/memories/snapshots.py`：`MemoryListScopeKey`、`sort_memory_records_desc`、`MemorySnapshotStore`（复用 `mem_ai.api.pagination` 实现跨副本 PostgreSQL 有界快照游标分页、用户/筛选条件绑定与 `422 cursor_expired` / `invalid_cursor` 校验）。
  - `src/mem_ai/modules/memories/service.py`：`ConversationAccessChecker` 协议与 `DefaultConversationAccessChecker`（待 05/08 接入真实会话归属检查）、`resolve_and_validate_scope`、`map_memory_provider_error`、`list_memories`、`create_explicit_memory`（Tx 1 锁 `memory_scopes` 行并校验用户状态与屏障 -> 事务外调用 `MemoryProvider` -> Tx 2 复查用户状态与 `memory_scopes` 代次/屏障再提交终态或转 `reconciling`）。
  - `src/mem_ai/modules/memories/router.py`：`GET /v1/memories`、`POST /v1/memories`（同步单条确认 201；异步受理或超时/屏障竞态待核验返回 202 + `Location` + `operationId`/`status`/`statusUrl`）与可覆盖依赖 `get_conversation_access_checker`。
- **MemOS Cloud 适配器与装配（`src/mem_ai/modules/memories/adapters/memos.py`、`src/mem_ai/core/config.py`、`src/mem_ai/application.py`）**：
  - `MemOSMemoryProvider`：实现 `MemoryProvider` 协议，使用服务端 `SecretStr` 凭据（`Authorization: Token <api_key>`），通过 `tags`（`memai:scope:*`、`memai:conv:*`、`memai:epoch:*`、`memai:write:*`）与 `info`（`biz_id`、`scene`、`business_type`、`custom_status`）编码范围与代次，区分发前连接/池失败（`failed`）与发后超时/504（`reconciling`），识别软失败 `data.success == false`，校验返回项 `user_id` 防止跨用户泄漏，并通过 `MemorySnapshotStore` 实现 `POST /get/memory` 稳定游标分页。
  - `src/mem_ai/core/config.py` 与 `.env.example`：新增 `MEMORY_PROVIDER_MODE`、`MEMOS_BASE_URL`、`MEMOS_API_KEY`、`MEMOS_REQUEST_TIMEOUT_SECONDS`、`MEMOS_LIST_PAGE_SIZE`、`MEMOS_LIST_MAX_ITEMS`、`MEMORY_LIST_SNAPSHOT_TTL_SECONDS`。
  - `src/mem_ai/core/logging.py`：补充精确键 `content` 与标记 `memory_value`、`memory_content`、`user_content` 脱敏，同时保留 `content_type`/`content_length` 等常规 HTTP 元数据键。
  - `src/mem_ai/db/model_registry.py` 与 `migrations/versions/20261121_0004_add_jobs_operations_and_memory_tables.py`：注册 ORM 模型并新增 `20261121_0004` 迁移（支持完整 `upgrade`、`alembic check` 与 `downgrade`）。

### 复用、扩展与职责分工

- 复用 01 的 `MemoryProvider` 契约、`ApiResponse`/`ok_response`、`PaginatedResponse`/`CursorPayload`/`encode_cursor`/`decode_cursor`/`hash_filters`、`AppError` 体系、`Base`/`UUIDPrimaryKeyMixin`/`CreatedAtMixin`/`TimestampMixin`/`is_unique_violation`、`ProviderRegistry` 与共享 `httpx.AsyncClient`。
- 复用 02 的 `AuthContextDep`、`users_service.get_active_user` / `ensure_user_active_in_session` 与 `test_auth_adapter`。
- 严格保持 `memories -> jobs.service`、`memories -> operations.service`、`memories -> users.service` 单向服务依赖，`jobs` 与 `operations` 保持通用无领域耦合，Router 零持久化/适配器依赖。

### 验证记录与剩余限制

- `.venv/bin/alembic upgrade head && .venv/bin/alembic check`：通过（升级至 `20261121_0004`，`No new upgrade operations detected.`）。
- `.venv/bin/pytest -q`：62 passed（覆盖 `tests/integration/test_memories_and_jobs_persistence.py` 的同步/异步/超时/屏障竞态/租约过期与跨副本快照、`tests/integration/test_memos_adapter_contract.py` 的受控 HTTP 上游与脱敏、`tests/api/test_memories_api.py` 的完整 HTTP 路由与端到端 MemOS 链路，以及 `test_database_and_migrations.py` 的空库升级/降级/漂移检测）。
- `.venv/bin/ruff check .` 与 `.venv/bin/ruff format --check .`：All checks passed，104 files formatted。
- `.venv/bin/mypy src`：Success: no issues found in 66 source files（strict 模式）。
- `python3 tools/check_architecture.py`：工程目录、功能索引、文档链接、分层导入、依赖图和已落地源码检查通过。
- **剩余限制与待补齐项**：
  1. **MemOS Cloud 线上真实接口能力实测（R1）**：因本地环境未配置 `MEMOS_API_KEY`，经用户确认本切片跳过线上实测，基于仓库内 MemOS Cloud API 文档与受控 `httpx.MockTransport` 完成适配器实现与契约测试；待配置受控测试用 `MEMOS_API_KEY` 后（最晚于 Ticket 18 生产验收前）需补跑真实线上探测以确认 `async_mode=false` 实际返回结构、`get/status` 粒度、`tags`/`info` 过滤及删除收敛延迟。
  2. **会话归属真实查询接线**：因 `conversations` 模块在 Ticket 05 落地，本切片通过可覆盖依赖 `get_conversation_access_checker` 暴露 `ConversationAccessChecker` 接缝（默认返回 `404 conversation_not_found`），05/08 落地后接入真实会话拥有者与 `deleting` 状态检查。

### 目录与文档同步

- 已按 `backend-design` 技能同步 `docs/backend-scaffold-design.md` 第 3 节当前目录状态、功能定位索引、目录树说明与职责表，同步 `.env.example` 与 `tests/api/test_health_and_foundation.py` 迁移头版本断言（`20261121_0004`）。
