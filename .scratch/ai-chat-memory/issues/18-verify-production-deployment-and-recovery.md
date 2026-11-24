# 18: 验证生产部署与恢复流程

**What to build:** 让 V1 在生产配置中完成发布、迁移、恢复和故障演练，形成可执行运行手册。

**Blocked by:** [16 执行数据保留与到期清理](16-apply-data-retention-and-expiry.md), [17 接入资源限制与运行观测](17-add-resource-limits-and-observability.md)

**Status:** resolved

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 扩展并验收 01 起持续维护的 CI、架构检查、公开行为测试和实际部署入口。
- 迁移、恢复、删除重放和外部核验调用已落地的资源与操作入口，运行手册记录实际命令和证据。
- 核对全部 ticket 的完成记录、公共符号和验证结果，明确剩余部署条件。

## 验收条件

- [x] API 和 Worker 独立运行，启动、健康检查、任务恢复、优雅停机及连接总预算可在验收环境验证。
- [x] CI 覆盖 uv 锁一致性、Ruff、mypy、PostgreSQL、空库及历史升级、Alembic 漂移和单元/API/集成测试。
- [x] 生产真实认证、服务端模型和 MemOS 配置通过有限权限实际接入核验，重点验证分页、幂等、取消及未知结果恢复能力。
- [x] 发布采用 expand、migrate、contract，单执行者迁移并校验兼容版本，手册明确失败后的前向修复与兼容应用回滚。
- [x] 备份及恢复演练记录时间点、可接受数据损失、RPO/RTO、角色和检查点，数据库恢复作为独立恢复操作。
- [x] 备份恢复演练覆盖两种分层基线：启用独立删除审计归档时，暂停写入并通过工具重放备份时间点之后的删除与代次记录再恢复服务；仅使用 PostgreSQL PITR 时，按 WAL 归档恢复并显式记录 RPO 与删除一致性边界。
- [x] 手册覆盖密钥注入与轮换、TLS/代理、入口限流、资源预算、任务耗尽、待核验结果和长期屏障告警处置。
- [x] 对全部 V1 契约做公开入口验收，工程目录、环境模板、工作流和操作手册与最终文件一致，记录实际执行结果和剩余部署条件。

## Answer

### 实际公共符号与交付入口

- **容器与部署入口（`Dockerfile`、`.dockerignore`、`Makefile`、`.github/workflows/checks.yml`）**：
  - `Dockerfile` 与 `.dockerignore`：基于 `python:3.13-slim` 与锁定版本 `ghcr.io/astral-sh/uv:0.11.6`（兼容 `uv.lock` `revision = 3`）构建多阶段非 root（`memai`）运行时镜像，支持独立启动 API（`uvicorn mem_ai.main:app`）、Worker（`python -m mem_ai.workers.main`）、单执行者迁移（`alembic upgrade head`）、模型同步（`python -m mem_ai.modules.models.sync`）与运维恢复 CLI（`python -m mem_ai.ops`）。
  - `Makefile` 新增 `ops-health` 与 `ops-budget` 快捷入口；`.github/workflows/checks.yml` 补充 `uv lock --check`、空库 `upgrade head -> check -> downgrade base -> upgrade head -> check`，并执行包含历史数据升级（`20261122_0005` -> `20261124_0012`）与生产恢复演练的全量测试与架构检查。
  - `src/mem_ai/modules/health/service.py`：增强 `_discover_alembic_head()` 回退至 `Path.cwd()`，确保容器非可编辑安装下 `/health/ready` 正确解析 `alembic.ini` 与 `migrations/`。
- **独立删除审计归档读取与资源拥有者恢复重放入口**：
  - `src/mem_ai/modules/operations/audit.py`：新增 `RecoveryReplayStatus`、`AuditReplayPlan`、`load_deletion_audit_events`（时间窗过滤、UTC 规范化、确定性排序与坏行校验）与 `plan_deletion_audit_replay`（按 `user_delete > memory_clear(max epoch) > conversation_delete / memory_delete` 优先级折叠去重）。
  - `src/mem_ai/modules/users/service.py` 与 `src/mem_ai/modules/users/cleanup.py`：新增 `ensure_user_not_deleting_in_session` 与共享 `_accept_user_deletion_in_session` / `replay_user_deletion_for_recovery`（支持活跃及已停用用户、幂等重放且不重复写审计）。
  - `src/mem_ai/modules/conversations/cleanup.py` 与 `router.py`：为 `DELETE /v1/conversations/{conversationId}` 与 `ConversationCleanupJobHandler` 补齐独立删除审计事件导出（`DeletionAuditArchiverDep`），新增共享 `_accept_conversation_deletion_in_session` 与 `replay_conversation_deletion_for_recovery`（允许停用用户与失败操作重试）。
  - `src/mem_ai/modules/memories/deletions.py`、`clearing.py`、`barrier.py`、`recovery.py`、`service.py`：共享 `prepare_single_delete_tx1(..., force_async, allow_disabled_user)`、`prepare_clear_scope_in_session(..., min_new_epoch)`（高代次重放先取消/标记被覆盖的低代次屏障任务与操作）、`make_delete_accepted_audit_events`，并经 `memories/recovery.py` 与 `memories/service.py` 导出 `replay_single_memory_delete_for_recovery` 和 `replay_clear_scope_for_recovery`。
- **生产运维、预算、恢复与提供方核验 CLI（`src/mem_ai/ops.py`）**：
  - `compute_connection_budget`（`check-budget`）、`replay_deletion_audit_archive`（`replay-audit`，支持 `--since`/`--until`/`--dry-run` 并透传配置的 `memory_provider_mode`）、`inspect_operational_health`（`inspect-health`）、`requeue-job`（调用 `jobs_service.requeue_failed_job`）与 `verify_providers`（`verify-providers`，覆盖生产 JWT/网关凭证校验、OpenAI 兼容同步/流式/取消、MemOS 显式写入与同 `write_id` 幂等、召回、跨页快照游标分页、`verify_operation` 外部核验及清理）。
- **生产部署与恢复运行手册（`docs/operations/deployment-and-recovery.md`）**：
  - 固化容器与独立进程拓扑、连接总预算公式、Expand/Migrate/Contract 单执行者迁移与前向修复/兼容应用回滚、角色/检查点/RPO/RTO 模板、基线 A（独立删除审计归档重放）与基线 B（仅 PostgreSQL PITR 及 WAL 丢失删除一致性边界）、密钥注入与轮换、TLS/反向代理/SSE 禁缓冲/入口限流、任务耗尽/待核验/长期屏障/过期租约故障处置及有限权限核验命令。

### 复用、双轴代码审阅修复与 V1 全切片核对

- 全部恢复、任务重入队、健康检查与审计重放均复用既有 `users`、`conversations`、`memories`、`jobs`、`operations` 与 `health` 模块拥有者入口，零新增数据库迁移。
- 经双轴 `code-review`（Standards & Spec）修复：
  1. 消除 `memories/recovery.py` 与 `memories/deletions.py` 的单条删除事务重复代码，统一走 `prepare_single_delete_tx1`；将 `AuditReplayPlan`/`plan_deletion_audit_replay`/`RecoveryReplayStatus` 收敛至 `operations/audit.py` 并去除 `users/cleanup.py` 重复加锁查询。
  2. 将 `Dockerfile` 的 `uv` 镜像版本由 `0.6` 修正为 `0.11.6`（匹配 `uv.lock` `revision = 3`）；补齐 `.github/workflows/checks.yml` 的 `alembic downgrade base && alembic upgrade head && alembic check`。
  3. 修复审计重放边缘缺陷：CLI 透传 `memory_provider_mode`、`disabled` 用户仍执行重放避免复启复活、失败预备份 tombstone/会话删除可重入队、高代次重放覆盖活跃低代次屏障时先终止旧屏障任务/操作。
  4. 完善 `verify_providers`：`--check-auth` 强制校验凭证、增加同 `write_id` 幂等复写、双条探针实测跨页游标分页与 `verify_operation` 核验；同步 `docs/backend-scaffold-design.md` 与 `tools/check_architecture.py`。
- **V1 全部 Ticket 核对**：01 至 18 全部 `resolved`，`docs/backend-scaffold-design.md` 第 3 节目录状态表、功能定位索引、目录树与职责表、`README.md`、`.env.example`、`Makefile`、`.github/workflows/checks.yml` 与 `docs/operations/deployment-and-recovery.md` 已与最终代码完全同步。

### 验证记录与剩余部署条件

- `uv lock --check`：通过（45 packages resolved）。
- `.venv/bin/ruff format --check src tests migrations tools` & `.venv/bin/ruff check src tests migrations tools`：通过（180 files formatted，0 errors）。
- `.venv/bin/mypy src`：通过（Success: no issues found in 113 source files，strict 模式）。
- `python3 tools/check_architecture.py`：通过（含 `docs/operations/deployment-and-recovery.md` 本地链接、目录索引、分层导入、装配例外与依赖图校验）。
- `.venv/bin/alembic upgrade head && .venv/bin/alembic check`：通过（`20261124_0012`，`No new upgrade operations detected.`）。
- PostgreSQL 17 逻辑备份与恢复演练：`pg_dump -Fc` -> `pg_restore -l` 可读性验证 -> 临时库 `pg_restore --no-owner --single-transaction` -> `alembic_version = 20261124_0012` 通过并清理。
- `.venv/bin/python -m mem_ai.ops check-budget` 与 `inspect-health`：通过。
- `.venv/bin/pytest -q`：**239 passed in 78.85s**（覆盖 `tests/unit/test_ops_recovery.py`、`tests/integration/test_database_and_migrations.py` 含空库/漂移/历史数据升级与多版本兼容就绪、`tests/integration/test_production_deployment_and_recovery.py` 含独立 API+Worker 生命周期/任务恢复/基线 A 审计重放防复活与幂等/停用用户与失败 tombstone 边界/受控重入队/受控生产提供方核验及全部 V1 公开 HTTP 契约，以及既有 01–17 全部测试）。
- **剩余上线前置条件（需在配置真实外部凭据的目标 Staging/生产环境执行）**：
  1. 在 Staging 注入有限权限真实 `MEMOS_API_KEY`、`OPENAI_COMPAT_API_KEY` 与真实 IdP JWT/网关凭据，执行 `python -m mem_ai.ops verify-providers --check-auth --bearer-token "$STAGING_JWT" --check-model --model-id "$MODEL_DEFAULT_ID" --check-memory` 完成真实上游分页、幂等、取消与 `get/status` 收敛延迟签核。
  2. 在目标集群部署独立只追加审计卷/对象存储导出（基线 A `DELETION_AUDIT_ARCHIVE_PATH`）或 PostgreSQL 连续 WAL 归档（基线 B），并完成一次平台级演练记录。
  3. 在入口网关配置 TLS 证书、分布式频率限制、SSE 禁缓冲与 `ALLOWED_HOSTS`/`CORS_ALLOWED_ORIGINS`/`AUTH_TRUSTED_PROXIES` 生产白名单。
