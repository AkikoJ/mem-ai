# Mem-AI 生产部署、迁移与恢复运行手册

本手册面向 Mem-AI V2 生产与验收环境，覆盖容器构建、统一单进程后端运行、连接预算校验、Expand/Migrate/Contract 数据库发布、两种分层备份恢复基线（独立删除审计归档重放与 PostgreSQL PITR）、密钥注入与轮换、TLS/反向代理与入口限流配置、故障处置，以及有限权限提供方接入核验。

相关工程契约见 [后端工程设计](../backend-scaffold-design.md)、[总体设计](../../DESIGN.md)、[ADR-0007](../adr/0007-unified-single-process-backend.md) 与 [ADR-0005](../adr/0005-memory-epochs-and-deletion-barriers.md)。

---

## 1. 部署拓扑与统一单进程运行

Mem-AI 后端服务采用**统一单进程架构**，将 HTTP/SSE 请求处理与持久后台任务执行内嵌到同一 Python 进程中，使用同一多阶段容器镜像（[`Dockerfile`](../../Dockerfile)、[`.dockerignore`](../../.dockerignore)），以非 root 用户 `memai` 运行，依赖由 [`uv.lock`](../../uv.lock) 锁定。横向扩展时部署多个同构单进程副本，通过 PostgreSQL 行锁跳过领取（`FOR UPDATE SKIP LOCKED`）与周期窗口去重安全并发消费后台任务。

### 1.1 镜像构建与验证

```bash
docker build -t mem-ai:v2 .
```

### 1.2 进程启动入口

| 角色 | 启动命令 | 暴露端口与探针 |
| --- | --- | --- |
| 统一后端服务 | `uvicorn mem_ai.main:app --host 0.0.0.0 --port 8000` | `GET /health/live`、`GET /health/ready`、受控 `GET /metrics`；可选原子心跳文件 `WORKER_HEARTBEAT_FILE` |
| 单执行者迁移 | `alembic upgrade head && alembic check` | 发布前单实例 Job 执行 |
| 模型配置同步 | `python -m mem_ai.modules.models.sync` | 启动自动同步（`MODEL_SYNC_ON_STARTUP=true`）或发布 Job 显式幂等同步 |
| 运维与恢复 CLI | `python -m mem_ai.ops <subcommand>` | `check-budget` / `inspect-health` / `replay-audit` / `requeue-job` / `verify-providers` |

### 1.3 优雅停机与恢复语义

统一后端服务收到 `SIGTERM` 后按序执行以下步骤：

1. 将 `app.state.ready` 置为 `false`（`/health/ready` 返回 `503` 通知负载均衡摘流）。
2. 停止内嵌后台任务执行器领取新任务。
3. 在 `WORKER_SHUTDOWN_GRACE_SECONDS`（默认 15s）宽限期内，并行排空在途 HTTP 请求、已进入模型调用的同步生成任务（`SyncGenerationTaskRegistry`）及在途后台任务处理器。
4. 未能在宽限期内完成的后台任务保留可恢复 `running` 租约状态，随租约到期由其他健康副本接管恢复，并自动触发 `generation_lease_recovery` 周期任务收敛崩溃遗留的过期生成。
5. 关闭共享 HTTP 客户端与数据库连接池。

### 1.4 PostgreSQL 连接总预算校验

每个统一后端服务副本的最大连接数为 `DB_POOL_SIZE + DB_MAX_OVERFLOW`。总连接预算公式为：

$$\text{total\_required} = \text{replicas} \times (\text{DB\_POOL\_SIZE} + \text{DB\_MAX\_OVERFLOW}) + \text{reserved\_connections}$$

其中 `reserved_connections` 至少预留 10 个连接给 Alembic 迁移、备份工具和运维诊断；当内嵌后台任务执行器启用（`WORKER_LOOP_ENABLED=true`，默认）时，单进程池上限须满足 `DB_POOL_SIZE + DB_MAX_OVERFLOW >= MAX_CONCURRENT_REQUESTS + MAX_CONCURRENT_GENERATIONS + WORKER_CONCURRENCY + 1`。

部署或扩缩前执行强制校验（超预算或池不足时退出码为 `1`）：

```bash
uv run python -m mem_ai.ops check-budget \
  --replicas 4 \
  --reserved-connections 10 \
  --postgres-max-connections 100
```

---

## 2. 发布流程：Expand / Migrate / Contract

所有生产数据库变更必须遵循 **Expand -> Migrate -> Contract** 三阶段模式，禁止在同一发布中直接执行破坏旧版运行的列删除、重命名或非空无默认约束收紧。

### 2.1 单执行者迁移与兼容版本校验

1. **备份前置**：在任何生产迁移前完成一次主库一致性备份并验证可读性（见第 3 节）。
2. **Expand 阶段**：仅添加新表、可空列、新索引或兼容视图。由**单个发布执行者**运行：
   ```bash
   uv run alembic upgrade head
   uv run alembic check
   uv run python -m mem_ai.modules.models.sync
   ```
3. **滚动升级兼容**：
   - 在新旧版本共存窗口内，为正在运行的旧版服务实例设置 `COMPATIBLE_ALEMBIC_REVISIONS`（如 `["20261123_0011","20261124_0012"]`），确保 `/health/ready` 在迁移升级完成后仍保持就绪。
   - 滚动发布新版服务实例，等待 `/health/ready` 返回 `200` 且 `migration_revision` 匹配目标版本。
4. **Migrate / Backfill 阶段**：通过可恢复的分批后台任务或幂等脚本完成数据回填，核对数据完整性。
5. **Contract 阶段**：待全部旧副本下线并稳定观察后，在后续独立发布中移除已废弃旧列或旧约束。

### 2.2 发布失败处置：前向修复与兼容应用回滚

- **优先前向修复（Forward-fix）**：若迁移已提交但新版存在缺陷，优先发布修复补丁或通过新增前向迁移修正，避免对生产库执行破坏性 `alembic downgrade`。
- **兼容应用回滚**：因为 Expand 阶段对旧版保持结构兼容，可直接将服务镜像回滚至上一稳定版本（配置 `COMPATIBLE_ALEMBIC_REVISIONS` 包含当前数据库 head 版本），验证 `/health/ready` 与关键业务接口恢复正常。
- **数据库恢复为独立操作**：仅在发生严重数据损坏或不可逆误操作时触发第 3 节的独立数据库恢复流程，绝不将数据库回档混入普通应用回滚动作。

---

## 3. 备份与恢复演练（分层一致性基线）

为满足“恢复备份后不复活已删除记忆与已删除主体”的约束（[ADR-0005](../adr/0005-memory-epochs-and-deletion-barriers.md)），所有数据库恢复演练与真实恢复操作必须按统一角色、检查点和 RPO/RTO 模板执行并留存记录。

### 3.1 角色、检查点与恢复记录模板

| 角色 | 职责 |
| --- | --- |
| 事故指挥（IC） | 批准恢复目标时间点 $T_0$、评估可接受数据损失与窗口、签发停写与恢复放行 |
| DBA / 发布执行者 | 执行备份校验、主库或目标库物理/逻辑恢复、验证 `alembic_version` 与数据完整性 |
| 应用运维 | 暂停入口写入与缩容服务副本至 0、执行删除审计重放、重启服务副本收敛后恢复流量 |
| 验收人 | 抽检恢复后健康状态、屏障/任务收敛、删除主体隔离与 V1 核心读写回归 |

每次演练或恢复必须填写下表并归档：

| 记录项 | 填写内容（示例/基线目标） |
| --- | --- |
| 恢复基线类型 | 基线 A（独立删除审计归档） / 基线 B（仅 PostgreSQL PITR） |
| 目标恢复时间点 $T_0$（UTC） | 如 `2026-11-24T07:25:52Z` |
| 可接受数据损失说明 | $T_0$ 之后的未归档会话与消息损失；删除意图按对应基线处理 |
| 目标 RPO / 实测 RPO | 基线 A：数据同快照/WAL，删除与代次 RPO = 0（归档实时 `fsync`）；基线 B：取决于 WAL 归档频率 |
| 目标 RTO / 实测 RTO | 如目标 <= 30 分钟，记录实际耗时 |
| 检查点 1（备份可读性） | `pg_restore -l` 或 `pg_verifybackup` 通过 |
| 检查点 2（写入与任务冻结） | 入口切维护/拒绝写流量，所有服务副本数降为 0 |
| 检查点 3（恢复与重放结果） | `alembic_version` 核对、`replay-audit` 干跑与执行 JSON 输出留存 |
| 检查点 4（恢复后验证） | `inspect-health` 无异常告警、服务副本收敛全部重放任务、`/health/ready` 200 |

### 3.2 迁移前备份与可读性校验

逻辑一致性备份（适用于中小库及迁移前快照）与可读性检查命令：

```bash
# 1. 创建自定义格式一致性备份
pg_dump -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -Fc -f "/backups/mem_ai_$(date -u +%Y%m%dT%H%M%SZ).dump"

# 2. 强制校验备份目录与可读性（不依赖主观猜测）
pg_restore -l /backups/mem_ai_<timestamp>.dump >/dev/null

# 3. 在隔离演练库验证可恢复性及迁移版本
psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE mem_ai_restore_drill;"
pg_restore -U "$POSTGRES_USER" -d mem_ai_restore_drill --no-owner --single-transaction /backups/mem_ai_<timestamp>.dump
psql -U "$POSTGRES_USER" -d mem_ai_restore_drill -tAc "SELECT version_num FROM public.alembic_version;"
psql -U "$POSTGRES_USER" -d postgres -c "DROP DATABASE mem_ai_restore_drill WITH (FORCE);"
```

> 生产大库应同时开启 PostgreSQL 物理基础备份（`pg_basebackup`）与连续 WAL 归档，并定期通过 `pg_verifybackup` 及隔离恢复库执行恢复演练。

### 3.3 基线 A（推荐生产配置）：启用独立删除审计归档

生产环境应配置 `DELETION_AUDIT_ARCHIVE_PATH`（挂载独立于主库备份周期的只追加审计卷或同步至对象存储只追加 JSONL，保留期覆盖最长备份恢复窗口）。全部 `user_delete`、`conversation_delete`、`memory_clear`（含 `memory_epoch_advanced`）与 `memory_delete`（含 `memory_tombstone_created`）均实时脱敏并 `fsync` 写入该归档。

**标准恢复步骤：**

1. **暂停写入与缩容服务副本**：在入口网关停止业务写入流量，将服务副本数缩至 `0`（防止旧任务抢先执行导致记忆复活）。
2. **恢复主库备份至时间点 $T_0$**：使用经过校验的备份快照将 PostgreSQL 恢复到 $T_0$，核对 `alembic_version`。
3. **干跑预览审计重放计划**：
   ```bash
   uv run python -m mem_ai.ops replay-audit \
     --archive-path "$DELETION_AUDIT_ARCHIVE_PATH" \
     --since "2026-11-24T07:25:52Z" \
     --dry-run
   ```
   核对输出中的 `user_deletes`、`conversation_deletes`、`memory_clears` 与 `memory_deletes` 清单。
4. **执行删除与代次重放（幂等）**：
   ```bash
   uv run python -m mem_ai.ops replay-audit \
     --archive-path "$DELETION_AUDIT_ARCHIVE_PATH" \
     --since "2026-11-24T07:25:52Z"
   ```
   工具仅调用既有资源拥有者入口执行本地原子隔离（无外发 HTTP 副作用）：
   - `user_delete`：立即将用户置为 `deleting`（阻止旧凭证访问）并入队 `user_delete` 任务；
   - `conversation_delete`：立即将存活会话置为 `deleting` 并入队 `conversation_delete` 任务；
   - `memory_clear` / `memory_epoch_advanced`：提升 `memory_scopes.epoch` 至归档代次、安装 `clearing` 屏障、**立即取消恢复库中所有旧代次排队记忆写入任务**并入队 `memory_clear` 远程清空任务；
   - `memory_delete`：立即写入 `memory_tombstones`（即刻拦截召回）、标记历史引用并入队 `memory_delete` 任务。
5. **先启动服务副本收敛外部副作用与清理任务**：启动服务副本（内嵌后台任务执行器自动运行），通过 `inspect-health` 观察所有重放删除任务收敛且 `barriers_clearing == 0`、`jobs_failed == 0`：
   ```bash
   uv run python -m mem_ai.ops inspect-health
   ```
6. **恢复 API 流量与回归验收**：启动 API 副本，确认 `/health/ready` 返回 `200`，抽检被删除用户返回 `403`、已清空用户无历史记忆复活。

### 3.4 基线 B（最小部署基线）：仅使用 PostgreSQL PITR

当部署未启用外部独立删除审计归档时，仅以 PostgreSQL 基础备份 + WAL 归档的时间点恢复（PITR）作为恢复手段：

1. 暂停入口写入与将所有服务副本数缩至 0。
2. 恢复最近的 `pg_basebackup` 基础备份，在 `postgresql.auto.conf` 配置 `restore_command` 与 `recovery_target_time = '<T0>'`（及 `recovery_target_action = 'promote'`），创建 `recovery.signal` 启动 PostgreSQL 完成 WAL 重放。
3. 核对 `alembic_version` 与关键表完整性后，先启动服务副本核验 `reconciling` / 过期租约任务收敛，再恢复入口流量。
4. **显式一致性边界与风险声明**：
   - 若 WAL 归档连续且完整，RPO 接近最后一个已归档 WAL 段（通常数秒至数分钟）。
   - **若 WAL 归档发生丢失或被迫回档至早于某次删除受理的 $T_0$**：主库内部表（`operations`、`memory_scopes`、`memory_tombstones`）会一并回退，**系统无法自动获知 $T_0$ 之后的删除与代次递增记录**，恢复库中遗留的旧代次排队写入任务存在再次写入外部记忆提供方的风险。此时必须在启动服务副本前由运维手动核查外部工单/审计日志，通过 SQL 取消可疑旧写入任务并手动重新触发对应用户的清空/删除操作，或升级启用基线 A 的独立删除审计归档。

---

## 4. 密钥注入、轮换与安全边界

### 4.1 密钥注入与零落盘原则

- 严禁将任何真实密钥写入镜像、Git 仓库或 `.env.example`。所有敏感配置（`POSTGRES_PASSWORD`、`AUTH_JWT_HS256_SECRET`、`AUTH_GATEWAY_SHARED_SECRET`、`DELETION_RECEIPT_SECRET`、`METRICS_BEARER_TOKEN`、`OPENAI_COMPAT_API_KEY`、`MEMOS_API_KEY`）通过平台 Secret Manager 或运行时只读环境变量/文件注入。
- 生产环境（`APP_ENV=production`）启动强校验会自动拒绝：`AUTH_MODE=development`、通配 `ALLOWED_HOSTS=["*"]`、公网开放 `METRICS_ALLOWED_NETWORKS`（如 `0.0.0.0/0` 或 `::/0`），以及短于 32 字符的签名/回执密钥。

### 4.2 密钥轮换步骤与影响范围

| 密钥 | 轮换步骤 | 影响与注意事项 |
| --- | --- | --- |
| `OPENAI_COMPAT_API_KEY` / `MEMOS_API_KEY` | 上游签发新 Key -> 更新 Secret -> 滚动重启服务副本 -> 观察请求正常后吊销旧 Key | 零停机；旧 Key 在全部副本滚动完成后吊销 |
| `METRICS_BEARER_TOKEN` | 先向采集端配置新 Token 或短暂双配置 -> 更新应用 Secret 滚动重启 -> 确认抓取成功 | 仅影响内部监控抓取出入，不影响业务流量 |
| `POSTGRES_PASSWORD` | 推荐新建过渡 DB 角色或分阶段切密 -> 更新 Secret 滚动重启服务副本 -> 清理旧凭据 | 滚动期间保留连接预算余量 |
| `AUTH_JWT_HS256_SECRET` / `AUTH_GATEWAY_SHARED_SECRET` | 与上游 IdP/网关协同窗口切换；若需零中断，建议在网关层或 IdP 侧做短暂双签过渡 | 切换瞬间未更新的旧令牌将返回 `401` |
| `DELETION_RECEIPT_SECRET` | 仅在密钥泄漏或定期计划窗口轮换；保持密钥在 `DELETION_RECEIPT_TTL_SECONDS`（默认 30 天）内稳定 | **注意**：轮换会使所有未到期的既有用户删除回执立即失效；若仅需作废单个回执，应走单条回执撤销时间（`deletion_receipt_revoked_at`）而非全局轮换 |

### 4.3 TLS、反向代理与入口限流基线

- **TLS 与反向代理**：生产入口必须启用 TLS 终止，配置显式 `ALLOWED_HOSTS`、`CORS_ALLOWED_ORIGINS`、`AUTH_TRUSTED_PROXIES` 与 `TRUST_REQUEST_ID_FROM_PROXIES_ONLY=true`。网关必须覆盖或剔除客户端伪造的身份头（`X-Gateway-Auth`、`X-Auth-*`、`X-Forwarded-For`）。
- **SSE 与超时协调**：
  - 反向代理对 `/v1/chat` 关闭缓冲（Nginx：`proxy_buffering off; proxy_cache off; chunked_transfer_encoding on;`），并设置 `proxy_read_timeout >= PROXY_READ_TIMEOUT_SECONDS`（且须满足 `CHAT_STREAM_HEARTBEAT_INTERVAL_SECONDS < PROXY_READ_TIMEOUT_SECONDS`，应用启动时已强校验）。
- **两层限流与进程资源预算**：
  - **入口网关**：承担跨副本分布式频率限制（按用户主体 / IP / 路由限流）。
  - **应用进程**：通过可替换 `RateLimiter` 接缝、`MAX_REQUEST_BODY_BYTES`（`413 payload_too_large`）、`MAX_CONCURRENT_REQUESTS`、`MAX_CONCURRENT_GENERATIONS`（`429` + `Retry-After`）、`CHAT_MESSAGE_MAX_CHARS`、`CHAT_GENERATION_TIMEOUT_SECONDS`、`CHAT_STREAM_TIMEOUT_SECONDS` 与 `WORKER_CONCURRENCY` 保护单实例资源。

---

## 5. 故障与告警处置手册

日常巡检及故障定位首选两个受控入口：受保护的 `GET /metrics`（白名单网络 + `METRICS_BEARER_TOKEN`）与运维诊断命令：

```bash
uv run python -m mem_ai.ops inspect-health
```

### 5.1 后台任务重试耗尽（`mem_ai_jobs_failed > 0` / `exhausted_failed_jobs`）

1. 查询失败任务与最近执行摘要（禁止记录敏感正文）：
   ```sql
   SELECT id, job_type, business_id, attempt_count, max_attempts, external_ref, last_error_code, last_error_summary, updated_at
   FROM jobs WHERE status = 'failed' ORDER BY updated_at DESC LIMIT 20;
   ```
2. 结合 `job_attempts` 与外部提供方状态确认故障根因已修复（如密钥恢复、上游限流解除、网络恢复）。
3. 使用受控运维入口重新入队（记录操作原因，增加有限重试次数，不直接手改数据库状态）：
   ```bash
   uv run python -m mem_ai.ops requeue-job \
     --job-id "<job-uuid>" \
     --extra-attempts 3 \
     --reason "upstream provider recovered after incident INC-xxxx"
   ```

### 5.2 外部结果待核验（`mem_ai_jobs_reconciling > 0` / `reconciling_jobs_pending`）

1. 检查 `jobs`（`status = 'reconciling'`）中的 `external_ref`、`metadata_json`（包含 `write_id`/`biz_id`/`memory_epoch`）与 `next_run_at`。
2. 确认是否为普通超时待自动轮询（Worker 会按 `WORKER_RECONCILE_DELAY_SECONDS` 自动调用 `verify_operation` 收敛）或因清空超上限窗口保留的审计记录（`clear_settle_timeout_alerted = true`）。
3. 若长期停滞且上游提供方不支持精确单次状态查询，先核实上游是否已实际落库或已被后续清空覆盖，再按需通过上游控制台核验并决定是否触发受控清理或人工确认。

### 5.3 长期记忆删除屏障告警（`mem_ai_memory_barriers_active > 0` / `mem_ai_memory_barrier_oldest_duration_seconds` 持续升高）

1. 查询处于 `clearing` 或 `reconciling` 的范围及关联操作：
   ```sql
   SELECT ms.user_id, ms.provider_id, ms.epoch, ms.barrier_state, ms.barrier_operation_id, op.status, op.progress, op.error_code
   FROM memory_scopes ms
   LEFT JOIN operations op ON op.id = ms.barrier_operation_id
   WHERE ms.barrier_state <> 'idle';
   ```
2. 检查对应 `memory_clear` 或 `user_delete` 任务状态：
   - 若正在等待旧代次在途写入收敛，确认未超过 `MEMORY_INFLIGHT_SETTLE_TIMEOUT` + `MEMORY_CLEAR_COMPENSATION_DELAY_SECONDS`；
   - 若因记忆提供方故障导致最终清空失败或 `failed` 耗尽，修复提供方连通性后用 `requeue-job` 恢复对应的 `memory_clear` / `user_delete` 任务；
   - **严禁直接手工把 `memory_scopes.barrier_state` 改回 `idle`**，除非已人工确认全部远程记忆清空完成，否则会破坏删除屏障语义。

### 5.4 过期生成租约与记忆降级告警

- **`mem_ai_generations_expired_leases > 0`**：确认独立 Worker 进程存活（检查 `WORKER_HEARTBEAT_FILE` 与 Worker `/health/live`），`generation_lease_recovery` 会自动收敛；同时新到达同会话的请求亦会内联接管过期租约。
- **`mem_ai_memory_provider_degraded == 1`**：说明普通记忆召回发生上游故障并已降级为空记忆，普通对话仍可完成且 `/health/ready` 保持 `200`；排查 `MEMOS_BASE_URL`、网络与限额。

---

## 6. 有限权限真实提供方核验与 V1 验收清单

在切换真实生产流量前，使用有限权限的 staging/验收凭据执行真实接入核验（不输出密钥或令牌）：

```bash
# 1. 验证生产认证、服务端模型（同步/流式/取消）与 MemOS（写入幂等/召回/快照游标分页/单条删除/外部核验）
uv run python -m mem_ai.ops verify-providers \
  --check-auth --bearer-token "$STAGING_JWT" \
  --check-model --model-id "$MODEL_DEFAULT_ID" \
  --check-memory

# 2. 验证本地/验收全量静态、迁移、测试与架构检查
uv lock --check
uv sync --locked --dev
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run alembic upgrade head
uv run alembic check
uv run pytest
python3 tools/check_architecture.py
```

### 本地演练实测记录（2026-11-24）

- **CI 与静态检查**：`uv lock --check`、`ruff check`、`ruff format --check`、`mypy src`（strict）、`tools/check_architecture.py` 全部通过。
- **PostgreSQL 17 迁移与历史升级**：空库 `upgrade head` / `downgrade base` / `upgrade head` / 元数据漂移检测、含历史数据（`20261122_0005` -> `20261124_0012`）无损升级与多版本兼容就绪校验均通过实测。
- **PostgreSQL 17 备份与恢复演练**：实测 `pg_dump -Fc` -> `pg_restore -l` 可读性验证 -> 隔离库 `pg_restore --single-transaction` -> `alembic_version = 20261124_0012` 通过；`tests/integration/test_production_deployment_and_recovery.py` 完整演练了统一单进程生命周期、心跳与任务恢复、基线 A 独立删除审计归档干跑/重放/幂等/防旧任务复活、耗尽任务受控重入队、受控提供方核验及全部 V2 公开 HTTP 契约。
- **剩余上线前置条件（需在拥有真实外部凭据的 Staging 环境执行）**：
  1. 注入受控测试用 `MEMOS_API_KEY`、`OPENAI_COMPAT_API_KEY` 与真实 IdP JWT/网关凭据，执行一次 `python -m mem_ai.ops verify-providers --check-auth --check-model --check-memory` 验证真实上游分页、幂等、取消与 `get/status` 收敛延迟。
  2. 在目标集群配置外部对象存储/独立审计卷挂载 `DELETION_AUDIT_ARCHIVE_PATH`（基线 A）或配置 PostgreSQL WAL 连续归档（基线 B）并完成一次平台级演练签核。
  3. 在入口网关落盘 TLS 证书、分布式频率限制规则、SSE 禁缓冲与 `ALLOWED_HOSTS`/`AUTH_TRUSTED_PROXIES` 生产白名单。
