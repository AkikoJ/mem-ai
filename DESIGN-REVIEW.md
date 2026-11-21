# Mem-AI 设计评审记录

评审日期：2026-11-21

评审对象：[总体设计](DESIGN.md)、[后端脚手架工程设计](docs/backend-scaffold-design.md)、[领域术语](GLOSSARY.md)、[已接受 ADR](docs/adr/)、[实施规格](.scratch/ai-chat-memory/spec.md)、全部 18 份 ticket，以及仓库内 [MemOS Cloud 文档](.agents/skills/memos-cloud/resources/cn/index.md) 和 [tools/check_architecture.py](tools/check_architecture.py)。

本文只记录会实际影响实现、验收或生产运维的问题，不改变已接受的核心架构决策。问题收敛后在对应条目记录结论，或同步回 spec、ticket、设计文档及 ADR。

## 1. 总体结论

主干架构与一致性设计扎实，以下决策成立且无需调整：

- 模块化单体配合 `ModelProvider`、`MemoryProvider`、`PersonaProvider` 接缝，首版推迟 Cordis。
- 使用 PostgreSQL 统一 `jobs` 表与独立 Worker 承载异步工作，首版不引入额外消息队列或 Redis。
- 外部 HTTP 调用放在数据库事务之外，状态转换使用短事务。
- 每次输入对应独立生成记录；用户消息先提交，完整回复、生成成功与普通记忆任务原子提交；失败与取消回合保留历史并从后续模型上下文过滤。
- 聊天幂等边界完整，`stream` 排除出业务摘要，`running`/`succeeded`/`failed`/`cancelled` 的重试语义明确。
- 召回引用仅保存元数据，记忆正文由提供方管理，隐私与一致性边界合理。

需要前置收敛的主要风险集中在：**MemOS Cloud 实际能力与理想化记忆契约的差距**、**备份恢复重放删除记录的持久化载体缺失**、**部分 ticket 依赖漏边**以及**几处并发与删除状态机留白**。

## 2. 问题总览

> 2026-11-21 处置更新：全部 14 项问题均已将设计结论沉淀到对应设计文档、ADR、spec、ticket 与架构检查工具中（见下表“状态与落点”）；仅 R1 保留一项“在 ticket 04 动工前对真实 MemOS Cloud 环境做一次实测验证”的前置执行动作。

### 设计与契约问题

| 编号 | 级别 | 问题摘要 | 影响切片 | 状态与落点 |
| --- | --- | --- | --- | --- |
| R1 | 高 | MemOS Cloud 实际能力与 `MemoryProvider` 契约存在结构性脱节 | 04、08、09、13、14、15、18 | 设计已收敛（待 04 前实测确认）：`DESIGN.md`、`docs/backend-scaffold-design.md` §6.2/§8.2/§11.2/§11.3、ADR 0005、spec、ticket 04/13/14/16 |
| R2 | 高 | 普通对话写入的记忆范围归属（`user` vs `conversation`）缺少规则 | 08、09、13、14 | 已解决：`memoryPolicy.writeScope` 控制普通回合范围（默认 `user`），显式创建默认 `user` 并支持 `conversation`，已同步至 `DESIGN.md`、`docs/backend-scaffold-design.md` §2.2、spec、ticket 03/04/08/09 |
| R3 | 高 | “备份恢复后重放删除记录”缺少独立于主库之外的持久化载体 | 13、14、15、18 | 已解决：确立“独立删除审计归档接缝 / PostgreSQL PITR 基线”分层恢复机制，已同步至 `DESIGN.md`、`docs/backend-scaffold-design.md` §2.2/§10/§12.4、ADR 0005、spec、ticket 13/15/18 |
| R4 | 中 | Ticket 依赖图存在 3 处客观的依赖缺失（`14`/`15` 缺 `10`，`17` 缺 `13`） | 10、13、14、15、17 | 已解决：已补齐 `10 -> 14` 与 `13 -> 17`，同步至 `spec.md` 与 ticket 14/17 |
| R5 | 中 | Agent、人设草稿与版本的关系存在歧义，且默认模型停用对旧会话的影响未定义 | 03、05、06、11 | 已解决：草稿唯一存 `agents` 行、`persona_versions` 仅存不可变快照、三接口分工明确、停用默认模型返回 `model_unavailable`（允许策略许可下显式指定模型），已同步至 `DESIGN.md`、`docs/backend-scaffold-design.md` §6.2/§8.1、spec、ticket 03/06 |
| R6 | 中 | `PersonaProvider` 读写职责混装，且短事务内读人设与未来远程拆分冲突 | 03、05、11 | 已解决：`PersonaProvider` 收缩为只读契约、对话准备在事务外读取人设快照，已同步至 `DESIGN.md`、`docs/backend-scaffold-design.md` §2.2/§3/§5.2、spec、ticket 03/05 |
| R7 | 中 | 过期生成租约的接管依赖 Worker 存活，Worker 故障时会话持续 409 | 07、10 | 已解决：新请求领取租约时同事务直接收敛并接管已过期租约，Worker 保留为兜底，已同步至 `DESIGN.md`、`docs/backend-scaffold-design.md` §5.2、ADR 0004、spec、ticket 07 |
| R8 | 中 | 同步请求断连行为未定义，流式断连即取消的取舍未记录在 ADR | 05、12 | 已解决：同步请求进入模型调用后不因客户端断连中止、流式断连取消上游并在 ADR 0004 记录取舍，已同步至 `DESIGN.md`、`docs/backend-scaffold-design.md` §5.2、ADR 0004、spec、ticket 05/12 |
| R9 | 中 | 用户与会话删除的状态机边界留白（`user_identities` 处置、活跃租约收敛、`deleting` 状态码） | 02、14、15 | 已解决：删除完成前保留 `user_identities` 防静默重建、明确活跃租约收敛顺序及 `deleting` 会话 `404`/`409` 语义，已同步至 `DESIGN.md`、`docs/backend-scaffold-design.md` §6.2/§8.2/§9.1/§11.3、ADR 0005、spec、ticket 14/15 |
| R10 | 低 | 模型配置同步到 PostgreSQL 的具体录入入口待细化 | 06 | 已解决：明确由服务端受控配置在启动或显式同步命令中幂等写入 `model_configs`，已同步至 `DESIGN.md`、`docs/backend-scaffold-design.md` §2.2/§6.2/§10、spec、ticket 06 |

### 文档与工程工具问题

| 编号 | 级别 | 问题摘要 | 影响范围 | 状态与落点 |
| --- | --- | --- | --- | --- |
| D1 | 中 | 删除屏障、操作进度与 tombstone 等高风险流程缺少显式状态机与时序图 | 13、14、15 | 已解决：已在 `docs/backend-scaffold-design.md` 新增 §11.3.1 状态转换表与 Mermaid 清空时序图，并由 `DESIGN.md`、spec、ticket 13 引用 |
| D2 | 低 | 同一契约规则在多份文档中重复描述，长期有漂移风险 | 全局文档 | 已缓解：已收拢 `DESIGN.md` 部分重复细节并指向 `docs/backend-scaffold-design.md` 唯一维护入口 |
| D3 | 低 | `DESIGN.md` 存在悬空的“用户级个性化配置”概念及 TypeScript 类型示意 | 03、04、05 | 已解决：已移除悬空概念并为 `MemoryContext`/`PersonaSnapshot` 标注“结构示意（非实现语言）” |
| D4 | 低 | `tools/check_architecture.py` 重复路由检测未拼接 `APIRouter(prefix=...)`，后续易误报 | 01 及后续路由 | 已修复：已更新 `tools/check_architecture.py` 解析并拼接 `APIRouter(prefix=...)` 后判重，并同步 `docs/backend-scaffold-design.md` §13.4 |

---

## 3. 设计与契约问题详情

### R1（高）：MemOS Cloud 实际能力与 `MemoryProvider` 契约存在结构性脱节

当前设计（[docs/backend-scaffold-design.md](docs/backend-scaffold-design.md) §8.2/§11.2/§11.3、[ADR 0005](docs/adr/0005-memory-epochs-and-deletion-barriers.md)、[ticket 04](.scratch/ai-chat-memory/issues/04-list-and-explicitly-create-memories.md)）把显式创建、两级范围隔离、游标分页和清空屏障建立在“提供方支持单条 CRUD、客户端幂等键、结果核验与按范围删除”的假设上。对照仓库内 MemOS Cloud 文档，存在 5 处直接差异：

| 设计假设 | MemOS Cloud 文档现状 | 来源 |
| --- | --- | --- |
| 显式创建确认后返回 `201` 和单条稳定记忆 `id` | 只有 `POST /add/message`，由模型从消息中抽取/合并 0 至多条记忆，无单条创建返回单个 `memory_id` 的契约 | [api-add-message.md](.agents/skills/memos-cloud/resources/cn/api-add-message.md) |
| 写入使用稳定任务 ID 作为上游幂等标识并可核验结果 | `addMessage` 未提供文档化的客户端幂等键参数 | [api-add-message.md](.agents/skills/memos-cloud/resources/cn/api-add-message.md) |
| 上游迟到执行属于边缘异常；结果未知时持续暂停屏障并人工告警 | 默认异步模式，抽取通常延迟数秒至数十秒，迟到生效是常态；若无精确写入状态查询，单次网络超时即可让该用户记忆功能长期锁死在屏障中 | [features-async-mode.md](.agents/skills/memos-cloud/resources/cn/features-async-mode.md) |
| 支持 `user`/`conversation` 硬隔离，且删会话只删会话记忆 | `searchMemory` 的 `conversation_id` 仅优先召回、不隔离；`filter` 不直接支持 `conversation_id`；`deleteMemory` 仅支持按 `user_id` 全删或按 `memory_ids` 精确删；`getMemory` 不支持 filter | [api-search-memory.md](.agents/skills/memos-cloud/resources/cn/api-search-memory.md)、[api-delete-memory.md](.agents/skills/memos-cloud/resources/cn/api-delete-memory.md)、[api-get-memory.md](.agents/skills/memos-cloud/resources/cn/api-get-memory.md) |
| 列表游标分页可在适配器内部维护有界分页快照 | `getMemory` 为 `page`/`size` 分页；但 V1 是无状态多 API 副本且无 Redis，快照存内存无法跨副本，存 PostgreSQL 又未在 §6.2 与 §11.4 建表和规划清理 | [api-get-memory.md](.agents/skills/memos-cloud/resources/cn/api-get-memory.md)、[docs/backend-scaffold-design.md](docs/backend-scaffold-design.md) §6.2/§8.2 |

当前计划在 04 的适配器实现和 18 的部署验收中确认供应商能力，而 08、09、13–15 的正确性高度依赖这些能力，核验时点偏晚。

**建议**：

1. 在 `04` 开始前对 MemOS Cloud 做一次最小能力实测，确认 `async_mode=false` 返回字段、`get/status` 可核验粒度、重复提交行为、按 ID 删除收敛延迟及 `tags`/`info` 过滤能力。
2. 根据实测结果二选一收敛契约：
   - **贴合 MemOS 异步抽取模型**：放宽显式创建响应契约（允许返回受理状态或批次关联），用 `tags`/`info` 明确编码范围与过滤限制，并将清空屏障的解除条件改为“等待在途调用完成/超过上限窗口后执行最终全量清空（必要时二次补偿）”，避免永久锁死用户记忆。
   - **本地维护权威索引**：若产品必须保留 `201 {id}` 单条显式记忆、跨副本稳定游标与精确按会话清理，在 PostgreSQL 显式增加记忆元数据/分页快照表及保留清理规则。

---

### R2（高）：普通对话写入的记忆范围归属缺少规则

[DESIGN.md](DESIGN.md) §3.4 定义了用户记忆（跨会话）和会话记忆（仅当前会话）两个范围，[ticket 04](.scratch/ai-chat-memory/issues/04-list-and-explicitly-create-memories.md) 规定显式创建默认为用户级记忆。但普通对话完成后异步写入（[ticket 09](.scratch/ai-chat-memory/issues/09-persist-and-retry-memory-writes.md)）的记忆归属哪个范围、会话级记忆如何产生、`Persona.memoryPolicy` 是否控制写入范围，文档均未定义。

同时 MemOS 的 `searchMemory` 不按 `conversation_id` 做硬隔离，会话级隔离必须由适配器在写入标记和检索过滤（[features-filters.md](.agents/skills/memos-cloud/resources/cn/features-filters.md)）两端显式配合。

**建议**：在 `04` 开始前明确并写入 spec：

- 普通回合写入的默认范围，以及人设 `memoryPolicy` 是否允许覆盖；
- 会话记忆的产生入口与判定方式（显式创建是否允许指定 `scope="conversation"`，或普通回合如何区分用户级/会话级）；
- 适配器写入元数据标记与检索过滤条件的映射规则。

---

### R3（高）：“备份恢复后重放删除记录”缺少持久化载体

[DESIGN.md](DESIGN.md) §6.2、[docs/backend-scaffold-design.md](docs/backend-scaffold-design.md) §12.4、[ADR 0005](docs/adr/0005-memory-epochs-and-deletion-barriers.md) 与 [ticket 18](.scratch/ai-chat-memory/issues/18-verify-production-deployment-and-recovery.md) 都将“备份恢复后暂停写入，重放备份时间点之后的删除与代次记录；删除记录必须有覆盖备份恢复窗口的可恢复副本”作为正式契约和上线验收项。

但整个系统架构仅包含单个 PostgreSQL 实例，没有任何独立于主库之外的持久化通道（如外部只追加审计存储、对象存储归档或独立审计库），且 `13`/`14`/`15` 的删除记录均只写入主库。主库回滚至历史备份点时，该时间点之后的删除与代次记录会一并丢失，导致 `18` 的验收条件在当前架构中无法兑现。

**建议**：二选一明确写入架构：

- 在 `13`–`15` 增加独立于主库备份周期的删除/代次审计事件落盘（例如写入对象存储或独立只追加审计库），并在 `18` 提供可执行重放工具；
- 或收窄承诺，明确以 PostgreSQL PITR（WAL 归档精确时间点恢复）为主，并如实界定 WAL 丢失时的 RPO 与删除一致性边界。

---

### R4（中）：Ticket 依赖图存在 3 处客观的依赖缺失

核对 [.scratch/ai-chat-memory/spec.md](.scratch/ai-chat-memory/spec.md) 依赖图与各 ticket 验收条件，存在 3 处遗漏：

1. [ticket 14](.scratch/ai-chat-memory/issues/14-query-and-delete-owned-conversations.md) 验收条件要求后台分批清理“**相关幂等**”，但依赖为 `07、09、13`，未依赖 `10`（支持聊天幂等）。
2. [ticket 15](.scratch/ai-chat-memory/issues/15-delete-user-and-associated-data.md) 验收条件要求清理“**幂等**”，但依赖为 `11、13、14`，整条传递依赖链完全绕过 `10`。
3. [ticket 17](.scratch/ai-chat-memory/issues/17-add-resource-limits-and-observability.md) 验收条件要求指标覆盖“**屏障时长**”，但依赖为 `09、12`，未依赖 `13`（记忆删除屏障）。

**建议**：在动工前立即更新 [spec.md](.scratch/ai-chat-memory/spec.md) 与对应 ticket，补齐 `10 -> 14`（自动传递覆盖 `15`）和 `13 -> 17` 两条直接依赖边。

---

### R5（中）：Agent、人设草稿与版本关系存在歧义，且默认模型停用场景未定义

- [DESIGN.md](DESIGN.md) §3.5 把 `version` 与 `status: 'draft' | 'published' | 'archived'` 放在同一 `Persona` 结构上；[docs/backend-scaffold-design.md](docs/backend-scaffold-design.md) §6.2 中 `agents` 持有草稿和当前发布版本引用，`persona_versions` 也带发布状态；API 又同时暴露 `PATCH /v1/agents/{agentId}`、`POST /v1/agents/{agentId}/versions` 和 `POST /v1/agents/{agentId}/publish`。草稿究竟存在 `agents` 行上还是 `persona_versions` 未发布行上、`/versions` 与 `/publish` 的职责边界容易产生分歧。
- `defaultModelId` 与 `memoryPolicy` 是否固化在不可变人设版本快照中未明确。若已固化，当该模型被服务端停用后，旧会话在请求省略 `modelId` 时将无法继续（V1 无自动故障转移），需要定义明确错误码及是否允许客户端显式传入其他已启用 `modelId` 绕过。
- [DESIGN.md](DESIGN.md) §3.5 仍存在“删除人设”措辞，其余位置已统一为“停用 Agent”。

**建议**：在 `03` 实现前明确草稿存储模型、版本快照字段清单、三个管理接口的精确语义及默认模型停用时的旧会话处理策略，并统一用词。

---

### R6（中）：`PersonaProvider` 读写职责混装且与未来远程拆分冲突

- **职责混装**：[DESIGN.md](DESIGN.md) §3.1 将 `PersonaProvider` 定义为包含读取、草稿修改、发布与停用的读写接口，但 [docs/backend-scaffold-design.md](docs/backend-scaffold-design.md) §2.2 规定管理路由直接调用 `agents` 服务并不经过 `PersonaProvider`，而 `Conversation Core` 仅需要只读人设快照。
- **事务边界冲突**：[docs/backend-scaffold-design.md](docs/backend-scaffold-design.md) §5.2 第 2 步将“固定人设”放在数据库短事务内。若未来按 [DESIGN.md](DESIGN.md) §10.2 将 `PersonaProvider` 替换为远程 HTTP 适配器，在数据库短事务内触发外部调用会直接违反 §5.1“外部调用在事务外执行”的原则；同时若 `conversations` 对本地 `agents`/`persona_versions` 建强外键，也会阻断人设独立拆分。

**建议**：将对话内核依赖的 `PersonaProvider` 收缩为只读快照契约；对话准备阶段在短事务**之前**通过 `PersonaProvider` 解析人设版本，再进入短事务写入会话与生成记录（并明确 V1 是否保留本地外键或使用逻辑引用）。

---

### R7（中）：过期生成租约的接管依赖 Worker 存活

[docs/backend-scaffold-design.md](docs/backend-scaffold-design.md) §5.2 与 §11.1 将过期生成租约恢复交给 Worker 任务处理。如果会话单活跃生成在数据库层依赖 `status = 'running'` 的部分唯一索引或状态判断，且 API 领取租约时不检查旧租约是否过期，那么一旦 Worker 停机或恢复延迟，该会话在租约过期后仍会持续返回 `409 conversation_generation_conflict`。

**建议**：在 `07` 中规定新请求领取生成租约时，若检测到已有 `running` 回合且其 `lease_expires_at <= now()`，在同一短事务内先将旧回合条件收敛为 `failed`（`generation_lease_lost`）后直接接管租约；Worker 恢复器作为没有新请求到达时的兜底机制保留。

---

### R8（中）：同步请求断连行为未定义，流式断连取舍缺少说明

- [docs/backend-scaffold-design.md](docs/backend-scaffold-design.md) §8.4 明确流式断连会尝试取消上游并将未提交的生成标为 `cancelled`，随后同幂等键重试返回 `409 generation_cancelled`（需换新键重生成）。但文档**未定义同步请求（`stream=false`）在客户端超时断连时的行为**：若同步请求也因客户端断连而取消，则“首次响应丢失后用原幂等键重试取回完整 JSON”的恢复能力在网络读超时场景下会失效。
- 流式“断连即取消”与“断连后继续生成至期限供查询/重试取回”是成本控制与弱网体验之间的产品权衡，建议在 [ADR 0004](docs/adr/0004-generation-idempotency-and-commit-boundary.md) 中补记取舍依据。

**建议**：在 `05`/`12` 中明确区分同步与流式断连策略（建议同步请求进入模型调用后不因客户端 HTTP 断开而中止，确保提交后可通过原幂等键取回），并在 ADR 0004 记录流式断连即取消的理由。

---

### R9（中）：用户与会话删除流程的状态机边界留白

1. **`users` 与 `user_identities` 的最终处置未定义**：
   [docs/backend-scaffold-design.md](docs/backend-scaffold-design.md) §6.2 中 `users.status` 只有 `active`/`disabled`/`deleting`（无 `deleted`），§9.1 规定“按已验证 `(issuer, subject)` 查找或创建稳定本地用户”。若 `15` 在本地清理完成后物理删除 `user_identities`，外部 IdP 仍有效的 JWT 再次访问时会静默重建一个全新 `active` 账号（甚至发生在远程清理尚未结束之时）；若永久保留 `user_identities`，则该主体永远无法再使用服务，也可能与用户数据清除预期冲突。
2. **删除处理器遇到活跃生成租约的收敛方式及 `deleting` 态 HTTP 状态码未明确**：
   `14`/`15` 要求清理前确认在途生成收敛，但未说明清理任务遇到仍在有效期内的生成租约时，是被动等待其完成/过期，还是主动吊销租约；另外，会话已标记 `deleting` 但记录尚未删完期间，查询会话/生成或重放幂等键应返回 `404` 还是 `409` 也未统一约定。

**建议**：在 `13`/`14` 开始前明确：

- 用户删除完成后 `users` 与 `user_identities` 的留存或销毁策略（以及远程清理完成前如何阻止同一 `(issuer, subject)` 重建）；
- 删除任务对活跃生成租约的处置策略，以及 `deleting` 态会话在读/写/幂等查询下的统一错误码。

---

### R10（低）：模型配置录入途径待细化

[DESIGN.md](DESIGN.md) §3.2 规定模型配置保存在 PostgreSQL，且管理后台不在 V1 范围。[ticket 06](.scratch/ai-chat-memory/issues/06-discover-and-select-models.md) 已给出“通过受控配置持久化或同步”的方向，但未细化具体落点（如启动时从配置文件/环境变量幂等 upsert、种子迁移或运维 CLI），也未说明配置中移除/停用模型时与已有会话的联动（见 R5）。

**建议**：在 `06` 实现时敲定同步机制与停用行为，并与 R5 保持一致。

---

## 4. 文档与工程工具问题详情

### D1（中）：高风险删除与屏障流程缺少显式状态机

生成状态机在文档中已较清晰，但以下跨表/跨进程并发流程目前只有压缩的文字描述，实现时容易推导出不一致的分支：

- `memory_scopes` 屏障生命周期（正常、清空中、待核验、解除）及并发写入登记串行顺序；
- `operations` 删除操作的分阶段进度字段与终态转换；
- `memory_tombstones` 的核验与清理生命周期；
- 清空期间旧普通写入、显式创建、迟到上游响应与最终清空的完整时序。

**建议**：在 `13` 开始前补充状态转换表与清空时序图，作为 `13`–`15` 的共同实现依据。

### D2（低）：同一规则在多份文档重复描述

幂等、SSE 事件、保留期限（24h/30 天）和删除屏障规则在 `DESIGN.md`、`docs/backend-scaffold-design.md`、`spec.md` 与 ADR 中均有完整复述。虽然 [docs/backend-scaffold-design.md](docs/backend-scaffold-design.md) §2.1 约定了事实维护位置，但重复细节在后续修改中容易漂移。

**建议**：后续修订时让 `DESIGN.md` 只保留原则与链接，具体状态码、字段和保留参数统一收敛到 `docs/backend-scaffold-design.md`。

### D3（低）：`DESIGN.md` 存在悬空概念与 TypeScript 类型示意

- [DESIGN.md](DESIGN.md) §3.3 的 Prompt Compiler 输入层包含“用户级个性化配置”，但全套表结构、API 与 18 张 ticket 均无对应模型和入口。若 V1 不包含，建议移除或明确标注为后续预留。
- [DESIGN.md](DESIGN.md) §3.4/§3.5 使用 TypeScript 代码块描述 `MemoryContext` 与 `Persona`，建议明确标注为结构示意，或在 `01`/`03`/`04` 落地后改引实际 Python 协议类型。

### D4（低）：`tools/check_architecture.py` 重复路由检测存在潜在误报

[tools/check_architecture.py](tools/check_architecture.py) 的 `check_duplicate_literal_routes` 仅通过 AST 提取路由装饰器的第一个字面量字符串（如 `@router.get("")`），未拼接所属 `APIRouter(prefix=...)`。当 `agents`、`conversations`、`memories` 等模块按 FastAPI 常规写法各自声明 `@router.get("")` 时，会被归并为同一个 `("GET", "")` 并误报重复路由。

**建议**：在 `01` 扩展架构检查或新增业务路由前，补全 `APIRouter` 前缀解析，或改用 §13.4 规划的应用装配后动态路由判重。
