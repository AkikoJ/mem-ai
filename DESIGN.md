# Mem-AI 总体设计

后端工程结构、FastAPI 与 PostgreSQL 技术选型、SQLAlchemy ORM、事务、迁移、测试和部署方案见 [后端脚手架工程设计](docs/backend-scaffold-design.md)。

## 1. 目标

Mem-AI 是一个可复用的 AI 对话后端和 API。它提供带长期记忆的对话能力，并允许在不修改对话内核的情况下替换模型、记忆实现和 Agent 人设。

首版采用模块化单体，API 与 Worker 使用独立进程入口，共享业务模块和 PostgreSQL。模块通过稳定的提供方接口保留未来独立部署的接缝。

首版不引入 Cordis。Cordis 作为未来可选的插件运行时，只在系统需要运行时插件加载、卸载、依赖管理、第三方插件或多 Agent 组合时引入。

## 2. 已确认的设计边界

### 2.1 产品边界

首版聚焦以下能力。

- 对外提供 AI 对话 API
- 支持同步和流式回复
- 支持多个服务端注册的模型
- 支持用户记忆和会话记忆
- 支持 MemOS 作为可替换的记忆实现
- 支持用户拥有、修改、发布和停用自己的私有 Agent 人设
- 支持聊天幂等、生成终态查询、会话与用户删除
- 支持后台任务恢复、记忆删除屏障、到期清理和最小生产运行能力
- 支持人设版本化
- 保存应用自己的原始对话记录
- 预留记忆服务和人设服务的远程适配器

后续阶段再考虑以下能力。

- 用户自定义 API Key
- 用户自定义 Base URL
- 公开人设市场
- 团队共享人设
- 团队级或工作区级共享记忆
- 管理员角色和管理后台
- Cordis 插件运行时
- 具体认证供应商的绑定

### 2.2 部署策略

API 进程负责 HTTP、对话编排和提供方调用。Worker 进程领取 PostgreSQL 持久任务，执行记忆写入、删除和周期清理。两者分别管理连接池、HTTP 客户端、并发和停机流程。

```text
API 进程 → Conversation Core → 提供方接口 → 模型、人设和记忆适配器
                    ↓
                PostgreSQL ← Worker 进程 → 任务处理器 → 记忆适配器
```

模块通过项目自己的接口协作。未来可将 `MemoryProvider` 或 `PersonaProvider` 替换为远程适配器，对话编排继续使用原有契约。

## 3. 核心架构

```text
┌──────────────────────────────┐
│            Chat API           │
│       同步和流式响应入口       │
└──────────────┬───────────────┘
               │
┌──────────────▼───────────────┐
│       Conversation Core      │
│      组织一次对话的完整流程     │
└───────┬────────┬─────────────┘
        │        │
┌───────▼────┐ ┌─▼────────────────┐
│ ModelRouter│ │ PromptCompiler   │
│ 选择模型    │ │ 合并人设和记忆     │
└───────┬────┘ └───┬───────────────┘
        │          │
┌───────▼──────┐ ┌─▼───────────────┐
│ModelProvider │ │PersonaProvider  │
│模型适配器     │ │人设和版本读取     │
└──────────────┘ └─────────────────┘
               │
        ┌──────▼──────┐
        │MemoryProvider│
        │记忆检索和写入 │
        └─────────────┘
```

### 3.1 Conversation Core

`Conversation Core` 校验认证主体和资源归属，解析幂等记录，获取会话生成租约，固定人设和模型，召回记忆并编译输入。调用模型之前持久化用户消息和实际注入的记忆引用，调用成功后原子保存完整 Assistant 回复、生成成功状态和普通记忆任务。

提供方接口由项目拥有，Python 实现使用类型化协议和数据模型。

| 接口 | 能力和结果契约 |
| --- | --- |
| `ModelProvider` | 同步完整生成和异步流，统一增量、结束原因、用量、超时及取消语义 |
| `MemoryProvider` | 召回、显式创建、完整回合写入、游标列表、单条删除、按范围清空、操作结果核验 |
| `PersonaProvider` | 私有人设及不可变发布版本的只读快照解析（管理草稿、发布和停用由 `agents` 模块负责） |

记忆适配器将结果归一为已完成、待确认或已失败，并暴露稳定的外部关联标识和幂等能力。外部结果未知时保留可核验状态。供应商能力通过适配器契约测试确认，提供方的原始结构封装在适配器内。

#### 对话流程的共同入口

同步和流式对话共用一次生成的准备、上下文构造和结果收敛。Conversation Core 负责组织流程，生成持久化阶段负责状态转换与事务，SSE 层负责事件表达和连接通知。幂等重试解析原生成，取消和过期恢复调用同一终态入口。

这些职责及增量接入顺序以 [共享生成生命周期](docs/backend-scaffold-design.md#54-共享生成生命周期) 为实现契约，模块与数据所有权见 [工程职责](docs/backend-scaffold-design.md#22-模块所有权)。实际公共符号随首个实现切片记录，后续切片在原入口扩展。

### 3.2 Model Router

`Model Router` 根据 Agent 默认配置、受控的请求参数和服务端策略选择模型。

首版按以下优先级选择模型。

```text
服务端强制策略
  -> 策略允许的请求 modelId
  -> Agent 默认模型
```

普通用户只能引用服务端已经注册并启用的 `modelId`。请求不能携带 API Key、Base URL 或未注册的模型配置。

V1 使用一次选定的模型完成生成，上游失败记录为该次生成失败。模型配置由服务端通过受控配置同步到 PostgreSQL，凭据保存在环境变量或密钥管理系统中，不进入客户端请求和普通业务数据表。若旧会话固定人设快照引用的默认模型已停用且请求省略 `modelId`，调用前返回稳定错误；策略允许时请求可显式传入其他已启用 `modelId` 继续。

### 3.3 Prompt Compiler

`Prompt Compiler` 将以下内容合并为模型输入：

```text
平台级规则
  -> Agent 人设
  -> 相关记忆
  -> 当前会话此前成功回合的最近消息
  -> 当前用户输入
```

记忆只在与当前问题相关时注入。记忆内容需要经过统一格式化，模型不应直接看到 MemOS 或其他供应商的原始响应结构。

建议使用如下语义：

```text
仅使用与当前问题明确相关的用户记忆。
如果记忆与当前输入冲突，或看起来属于其他用户，应忽略该记忆。
```

### 3.4 Memory Provider

记忆模块提供统一接口，具体实现可以是：

```text
MemOSMemoryAdapter
LocalMemoryAdapter
PostgresMemoryAdapter
RemoteMemoryAdapter
```

首版可以使用 MemOS 作为长期记忆实现。MemOS 的 HTTP 请求、鉴权、字段映射、范围过滤和错误处理全部封装在适配器内。

对话内核只知道统一的记忆结构（下方为数据结构示意，实际 Python 契约以 `contracts/memory_provider.py` 为准）：

```ts
// 结构示意（非实现语言）
type MemoryContext = {
  items: Array<{
    id: string
    type: 'fact' | 'preference' | 'goal' | 'summary'
    content: string
    relevance: number
    scope: 'user' | 'conversation'
  }>
}
```

首版支持两个记忆范围：

- 用户记忆（`user`），跨会话保留
- 会话记忆（`conversation`），只服务于当前会话

普通对话回合写入默认归属由人设快照的 `memoryPolicy.writeScope` 控制（默认 `user`，可设为 `conversation`）；显式创建默认为 `user`，支持指定 `conversation` 并校验会话归属。首版不支持 Agent、团队和工作区共享记忆。

### 3.5 Persona Provider

Agent 与人设由用户拥有且默认私有，拥有者通过 `agents` 管理接口编辑草稿、发布不可变版本和停用自己的 Agent；对话内核通过只读 `PersonaProvider` 解析最新或固定的不可变人设快照。

人设快照包含以下信息（下方为数据结构示意，实际 Python 契约以 `contracts/persona_provider.py` 与 `modules/agents/` 为准）：

```ts
// 结构示意（非实现语言）
type PersonaSnapshot = {
  agentId: string
  ownerId: string
  name: string
  version: number
  systemPrompt: string
  behaviorRules: string[]
  tone?: string
  language?: string
  defaultModelId: string
  memoryPolicy: {
    enabled: boolean
    recallLimit: number
    writeEnabled: boolean
    writeScope: 'user' | 'conversation'
  }
}
```

草稿保存在 `agents` 行上，`persona_versions` 仅保存已发布的不可变快照（含 `defaultModelId` 与 `memoryPolicy`）。普通对话写入由 `writeEnabled` 和 `writeScope` 控制并异步执行，显式记忆操作由各自 API 契约决定完成方式。会话创建时固定人设版本，后续草稿修改、版本发布和停用只影响新会话。

## 4. 对话流程

### 4.1 非流式请求

1. 认证得到 `AuthenticationContext`，检查用户启用状态、Agent 和会话归属。
2. 以用户、HTTP 方法、规范路由和 `Idempotency-Key` 查询幂等记录。命中时按原状态返回结果；未命中则在事务外通过 `PersonaProvider` 只读解析目标人设版本快照。
3. 短事务创建幂等记录、按需创建会话并记录固定人设版本、取得生成租约（若旧租约已过期则同事务先收敛旧回合后接管），创建 `running` 生成回合和原始用户消息。
4. 在事务外召回记忆、编译 Prompt。短事务校验租约及删除状态并保存实际注入引用，随后调用模型。同步请求进入模型调用后不因客户端 HTTP 断连而中止，以便生成期限内完成提交后由原幂等键取回结果。
5. 模型完整结束后，在一个短事务内校验租约令牌、用户和会话状态，写入完整 Assistant 消息、`succeeded` 状态、幂等结果和允许的记忆任务。
6. 提交成功后释放生成资格并返回结果。失败、取消和租约丢失均记录可查询终态。

模型上下文使用此前成功回合的用户消息和完整回复，再加当前输入。失败或取消回合中的用户消息保留在历史记录中，后续模型上下文依据生成状态筛选。

### 4.2 流式请求

流式入口复用上述身份、幂等、租约、事务和模型路由。用户消息与生成标识提交后发送 `conversation.started`，随后发送 `message.delta` 和保活 `heartbeat`。完整回复成功提交后发送 `conversation.completed`，流开始后的错误通过 `conversation.failed` 表达。事件字段和恢复方式见 [SSE 契约](docs/backend-scaffold-design.md#84-sse-和结果恢复)。

每个业务事件携带生成回合内递增的事件 ID。V1 客户端通过生成查询和原幂等键恢复状态。增量仅在当前连接内传输。

客户端断开时尝试取消上游。成功提交已经完成则保留成功，否则将生成转为 `cancelled`，丢弃部分 Assistant 输出并停止完整回合记忆入队。连接状态与数据库最终提交发生竞争时，以条件提交决定唯一终态（取舍依据见 [ADR 0004](docs/adr/0004-generation-idempotency-and-commit-boundary.md)）。

### 4.3 记忆写入

普通对话成功与记忆任务入队使用同一数据库事务。Worker 通过统一 `jobs` 表执行至少一次投递，复用稳定任务 ID，使用提供方幂等或结果核验避免重复副作用。写入失败重试，结果未知进入核验流程，达到尝试上限后留下可观测终态。

显式创建记忆在提供方同步确认并返回稳定单条 ID 时返回 `201`；若提供方仅受理异步抽取而尚未产生单条稳定 ID，返回 `202` 与可查询操作。单条删除和清空先持久化删除操作，确认完成返回 `204`，继续处理或结果待确认返回 `202`、操作 ID 和查询地址。清空使用 `confirm=true`。具体契约见 [接口规范](docs/backend-scaffold-design.md#8-api-规范)。

## 5. 并发、幂等和删除一致性

### 5.1 生成租约

同一会话同时只有一个生成回合。不同幂等请求争用活跃会话时返回 `409 conversation_generation_conflict`，不同会话可并行。

PostgreSQL 原子条件更新领取租约，生成唯一令牌并持续续期。若新请求发现旧 `running` 回合租约已过期，在同一短事务内先将其收敛为 `failed`（`generation_lease_lost`）后直接接管，避免 Worker 不可用时阻塞新请求；Worker 过期恢复器负责无后续新请求时的兜底收敛。续期、消息完成、状态更新和释放都校验令牌、有效期及用户和会话状态。数据库事务覆盖短时状态变更，外部 HTTP 和完整流传输在事务外执行。

### 5.2 聊天幂等

`POST /v1/chat` 必须携带 `Idempotency-Key`。唯一范围为 `(user_id, HTTP method, canonical route, key)`。业务摘要包含 Agent、传入的会话 ID 或缺省标记、消息和请求模型等语义参数，`stream` 作为传输偏好从摘要中排除。实际模型和人设版本保存在首次生成记录中，重试复用首次决定。

初次省略会话 ID 时，幂等占位、会话和生成关联原子创建。同键同摘要的并发请求只能创建一份会话及生成。幂等查询先于生成租约争用。

| 已有记录 | 重试结果 |
| --- | --- |
| `running` | `202`，返回原会话、生成 ID 和结果查询地址 |
| `succeeded` | `200 application/json`，返回已保存的完整结果，适用于任意 `stream` 偏好 |
| `failed`、`cancelled` | 返回原终态及稳定错误，重新生成需新键 |
| 同键不同业务摘要 | `409 idempotency_key_conflict` |

活跃幂等记录持续保留，终态记录从终态提交起保留 24 小时。到期后相同键作为新请求处理，生成记录仍按会话保留策略保存。查询和重试都重新校验当前资源权限、用户和会话状态。

### 5.3 删除屏障与记忆代次

用户与记忆提供方之间保存单调递增的记忆代次。每次生成、显式写入和普通记忆任务捕获该代次，写入发起登记与清空提升代次使用相同范围锁串行提交。清空操作原子提升代次、建立屏障、取消旧代次待执行任务，并暂停该范围的记忆读写。聊天可继续使用人设和成功历史消息，显式记忆创建遇到屏障返回 `409 memory_scope_clearing`。

已发出的外部写入需要核验实际结果，旧写入全部收敛后再次清空提供方，确认清空后解除屏障。若提供方缺乏单次写入精确状态查询能力，为防止单次网络超时永久锁死用户记忆，在途写入达到配置的上限收敛窗口后保留审计告警，执行最终远程清空与延迟二次补偿清空，确认后解除屏障；最终清空结果未知时屏障保持待确认并告警。详细状态机与清空时序见 [后端工程设计](docs/backend-scaffold-design.md#113-删除操作和代次屏障)。旧代次生成稍后完成时照常保存回复，并抑制旧代次记忆任务。

单条删除持久化待删除标记后立即从新召回中过滤。历史引用保留并标记已删除或无法解析，只保存引用元数据。记忆清理针对提供方派生数据，原始对话记录遵循会话和用户删除入口。

### 5.4 会话和用户删除

会话删除立即标为 `deleting` 阻止续聊并隐藏于列表，后台清理消息、引用、生成、相关幂等及该会话范围的记忆（提供方不支持按会话批量删除时按筛选出的目标 ID 分批删除并叠加本地删除标记隔离）。用户级记忆由显式清空或用户删除处理。在途生成在续期和最终提交前复查状态，失联租约由删除流程待其过期收敛后推进清理。

用户删除先将本地用户置为 `deleting` 阻止普通业务访问，并保持 `user_identities` 绑定防止删除期间旧凭证静默重建新账号；随后分批删除其 Agent、人设版本、会话、消息、引用、生成、幂等数据及全部用户与会话记忆。待本地与远程清理均确认完成后，最终阶段移除 `user_identities` 与用户业务记录，仅保留脱敏操作摘要和回执至有效期届满。

删除请求返回 `202` 和随机删除回执。服务端仅保存回执摘要，客户端通过 `Authorization: DeletionReceipt <receipt>` 查询指定删除操作。回执权限限定为该操作状态查询，具有可配置有效期。操作结果分别展示本地完成和远程待确认状态，全部完成后才标记 `succeeded`。

## 6. 数据模型

PostgreSQL 保存应用主数据和可恢复任务。表、约束及索引见 [数据库设计](docs/backend-scaffold-design.md#6-数据库模型和规范)。

| 表或视图 | 领域用途 |
| --- | --- |
| `users`、`user_identities` | 本地业务用户、停用/删除状态和已验证签发方与主体映射 |
| `agents`、`persona_versions` | 私有 Agent、唯一草稿、不可变发布版本和当前发布版本 |
| `conversations`、`messages` | 会话归属、固定人设和原始有序消息 |
| `generations` | 每次生成的用户消息、实际模型、人设版本、状态、错误和租约 |
| `idempotency_records` | 用户范围的请求摘要、生成关联及终态到期时间 |
| `message_recalled_memories` | 实际注入模型的提供方记忆引用及召回元数据 |
| `model_configs` | 受控同步的模型能力、启用状态及服务端凭据引用 |
| `jobs`、`job_attempts` | 统一持久任务、执行租约、重试和有限期执行摘要 |
| `memory_jobs` 只读视图 | 统一任务中记忆写入任务的领域查询视图 |
| `operations` | 删除与异步记忆操作的聚合进度、回执摘要与审计导出源 |
| `memory_scopes`、`memory_tombstones`、`memory_list_snapshots` | 用户与提供方记忆代次、屏障、待删除标记及多副本分页快照 |

### 6.1 召回记忆与对话消息的关联

`message_recalled_memories` 关联本次生成的原始用户消息，记录 `provider_id`、`memory_id`、`scope`、`relevance`、`prompt_position`、`memory_epoch`、`recalled_at` 和 `deleted_at`。只为实际注入 Prompt 的记忆建立记录，记忆正文由提供方管理。

`(user_message_id, provider_id, memory_id, scope)` 唯一，用户消息必须属于对应生成、用户和会话。按用户消息、提供方记忆 ID 建立索引，支持历史解释及删除标记。历史引用随父会话保留，提供方记忆删除后仍能说明当时使用过该引用。

### 6.2 数据保留

具体保留期限（含会话、日志、幂等及分页快照）以 [保留清理契约](docs/backend-scaffold-design.md#114-保留清理) 为准，由服务端环境配置。

周期 Worker 用有限批次、游标、租约和幂等操作执行到期清理，保留活跃任务、待确认外部结果、未完成删除所需映射和去重记录。显式删除优先于常规保留规则。生产恢复采用“独立删除审计归档重放”或“PostgreSQL PITR”分层保障，具体规则见 [发布和恢复](docs/backend-scaffold-design.md#124-发布和恢复)。

## 7. API 契约

聊天请求沿用业务字段别名，当前用户从认证上下文解析。

```http
POST /v1/chat
Idempotency-Key: 6bfeac57-8bf8-4204-9ad0-8af772175c37
Content-Type: application/json
```

```json
{
  "agentId": "agent_writer",
  "conversationId": "conv_001",
  "message": "帮我设计一个记忆系统",
  "modelId": "registered-model",
  "stream": true
}
```

省略 `conversationId` 时服务端在首个聊天请求中创建会话。携带会话 ID 时校验 Agent 一致性。

| 资源 | 路由 |
| --- | --- |
| 当前用户 | `GET /v1/users/me`、`DELETE /v1/users/me` |
| 聊天 | `POST /v1/chat` |
| 会话历史 | `GET /v1/conversations`、`GET /v1/conversations/{conversationId}`、`GET /v1/conversations/{conversationId}/messages` |
| 生成结果 | `GET /v1/conversations/{conversationId}/generations/{generationId}` |
| 会话删除 | `DELETE /v1/conversations/{conversationId}` |
| 私有 Agent | `POST /v1/agents`、`GET /v1/agents`、`GET /v1/agents/{agentId}`、`PATCH /v1/agents/{agentId}`、`DELETE /v1/agents/{agentId}` |
| 人设发布 | `POST /v1/agents/{agentId}/versions`、`POST /v1/agents/{agentId}/publish` |
| 记忆 | `GET /v1/memories`、`POST /v1/memories`、`DELETE /v1/memories/{memoryId}`、`DELETE /v1/memories?confirm=true` |
| 删除状态 | `GET /v1/operations/{operationId}` |
| 模型 | `GET /v1/models` |

Agent 删除表现为停用，已有会话可继续使用固定人设。所有业务资源校验当前用户归属。列表使用 `limit`、`cursor`、`items`、`next_cursor`、`has_more` 和稳定游标排序，详见 [接口规范](docs/backend-scaffold-design.md#8-api-规范)。

## 8. 错误处理

所有 JSON 响应统一采用 `{success, code, message, data, request_id}` 外壳；错误响应中 `success` 为 `false`、`data` 为 `null`，并包含稳定的小写下划线 `code`、可读 `message`、`request_id` 和对象型 `details`。

| 场景 | 行为 |
| --- | --- |
| 模型失败或超时 | 生成 `failed`，返回稳定应用错误 |
| 召回失败或删除屏障生效 | 降级为人设、成功历史和当前输入 |
| 普通记忆任务失败 | 已提交回复保持成功，任务重试或转待核验 |
| 显式创建失败 | 返回稳定错误，成功须提供方确认 |
| 删除仍执行或结果未知 | `202` 并返回可查询操作 |
| 活跃会话争用或幂等摘要冲突 | `409`，使用不同错误码 |
| 其他用户资源 | 统一 `404`，隐藏存在性 |
| 未注册、停用或无权使用模型 | 稳定配置或权限错误，上游调用前拒绝 |
| 流式输出中失败 | 记录终态，连接可写时发送失败事件 |

## 9. 安全和权限

认证分为凭证验证适配器和本地用户解析两层，统一产生 `AuthenticationContext(actor_id, user_id, auth_method, permissions)`。签发方与主体的验证结果映射到稳定本地用户，业务使用本地用户校验所有权。

开发与测试提供可替换身份适配器。生产配置接入实际身份验证，启动时拒绝开发适配器和未配置可信边界的身份 Header。可信网关模式需显式声明来源、可信代理和身份验证规则。内置注册、登录、刷新和注销留给后续认证产品范围。

停用和删除中的用户立即失去业务访问权限。删除回执仅允许查询指定操作。模型 API Key、Base URL 和 MemOS 凭据由服务端注册与密钥管理控制。

最小生产基线包括 CORS 白名单、可信 Host/代理、入口分布式限流、进程级并发和请求体限制、执行与流连接期限、Worker 并发预算、关联日志、指标、就绪与迁移版本检查、备份恢复和密钥轮换。具体规则见 [运行与部署设计](docs/backend-scaffold-design.md#12-日志健康检查和部署)。

## 10. 独立服务的拆分策略

首版不拆分服务。模块通过接口隔离，为未来远程适配器保留接缝。

### 10.1 记忆服务拆分条件

满足以下任一条件时，可以将记忆模块独立部署：

- 多个应用需要共享记忆能力
- 记忆检索或写入需要独立扩容
- 记忆数据需要单独的权限和部署边界
- 记忆模块的发布频率明显不同于对话 API
- 记忆供应商需要被多个业务复用

拆分后的结构如下：

```text
Conversation API
  -> RemoteMemoryAdapter
      -> Memory Service
          -> MemOS 或其他记忆实现
```

### 10.2 人设服务拆分条件

满足以下条件后再拆分人设服务：

- 多个应用需要共享 Agent 人设
- 人设需要独立发布和回滚
- 人设拥有权和访问控制需要独立管理
- 人设服务需要独立扩容或独立部署

### 10.3 Cordis 的后续位置

如果未来需要运行时加载和卸载插件、第三方插件生态、插件依赖管理或多 Agent 运行时组合，可以在装配层引入 Cordis。

```text
项目自己的 contracts
  ↑
Conversation Core
  ↑
Cordis Runtime
  ├── Model Plugin
  ├── Memory Plugin
  └── Persona Plugin
```

Cordis 不进入领域接口，也不成为 `Conversation Core` 的业务依赖。

## 11. 首版实现顺序

已确认的实现拆为 18 个垂直切片，完整依赖图和逐项链接见 [实施规格](.scratch/ai-chat-memory/spec.md#implementation-plan)。从 01 工程基础开始，随后沿依赖前沿推进认证、私有 Agent 和同步对话，再补齐幂等、记忆任务、流式终态、删除一致性、保留及生产运行。

每个 ticket 交付可独立验证的公开行为和相应持久化、恢复及测试，并记录复用入口和新增职责。01 建立架构检查和 CI 基础，后续切片扩展检查范围。最终 V1 契约由全部切片共同实现，生产发布以 18 的部署与恢复验收为准。

## 12. 验证重点

- 提供方替换复用对话编排，供应商契约覆盖范围隔离、游标、取消、幂等及未知结果核验。
- 跨用户资源始终隔离，开发身份无法进入生产配置，删除回执仅访问指定操作。
- 已发布人设固定，停用 Agent 阻止新会话并保留旧会话行为。
- 用户消息先提交，完整回复、生成成功与记忆任务原子提交，失败回合保留历史且从后续模型上下文过滤。
- 多进程会话竞争、租约过期及旧执行者恢复均受到令牌保护。
- 同键并发、隐式创建会话、初始响应丢失和终态重试均可恢复，业务摘要冲突返回确定错误。
- SSE 成功事件发生在提交之后，部分输出失败、断连和提交竞争得到唯一可查询终态。
- Worker 重启、重复领取、最大重试、未知上游结果及优雅退出可以恢复。
- 清空期间旧写入、迟到外部响应、旧生成结束、会话及用户删除竞态均遵守删除屏障。
- 到期批量清理保留活动工作和必要映射，生产迁移、应用回滚及备份恢复有可演练路径。
