# Mem-AI 总体设计

后端工程结构、FastAPI 与 PostgreSQL 技术选型、SQLAlchemy ORM、事务、迁移、测试和部署方案见 [后端脚手架工程设计](docs/backend-scaffold-design.md)，领域术语见 [GLOSSARY.md](GLOSSARY.md)，一体化单进程架构决策见 [ADR-0007](docs/adr/0007-unified-single-process-backend.md)。

## 1. 定位与目标

Mem-AI 是一个**自包含、一体化的 AI 对话应用 Python 后端服务**。它面向前端 Web/App/客户端直接提供带长期记忆的完整 AI 对话后端能力，在单一代码库与单一 Python 服务进程内构建覆盖认证、Agent 人设、对话编排、长期记忆、持久后台任务、观测与恢复运维的完整后端体系。

核心定位原则如下：

- **整体作为一个 Python 后端服务**：不追求内部各业务模块独立拆分部署，不再规划独立记忆服务、独立人设服务或远程内部适配器，也不引入 Cordis 等插件运行时。
- **纯单进程 + PostgreSQL 单栈运行**：以 PostgreSQL 为唯一必选有状态基础设施（不强制依赖 Redis、Celery 或消息队列）。主服务进程同时承担 HTTP/SSE 服务与内嵌的持久化后台任务执行器，横向扩展时采用同构单进程副本。
- **内部高内聚、外部保适配**：服务内部通过清晰的领域模块边界与分层纪律避免演变为大泥球；人设管理与读取统一归口 `agents` 模块；仅对外部第三方系统（大语言模型接口、MemOS 等记忆引擎）保留适配边界，用于隔离外部协议、错误处理与测试替换。
- **完整而克制的业务体系**：提供内置邮箱密码 + JWT 完整认证闭环，同时保持模型克制——暂不引入外部 OAuth/SSO/可信网关、角色权限（RBAC）与管理后台，所有活跃用户平等使用服务端已启用模型并严格隔离本人私有资源。

## 2. 完整后端体系与边界

### 2.1 六大体系支柱

Mem-AI 作为一体化 Python 后端服务，由以下 6 个相互协作的子体系构成完整闭环：

| 体系支柱 | 核心职责 |
| --- | --- |
| 1. API 接入与安全基座 | FastAPI 统一 JSON 响应/错误信封、稳定游标分页、请求关联 ID、Host/CORS/代理边界、限流接缝、请求体与进程并发预算 |
| 2. 内置认证与用户体系 | 本地注册、登录、Access/Refresh JWT 签发与轮换、主动注销、改密、用户生命周期状态与分阶段用户删除 |
| 3. Agent 与人设版本体系 | 私有 Agent 创建与草稿编辑、不可变人设版本快照发布、幂等停用及内部只读快照解析（统一归口 `agents`） |
| 4. 对话核心与生成体系 | 同步与 SSE 流式共享生成生命周期、同会话生成租约与过期接管、幂等提交与回放、纯 Prompt 编译、模型路由与外部模型适配 |
| 5. 长期记忆与一致性体系 | 用户/会话双范围召回与故障降级、PostgreSQL 分页快照、记忆代次、删除屏障、tombstone、删除回执与外部记忆引擎适配 |
| 6. 单进程内嵌任务与运维体系 | FastAPI `lifespan` 内嵌 PostgreSQL 持久任务循环（记忆写入、删除清理、租约兜底、到期保留清理）、结构化脱敏日志、低基数指标、就绪/迁移校验、备份恢复与运维 CLI |

### 2.2 产品边界

当前体系聚焦以下能力：

- 内置用户认证闭环（`email` + 密码注册/登录、短期 Access Token + 落库可撤销 Refresh Token、注销与改密；开放注册由配置开关控制）
- 对外提供同步与 SSE 流式 AI 对话 API
- 支持多个服务端注册的模型与受控模型路由
- 支持用户级与会话级长期记忆，支持以 MemOS 等外部引擎作为可替换记忆实现
- 支持用户拥有、编辑草稿、发布不可变版本和停用自己的私有 Agent 人设
- 支持聊天幂等、生成终态查询、会话历史、会话删除与用户删除
- 支持单进程内嵌持久后台任务、记忆删除屏障、到期保留清理、观测限流与生产恢复运维
- 由 PostgreSQL 持久化保存应用自己的原始对话记录与主数据

明确不在当前范围内的能力：

- 外部 OAuth/SSO、第三方 IdP 绑定与可信网关头认证
- 角色权限（RBAC）、多级管理员与 HTTP 管理后台（`/v1/admin/*`）
- 内部模块拆分为独立微服务、远程人设/记忆服务适配器、Cordis 插件运行时
- 用户自定义 API Key / Base URL
- 公开人设市场、团队共享人设、多租户工作区与团队级共享记忆
- 强制依赖 Redis、Celery 或外部消息队列

### 2.3 运行与部署形态

服务打包为单一 Python 工程与单一容器镜像，运行为**纯单进程服务**。FastAPI `lifespan` 统一管理数据库连接池、HTTPX 客户端、在线请求预算与内嵌的**后台任务执行器**（基于 PostgreSQL `jobs` 表的异步消费循环）。

```text
HTTP / SSE 客户端
        ↓
  Mem-AI 单进程服务（FastAPI + lifespan）
    ├── HTTP/SSE 路由 → 业务模块（auth / users / agents / conversations / models / memories / operations）
    │                    └── 外部适配层（ModelProvider / MemoryProvider）→ 外部模型 / 记忆引擎
    ├── 内嵌后台任务执行器（asyncio 消费循环 + 周期调度）
    │                    └── 清理/写入/恢复/保留处理器 → 外部记忆引擎
    ↓
  PostgreSQL（主数据、登录会话、持久任务 jobs、分页快照、删除屏障与操作状态）
```

单机或小型部署运行 1 个进程实例即可完成在线对话与全部后台异步任务；需要横向扩容时，部署多个**同构单进程副本**，借助 PostgreSQL `FOR UPDATE SKIP LOCKED` 领取锁、租约令牌与周期窗口去重安全并发消费，也可通过配置开关在特定副本上关闭内置任务循环。

## 3. 核心架构

```text
┌──────────────────────────────────────────────┐
│           FastAPI HTTP / SSE 接入层           │
│     统一信封 / 认证依赖 / 限流与并发预算       │
└──────────────────────┬───────────────────────┘
                       │
┌──────────────────────▼───────────────────────┐
│              Conversation Core               │
│            组织一次对话的完整流程             │
└───────┬──────────────┬──────────────┬────────┘
        │              │              │
┌───────▼────┐  ┌──────▼──────┐  ┌────▼──────────────┐
│ModelRouter │  │PromptCompiler│ │  agents 业务模块   │
│  选择模型   │  │合并人设与记忆│  │ 人设草稿/版本/读取 │
└───────┬────┘  └──────┬──────┘  └───────────────────┘
        │              │
┌───────▼──────┐ ┌─────▼─────────┐
│ModelProvider │ │MemoryProvider │   ← 仅隔离外部厂商协议
│外部模型适配器 │ │外部记忆适配器  │
└──────────────┘ └───────────────┘
```

### 3.1 Conversation Core

`Conversation Core` 校验当前登录用户与资源归属，解析幂等记录，获取会话生成租约，从 `agents` 模块读取固定人设快照并路由模型，召回记忆并编译输入。调用模型之前持久化用户消息和实际注入的记忆引用，调用成功后原子保存完整 Assistant 回复、生成成功状态和普通记忆任务。

仅面向外部第三方引擎保留项目拥有的类型化协议：

| 外部适配协议 | 职责与契约 |
| --- | --- |
| `ModelProvider` | 对接外部大模型接口，提供同步完整生成和异步流，统一增量、结束原因、用量、超时及取消语义 |
| `MemoryProvider` | 对接外部长期记忆引擎，提供召回、显式创建、完整回合写入、游标列表、单条删除、按范围清空与结果核验 |

人设管理与只读快照解析属于本服务内部能力，统一由 `agents` 模块直接提供，不再抽象为可外置的独立 `PersonaProvider`。记忆适配器将外部结果归一为已完成、待确认或已失败，并暴露稳定的外部关联标识和幂等能力，供应商原始结构封装在适配器内。

#### 对话流程的共同入口

同步和流式对话共用一次生成的准备、上下文构造和结果收敛。`conversations/service.py` 负责组织流程，`conversations/generation.py` 负责持久化阶段与状态转换，SSE 层负责事件表达和连接通知。幂等重试解析原生成，取消和过期恢复调用同一终态入口。

详细实现契约见 [共享生成生命周期](docs/backend-scaffold-design.md#54-共享生成生命周期)，模块所有权见 [工程职责](docs/backend-scaffold-design.md#22-模块所有权)。

### 3.2 Model Router

`Model Router` 根据 Agent 默认配置、受控请求参数和服务端策略选择模型，按以下优先级决定实际模型：

```text
服务端强制策略
  -> 策略允许的请求 modelId
  -> Agent 默认模型
```

所有已登录且处于 `active` 状态的用户均可使用服务端已注册并启用的全部 `modelId`（不做按角色的模型权限过滤）。请求不能携带 API Key、Base URL 或未注册的模型配置。

模型配置由服务端通过配置文件/环境变量在启动时或显式 CLI 命令中幂等同步到 PostgreSQL，凭据保存在环境变量或密钥管理系统中，不进入客户端请求和业务表。若旧会话固定人设快照引用的默认模型已停用且请求省略 `modelId`，调用前返回稳定错误；策略允许时请求可显式传入其他已启用 `modelId` 继续。

### 3.3 Prompt Compiler

`Prompt Compiler` 作为纯数据转换组件，将以下内容合并为模型输入：

```text
平台级规则
  -> Agent 人设
  -> 相关记忆
  -> 当前会话此前成功回合的最近消息
  -> 当前用户输入
```

记忆只在与当前问题相关时注入，并经过统一格式化，模型不直接接触外部记忆引擎的原始响应结构。系统指令明确要求：仅使用与当前问题明确相关的用户记忆；若记忆与当前输入冲突，应以当前输入为准。

### 3.4 外部记忆适配（Memory Provider）

记忆业务策略（范围隔离、召回裁剪、降级、分页快照、代次与删除屏障）内聚在 `memories` 模块中；底层外部记忆引擎通过 `MemoryProvider` 适配器隔离，例如首版使用 `MemOSMemoryProvider` 封装 MemOS Cloud 的 HTTP 请求、鉴权、字段映射、范围过滤、核验与错误映射。

对话内核只消费统一的记忆结构（实际 Python 类型以 `contracts/memory_provider.py` 为准）：

```python
# 结构示意
MemoryItem(id, type="fact|preference|goal|summary", content, relevance, scope="user|conversation")
```

记忆范围分为两类：

- **用户记忆（`user`）**：跨会话保留
- **会话记忆（`conversation`）**：仅服务于当前会话

普通对话回合写入范围由固定人设快照的 `memoryPolicy.writeScope` 控制（默认 `user`，可设为 `conversation`）；显式创建默认为 `user`，支持指定 `conversation` 并校验会话归属。

### 3.5 Agent 与人设版本

Agent 与人设由用户创建并私有拥有，草稿管理、版本发布、停用及对话时的只读快照解析全部归口 `agents` 模块。

人设快照包含以下核心字段（实际 Python 类型以 `modules/agents/` 契约为准）：

- `agentId`、`ownerId`、`name`、`version`
- `systemPrompt`、`behaviorRules`、`tone`、`language`
- `defaultModelId`
- `memoryPolicy`（`enabled`、`recallLimit`、`writeEnabled`、`writeScope`）

草稿保存在 `agents` 行上，`persona_versions` 仅保存已发布的不可变快照。会话创建时固定人设版本，后续草稿修改、新版本发布和停用 Agent 仅影响新会话，已存在的历史会话继续按其固定版本运行。

## 4. 对话流程

### 4.1 非流式请求

1. 校验内置 Access Token，解析本地 `active` 用户，检查 Agent 和会话归属。
2. 以用户、HTTP 方法、规范路由和 `Idempotency-Key` 查询幂等记录。命中时按原状态返回结果；未命中则从 `agents` 读取目标人设版本快照（新会话取最新已发布版本，续聊取会话固定版本）并校验模型路由可用性。
3. 短事务创建幂等记录、按需创建会话并固定人设版本、取得生成租约（若旧租约已过期则同事务先收敛旧回合后接管），创建 `running` 生成回合和原始用户消息。
4. 在事务外召回记忆、编译 Prompt。短事务校验租约及删除状态并保存实际注入引用，随后调用外部模型。同步请求进入模型调用后不因客户端 HTTP 断连而中止，以便在生成期限内完成提交后通过原幂等键取回结果。
5. 模型完整结束后，在一个短事务内校验租约令牌、用户和会话状态，原子写入完整 Assistant 消息、`succeeded` 状态、幂等结果和允许的记忆任务。
6. 提交成功后释放生成资格并返回结果。失败、取消和租约丢失均记录可查询终态。

模型上下文仅使用此前成功回合的用户消息和完整回复，再加当前输入；失败或取消回合的用户消息保留在历史记录中，但不进入后续模型上下文。

### 4.2 流式请求

流式入口复用上述身份、幂等、租约、事务和模型路由。用户消息与生成标识提交后发送 `conversation.started`，随后发送 `message.delta` 和保活 `heartbeat`。完整回复成功提交后发送 `conversation.completed`，流开始后的错误通过 `conversation.failed` 表达。事件字段与恢复方式见 [SSE 契约](docs/backend-scaffold-design.md#84-sse-和结果恢复)。

每个业务事件携带生成回合内递增的事件 ID。增量仅在当前连接内传输，客户端通过生成查询或原幂等键恢复终态。客户端断开时尝试取消上游：若成功提交已完成则保留成功，否则收敛为 `cancelled`、丢弃部分输出并阻止完整回合记忆入队（依据见 [ADR-0004](docs/adr/0004-generation-idempotency-and-commit-boundary.md)）。

### 4.3 记忆写入

普通对话成功与记忆任务入队使用同一数据库事务。主进程内嵌的**后台任务执行器**通过统一 `jobs` 表执行至少一次投递，复用稳定任务 ID，使用提供方幂等或结果核验避免重复副作用。写入失败按指数退避重试，结果未知进入 `reconciling` 核验流程，达到上限留下可观测终态。

显式创建记忆在提供方同步确认并返回稳定单条 ID 时返回 `201`；若提供方仅受理异步抽取而尚未产生单条稳定 ID，返回 `202` 与可查询操作。单条删除和清空先持久化删除操作，确认完成返回 `204`，继续处理或结果待确认返回 `202`、操作 ID 和查询地址；清空要求 `confirm=true`。

## 5. 并发、幂等和删除一致性

### 5.1 生成租约

同一会话同时只有一个生成回合，不同幂等请求争用活跃会话返回 `409 conversation_generation_conflict`，不同会话可并行。

PostgreSQL 原子条件更新领取租约并持续续期。若新请求发现旧 `running` 回合租约已过期，在同一短事务内直接收敛为 `failed`（`generation_lease_lost`）后接管；进程内嵌任务循环的周期恢复任务负责无后续请求时的兜底收敛。数据库事务只覆盖短时状态变更，外部模型和记忆 HTTP 均在事务外执行。

### 5.2 聊天幂等

`POST /v1/chat` 必须携带 `Idempotency-Key`，唯一范围为 `(user_id, HTTP method, canonical route, key)`。业务摘要包含 Agent、传入的会话 ID 或缺省标记、消息和请求模型等语义参数，`stream` 作为传输偏好排除在摘要之外。

| 已有记录 | 重试结果 |
| --- | --- |
| `running` | `202`，返回原会话、生成 ID 和结果查询地址 |
| `succeeded` | `200 application/json`，返回已保存的完整结果，适用于任意 `stream` 偏好 |
| `failed`、`cancelled` | 返回原终态及稳定错误，重新生成需新键 |
| 同键不同业务摘要 | `409 idempotency_key_conflict` |

活跃幂等记录持续保留，终态记录从终态提交起保留 24 小时。到期后相同键按新请求处理，查询和重试均复查用户与资源状态。

### 5.3 删除屏障与记忆代次

用户与记忆提供方之间保存单调递增的记忆代次（依据见 [ADR-0005](docs/adr/0005-memory-epochs-and-deletion-barriers.md)）。清空操作原子提升代次、建立屏障、取消旧代次待执行任务，并暂停该范围的记忆读写；聊天可继续降级运行，显式记忆创建返回 `409 memory_scope_clearing`。

已发出的外部写入须核验收敛（缺少单次状态查询能力时在配置的上限收敛窗口 `memory_inflight_settle_timeout` 后保留审计告警并执行最终清空与延迟二次补偿清空），确认远程清空完成后再解除屏障。单条删除持久化 tombstone 后立即从召回过滤，历史引用保留元数据并标记 `deleted_at`。

### 5.4 会话和用户删除

会话删除立即置为 `deleting` 阻止续聊并从列表隐藏，内嵌后台任务执行器分批清理消息、引用、生成、相关幂等及该会话范围记忆；用户级记忆不受会话删除影响。

用户删除将本地 `users.status` 置为 `deleting` 并**立即吊销该用户的全部登录会话（`auth_sessions`）**，所有普通业务、登录和刷新请求立即被拒绝；删除期间继续保留 `users` 行占位（持有唯一 `email` 约束，防止清理完成前同邮箱重复注册），由后台任务执行器分批清理 Agent、人设版本、会话、消息、引用、生成、幂等记录及全部外部记忆。待本地与远程清理均确认完成后，物理移除 `users` 与认证凭据，仅保留脱敏操作摘要和删除回执摘要至有效期届满。

用户删除返回 `202` 和一次性随机删除回执。服务端仅保存 HMAC 摘要，客户端通过 `Authorization: DeletionReceipt <receipt>` 查询指定删除操作进度。

## 6. 数据模型

PostgreSQL 保存全部应用主数据、登录会话与可恢复后台任务。详细表结构、约束与索引见 [数据库设计](docs/backend-scaffold-design.md#6-数据库模型和规范)。

| 表或视图 | 领域用途 |
| --- | --- |
| `users` | 本地用户主体、唯一登录标识（`email`）、密码哈希、生命周期状态（`active`/`disabled`/`deleting`）及删除时间 |
| `auth_sessions` | 落库可撤销的刷新令牌会话（令牌摘要、过期时间、轮换与吊销状态） |
| `agents`、`persona_versions` | 私有 Agent、唯一草稿、不可变发布版本和当前发布版本 |
| `conversations`、`messages` | 会话归属、固定人设版本和原始有序消息 |
| `generations` | 每次生成的用户消息、实际模型、人设版本、状态、错误和租约 |
| `idempotency_records` | 用户范围的请求摘要、生成关联及终态到期时间 |
| `message_recalled_memories` | 实际注入模型的外部记忆引用及召回元数据 |
| `model_configs` | 受控同步的模型能力、启用状态及服务端凭据引用 |
| `jobs`、`job_attempts` | 统一持久任务、执行租约、重试和有限期执行摘要 |
| `memory_jobs` 只读视图 | 统一任务中记忆写入任务的领域查询视图 |
| `operations` | 删除与异步记忆操作的聚合进度、回执摘要与审计导出源 |
| `memory_scopes`、`memory_tombstones`、`memory_list_snapshots` | 用户与提供方记忆代次、屏障、待删除标记及多副本分页快照 |

> **现状衔接说明**：现有 V1 源码仍使用 `user_identities` 承接过渡期外部身份映射，待内置认证重构切片落地后切换至 `users` 本地凭据 + `auth_sessions` 目标结构。

### 6.1 召回记忆与对话消息的关联

`message_recalled_memories` 关联本次生成的原始用户消息，仅记录实际注入 Prompt 的记忆元数据（`provider_id`、`memory_id`、`scope`、`relevance`、`prompt_position`、`memory_epoch`、`recalled_at`、`deleted_at`），记忆正文由外部记忆提供方管理。

### 6.2 数据保留

具体保留期限以 [保留清理契约](docs/backend-scaffold-design.md#114-保留清理) 为准。内嵌后台任务执行器通过周期去重任务按有限批次、游标、租约和幂等操作执行到期清理（涵盖过期登录会话、24 小时终态幂等、30 天任务执行日志、短期分页快照及到期操作摘要），保留活跃工作、待核验外部结果与未完成删除映射。生产恢复采用“独立删除审计归档重放”或“PostgreSQL PITR”分层保障。

## 7. API 契约

业务接口统一位于 `/v1` 前缀下，聊天与 Agent 等业务载荷保持 `camelCase` 字段别名，当前用户从内置 Access Token 解析。

```http
POST /v1/chat
Authorization: Bearer <access_token>
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

| 资源 | 路由 |
| --- | --- |
| 内置认证（目标收敛） | `POST /v1/auth/register`、`POST /v1/auth/login`、`POST /v1/auth/refresh`、`POST /v1/auth/logout`、`POST /v1/auth/password` |
| 当前用户 | `GET /v1/users/me`、`DELETE /v1/users/me` |
| 聊天 | `POST /v1/chat` |
| 会话历史 | `GET /v1/conversations`、`GET /v1/conversations/{conversationId}`、`GET /v1/conversations/{conversationId}/messages` |
| 生成结果 | `GET /v1/conversations/{conversationId}/generations/{generationId}` |
| 会话删除 | `DELETE /v1/conversations/{conversationId}` |
| 私有 Agent | `POST /v1/agents`、`GET /v1/agents`、`GET /v1/agents/{agentId}`、`PATCH /v1/agents/{agentId}`、`DELETE /v1/agents/{agentId}` |
| 人设发布 | `POST /v1/agents/{agentId}/versions`、`POST /v1/agents/{agentId}/publish` |
| 记忆 | `GET /v1/memories`、`POST /v1/memories`、`DELETE /v1/memories/{memoryId}`、`DELETE /v1/memories?confirm=true` |
| 操作与删除状态 | `GET /v1/operations/{operationId}` |
| 模型 | `GET /v1/models` |
| 健康与受控指标 | `GET /health/live`、`GET /health/ready`、`GET /metrics` |

列表接口统一使用 `limit`、`cursor`、`items`、`next_cursor`、`has_more` 和 `(created_at, id)` 稳定游标排序，详见 [接口规范](docs/backend-scaffold-design.md#8-api-规范)。

## 8. 错误处理

所有 JSON 响应统一采用 `{success, code, message, data, request_id}` 外壳；错误响应中 `success` 为 `false`、`data` 为 `null`，并包含稳定的小写下划线 `code`、可读 `message`、`request_id` 和对象型 `details`。

| 场景 | 行为 |
| --- | --- |
| 认证失败、令牌过期或已吊销 | `401`，返回稳定认证错误码 |
| 用户已停用或删除中 | `403`，立即拒绝业务访问 |
| 模型失败或超时 | 生成 `failed`，返回稳定应用错误 |
| 召回失败或删除屏障生效 | 降级为人设、成功历史和当前输入 |
| 普通记忆任务失败 | 已提交回复保持成功，内嵌执行器按退避重试或转待核验 |
| 显式创建失败 | 返回稳定错误，成功须提供方确认 |
| 删除仍执行或结果未知 | `202` 并返回可查询操作 |
| 活跃会话争用或幂等摘要冲突 | `409`，使用不同错误码 |
| 其他用户资源 | 统一 `404`，隐藏存在性 |
| 未注册或已停用模型 | 稳定配置错误，上游调用前拒绝 |
| 流式输出中失败 | 记录终态，连接可写时发送失败事件 |

## 9. 安全与访问边界

认证模型采用单一、自包含的内置机制，不做外部 IdP/网关适配，也不引入角色与权限系统：

- **身份验证**：本地 `email` + 强哈希密码校验；签发短期 HS256/RS256 Access Token 与高熵可撤销 Refresh Token（仅存哈希摘要至 `auth_sessions`，支持轮换、单会话注销与按用户全量吊销）；注册接口支持配置开关控制是否开放自助注册。
- **访问隔离**：仅保留两道防线——**有效登录态（且 `users.status == 'active'`）** 与 **本人私有资源归属校验**。所有活跃用户平等访问服务端已启用的全部模型，不维护 `permissions` 或 `required_permission`。
- **删除回执**：`Authorization: DeletionReceipt <receipt>` 仅限查询单个删除操作进度，普通业务入口显式拒绝。
- **运维与安全基线**：模型同步与故障恢复由配置 + CLI（`models/sync.py`、`ops.py`）和受控 `/metrics`（内网白名单与可选运维令牌）承担；服务内置 CORS/Host/可信代理白名单、限流接缝、请求体与进程并发预算、日志凭据/正文脱敏、迁移版本校验与密钥轮换规范。

## 10. 整体演进与架构收敛路线

本服务明确**不拆分为多服务、不引入 Cordis 插件运行时**。所有新增能力（例如后续若需知识库、工具调用或多模态附件）均以本仓内新增领域模块的方式演进，共享同一进程、同一 PostgreSQL 连接与同一工程检查基线。

基于已全部落地的 V1 基础实现（18 个垂直切片，见 [实施规格](.scratch/ai-chat-memory/spec.md#implementation-plan)），向新定位目标体系收敛聚焦以下 3 项清晰重构：

1. **单进程任务内嵌收敛**：将 `workers/main.py` 独立进程装配迁入 FastAPI `application.py` `lifespan` 启动与优雅排空，移除独立 Worker 进程与独立探针端口，合并连接池与并发预算配置。
2. **人设边界内聚收敛（已完成，ticket 01）**：不可变快照类型与按拥有者读取最新/固定版本的服务入口已归入 `modules/agents/`；`conversations` 直接调用该公开服务，旧 `personas` 模块与 `PersonaProvider` 协议已移除。
3. **纯内置认证与权限裁剪收敛**：落地内置 `email` + 密码 + `auth_sessions` 认证闭环，移除 `trusted_gateway`、外部 `user_identities` 映射、`AuthenticationContext.permissions` 与 `model_configs.required_permission`。

## 11. 验证重点

- 内置注册、登录、令牌轮换、注销、停用/删除用户即时失效与跨用户 `404` 隔离。
- 私有 Agent 草稿隔离、不可变人设版本固定、并发发布与停用后旧会话续聊。
- 原始用户消息先提交，完整回复、生成成功与记忆任务原子提交，失败回合保留历史且从后续模型上下文过滤。
- 同会话生成租约竞争、过期内联接管、同键幂等回放与摘要冲突拦截。
- SSE 事件顺序、心跳、部分失败、断连取消与提交竞争得到唯一可查询终态。
- 单进程内嵌后台任务执行器的并发隔离、优雅停机排空、进程重启/多副本 `SKIP LOCKED` 恢复、最大重试与 `reconciling` 核验。
- 清空屏障期间旧写入、迟到外部响应、旧生成结束、会话与用户删除（含防同邮箱重注册）竞态收敛。
- 定期保留清理保护活跃工作与待核验映射，生产迁移、应用回滚及备份审计重放有可演练路径。
