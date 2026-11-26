# 05: 切换用户删除防重注册与模型无权限路由至新契约（Migrate 核心业务）

**What to build:** 将用户删除流程切换为立即吊销全部登录会话并在清理完成前保留用户记录占位以阻止同邮箱重复注册，扩展当前用户信息返回邮箱，并移除模型发现与模型路由对模型权限字段的过滤，使所有活跃用户平等使用服务端已启用的全部模型。

**Blocked by:** [04 扩展本地用户凭据与登录会话并交付内置认证接口（Expand）](04-add-builtin-auth-and-login-sessions.md)

**Status:** resolved

**Parent:** [一体化单进程 Python 后端体系收敛规格](../spec.md)

## 实现边界

- 复用 04 的本地用户凭据与 `auth_sessions` 登录会话能力，在 `users` 删除受理事务中同步置用户状态为 `deleting` 并批量吊销其全部活跃登录会话，保持 `users` 行占位直至本地数据与外部记忆清理全部确认完成后再物理删除用户与会话记录；确保备份恢复审计重放同样吊销会话并收敛删除。
- 扩展 `GET /v1/users/me` 响应返回用户的 `email` 信息（保留既有字段与统一响应外壳）。
- 更新 `models` 服务与路由以及 `conversations` 模型选择调用方，移除按用户权限集合与 `required_permission` 过滤模型的逻辑，保留服务端强制模型策略、是否允许请求覆盖模型开关、启用状态与流式能力校验。

## 验收条件

- [x] 发起 `DELETE /v1/users/me` 后，该用户全部 `auth_sessions` 立即失效，现有 Access Token、Refresh Token 与重新登录全部被拒绝，且删除清理完成前使用同一邮箱重新注册返回 `409`。
- [x] 删除进行中仍可凭一次性删除回执查询操作进度，但普通业务接口拒绝该回执；待本地与远程记忆清理确认完成后，用户记录与会话被物理移除，此后同一邮箱方可作为全新用户重新注册。
- [x] 备份恢复审计重放遇到用户删除事件时，同样立即吊销登录会话并阻断业务访问，保持防复活一致性。
- [x] `GET /v1/users/me` 返回当前登录用户的标识、邮箱与状态。
- [x] `GET /v1/models` 与 `POST /v1/chat` 不再因权限集合拒绝已启用的注册模型，所有活跃用户平等访问全部已启用模型；未注册或已停用模型仍在调用外部模型前返回稳定错误。
- [x] Ruff、mypy、新增与既有测试以及 `python3 tools/check_architecture.py` 全部通过。


## Answer

已于 2026-11-26 完成。直接依赖 04 的内置认证与 `auth_sessions` 已在基线 `ffdb1eb` 落地，本切片在该实现上扩展，无新增迁移。

### 实际入口与协作

- `src/mem_ai/modules/users/cleanup.py`：`delete_user_for_owner` 与 `replay_user_deletion_for_recovery` 共享删除受理事务，在锁定用户后调用 `auth.service.revoke_user_sessions_in_session`，与 `deleting` 状态、删除操作及任务一同提交；恢复时用户已处于 `deleting` 也补做幂等会话吊销。
- `src/mem_ai/modules/auth/service.py`：`login_user` 签发事务持有用户共享锁；`refresh_tokens` 先定位不可变用户归属，再在写事务中按用户→登录会话顺序加锁并重读，避免并发签发逃过删除吊销或与删除发生死锁，同时保留刷新重放吊销家族及 `401` 契约。
- 复用既有 `UserCleanupJobHandler` 和 `users.repository.delete_user_and_identities` 最终阶段：本地与所有远程提供方清理确认前保留用户邮箱占位，完成后物理删除用户并通过 04 已有外键级联移除登录会话。同邮箱此后注册得到全新用户 ID；原删除回执继续查询脱敏操作。
- `src/mem_ai/modules/users/schemas.py` 的 `UserMeResponse` 与 `router.py` 的 `get_current_user` 新增 `email`，保留原字段与统一响应外壳；历史外部身份用户的邮箱为 `null`。
- `src/mem_ai/modules/models/service.py` 的 `list_available_models`、`resolve_model_for_chat` 移除用户权限参数与权限过滤；`models/router.py` 和 `conversations` 的 `ChatOptions`、`chat_options_from_settings`、`prepare_generation` 及路由调用方同步迁移，继续执行强制模型、请求覆盖开关、注册/启用、流式和能力校验。

### 验证

- 红→绿验证：删除成功后会话未吊销、当前用户邮箱缺失、普通用户模型发现/选型被旧权限拦截；并发登录原实现留下新会话、并发刷新原实现出现死锁，修复后对应回归通过。
- `tests/api/test_builtin_auth.py` 覆盖邮箱响应、全部会话立即吊销、Access/Refresh/登录拒绝、规范化邮箱冲突和回执业务隔离。
- `tests/integration/test_auth_session_deletion_races.py` 通过真实 PostgreSQL 锁与公开登录/刷新 HTTP 入口验证删除并发边界。
- `tests/integration/test_user_deletion.py` 通过应用内嵌执行器单步验证远程待确认期间保留邮箱，最终物理删除用户与会话后重新注册。
- `tests/integration/test_production_deployment_and_recovery.py` 验证恢复的 active/disabled/deleting 用户会话均被审计重放吊销，重复重放不重复入队。
- 模型 API/集成测试覆盖纯内置注册用户平等发现和选用已启用模型，保留旧 claims 差异、模型缺失/停用、强制/覆盖和能力限制回归。
- `python3 tools/check_architecture.py` 通过；`.venv/bin/ruff check .` 与 `.venv/bin/ruff format --check .` 通过；`.venv/bin/mypy src` 通过（119 个源文件）。
- `.venv/bin/pytest -q --tb=short --show-capture=no` 全量通过：**289 passed in 91.85s**。测试使用本机隔离 PostgreSQL 数据库；未执行构建或真实外部模型/记忆服务调用。
- 按 `code-review` 对基线 `ffdb1eb` 之后的变更完成两路独立只读审查：Standards 与 Spec 均无待修复问题。

### 文档与后续边界

已同步 `docs/backend-scaffold-design.md` 的模块所有权、当前状态、功能定位索引、目录注释、职责表和删除/模型契约。会话吊销由 `auth` 唯一负责，删除事务仍由 `users` 协调，模型策略仍由 `models` 负责。

06 继续迁移全量测试的认证接缝；07 再移除外部认证适配器、上下文权限、`user_identities` 与 `model_configs.required_permission` 结构。本切片保留这些过渡结构，但模型路由已不使用权限字段。
