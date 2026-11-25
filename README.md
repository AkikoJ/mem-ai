# mem-ai

一体化单进程 AI 对话应用 Python 后端服务（FastAPI + PostgreSQL 17 + SQLAlchemy 2.x + MemOS）。

项目不追求将内部模块拆分为独立微服务或插件运行时，而是以单一 Python 服务与 PostgreSQL 这一唯一必选基础设施，提供内置认证、私有 Agent 与人设版本、同步与 SSE 流式对话、长期记忆与删除屏障、内嵌持久后台任务、观测限流与恢复运维的完整后端体系。

核心设计入口：

- 总体定位与完整体系：[`DESIGN.md`](DESIGN.md)
- 工程目录、职责与收敛状态：[`docs/backend-scaffold-design.md`](docs/backend-scaffold-design.md)
- 领域术语表：[`GLOSSARY.md`](GLOSSARY.md)
- 一体化单进程架构决策：[`docs/adr/0007-unified-single-process-backend.md`](docs/adr/0007-unified-single-process-backend.md)
- 生产发布、恢复与运维手册：[`docs/operations/deployment-and-recovery.md`](docs/operations/deployment-and-recovery.md)

## 快捷命令（Makefile）

根目录提供 [`Makefile`](Makefile) 管理本地数据库、迁移、服务运行与静态检查：

```bash
make db-up          # 启动本地 PostgreSQL 容器并等待健康检查通过
make db-down        # 停止本地 PostgreSQL 容器（保留数据卷）
make migrate        # 执行 Alembic 升级到 head
make api            # 使用 .env 的 APP_HOST/APP_PORT 启动带 --reload 的本地 FastAPI 开发服务
make worker         # （V1 过渡入口，目标收敛为内嵌进主进程 lifespan）启动后台任务执行器
make ops-health     # 检查数据库就绪、任务积压、过期租约与记忆删除屏障
make ops-budget     # 校验服务副本的 PostgreSQL 连接总预算
make test           # 运行 pytest 测试集
make check          # 执行 Ruff lint/format、mypy 与架构检查
```

`make api` 和 `make api-prod` 会读取 `.env` 中的 `APP_HOST`、`APP_PORT`；未设置时默认使用 `127.0.0.1:8000`。终端已导出的变量或命令行参数优先级更高，例如 `make api APP_PORT=8848`。

生产容器构建（[`Dockerfile`](Dockerfile)）、发布迁移、备份恢复与故障处置见 [`docs/operations/deployment-and-recovery.md`](docs/operations/deployment-and-recovery.md)。

## 工程导航与检查

工程目录和功能定位索引位于 [`docs/backend-scaffold-design.md`](docs/backend-scaffold-design.md#3-工程目录)。开始实现前先按索引定位能力负责人和已有入口，完成后运行：

```bash
uv sync --locked --dev
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run alembic upgrade head
uv run alembic check
uv run pytest
python3 tools/check_architecture.py
```

`tools/check_architecture.py` 会校验已落地目录、功能索引重复路径、本地文档链接、分层导入方向与装配例外、重复字面量路由、ticket 依赖图一致性以及源码文件规模。

## 本地 PostgreSQL

安装并启动 Docker 和 Docker Compose，然后在项目根目录执行：

```bash
cp .env.example .env
```

编辑 `.env` 中的 `POSTGRES_PASSWORD`（如需自定义宿主机数据卷路径，可额外设置 `POSTGRES_DATA_DIR=/your/host/path`），再启动数据库：

```bash
docker compose up -d
```

数据库默认只监听本机的 `127.0.0.1:5432`，数据库名和用户名均为 `mem_ai`。如端口被占用，可修改 `.env` 中的 `POSTGRES_PORT`。未设置 `POSTGRES_DATA_DIR` 时数据保存在 Docker 命名卷 `postgres_data` 中，设置 `POSTGRES_DATA_DIR` 时直接绑定挂载至指定宿主机目录，普通的 `docker compose down` 均会保留数据。

查看状态与连接数据库：

```bash
docker compose ps
docker compose exec postgres psql -U mem_ai -d mem_ai
```

如果修改了 `.env` 中的用户名或数据库名，连接命令也需要使用对应的值。PostgreSQL 的初始化变量仅在数据卷首次初始化时生效，修改 `.env` 不会更改已有数据库中的用户、密码或数据库名。业务表由 Alembic 迁移管理。

## MemOS Cloud Skills

项目内安装了 [MemTensor 官方的两个 Skill](https://github.com/MemTensor/MemOS-Cloud-Skill/blob/main/README_zh.md)：

| Skill | 用途 |
| --- | --- |
| [`memos-cloud`](.agents/skills/memos-cloud/SKILL.md) | 给应用接入 MemOS Cloud，或排查 API / SDK 问题。 |
| [`memos-cloud-server`](.agents/skills/memos-cloud-server/SKILL.md) | 使用 Python CLI 搜索、写入、删除云记忆，提交反馈或管理知识库。 |

从仓库根目录打开客户端或新建会话。Codex、Cursor、GitHub Copilot、Gemini CLI 和 OpenCode 可以扫描 `.agents/skills/`；Claude Code 使用 `.claude/skills/` 下指向同一份 Skill 的相对符号链接。客户端若未显示 Skill，可让它读取根目录的 [`AGENTS.md`](AGENTS.md)，或直接指定上表中的 `SKILL.md`。不支持符号链接的检出环境也可使用这个方式。

仅安装不会调用云端 API。运行 `memos-cloud-server` 需要 Python 3、`requests`，以及由客户端进程继承的 `MEMOS_API_KEY` 和稳定的 `MEMOS_USER_ID`；不要将凭据写入仓库。默认只在用户明确请求时调用 MemOS Cloud。逐轮记忆需要单独启用。

上游快照：`b1007f74d431157f167e90b3ff78491e83b3a8ff`，按 [Apache 2.0](LICENSE) 使用。仓库版调整了云端调用和文档同步的触发条件；CLI 和随附参考资料来自上游。

## Matt Pocock Skills

已将 [mattpocock/skills](https://github.com/mattpocock/skills) 中当前可安装的 38 个 Skill 安装到 `.agents/skills/`，覆盖工程流程、代码评审、调试、测试、架构、研究和写作等场景。Claude Code 可通过 `.claude/skills/` 下的相对符号链接读取同一份文件。项目级规则见 [`AGENTS.md`](AGENTS.md)。`setup-matt-pocock-skills` 仅作为 Skill 安装，尚未运行它的项目配置流程。
