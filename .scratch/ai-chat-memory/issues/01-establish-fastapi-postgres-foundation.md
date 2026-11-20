# 01: 建立 FastAPI 与 PostgreSQL 基础

**What to build:** 让应用使用测试配置启动，连接 PostgreSQL、执行迁移并通过公开健康接口验证，为业务切片提供可替换的提供方和测试入口。

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

**Parent:** [Mem-AI v1 规格](../spec.md)

## 实现边界

- 建立应用装配、项目协议、公共分页与错误处理，后续业务切片复用这些入口。
- 本切片建立 Ruff、mypy、pytest 和 CI 的最小执行链，先接入 `python3 tools/check_architecture.py`，再按工程设计第 13.4 节将检查扩展到已落地模块。
- 业务实现按后续切片逐步增加，计划目录随实际文件更新。

## 验收条件

- [ ] `create_app()`、生命周期及 API 进程入口可用，资源在停机时释放，配置在启动时校验。
- [ ] SQLAlchemy 异步 Session 与 psycopg、连接池和 Alembic 同步迁移可用，外部调用在事务外执行。
- [ ] 空 PostgreSQL 数据库能够升级，元数据漂移检查能够发现缺失迁移。
- [ ] 存活和数据库就绪接口、请求 ID、JSON 日志及嵌套 error 信封可通过 HTTP 测试验证，错误码为稳定小写下划线。
- [ ] 项目拥有 ModelProvider、MemoryProvider 和 PersonaProvider 的类型化装配边界，依赖覆盖可替换提供方。
- [ ] fixture 可以创建应用、PostgreSQL、HTTP 客户端和受控提供方，隔离测试数据并清理资源。
- [ ] Ruff、mypy、pytest、架构与文档检查具备最小执行入口，CI 对当前已落地范围运行这些检查，后续切片扩展同一流水线。
- [ ] 目录注释、文件职责与实际新增文件同步。
