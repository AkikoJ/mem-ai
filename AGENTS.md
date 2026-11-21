# 开发流程

1. 先读取 [工程目录与功能定位索引](docs/backend-scaffold-design.md#3-工程目录)，确认目标功能的唯一负责模块、计划入口、相关测试和当前落地状态，再读取对应 spec、ticket 及 ADR。
2. 在目录索引指定的负责模块内，用 `rg` 搜索路由、符号、表、错误码、测试和调用方。索引没有对应能力时，先补充模块归属和入口，再开始实现。
3. 修改模块或公共接口前，读取 [工程职责与所有权](docs/backend-scaffold-design.md#2-架构和职责)。每个资源、业务状态转换和事务阶段都有唯一负责入口，跨模块通过该入口协作。
4. 同步、流式、取消和恢复流程共同使用 [生成生命周期](docs/backend-scaffold-design.md#54-共享生成生命周期)。外部协议适配、业务状态和通用任务执行按模块所有权分别实现。
5. 按当前切片交付已需要的能力，后续切片继续扩展目录索引中已有入口。按职责变化拆分文件，提取共享实现时一并迁移当前调用方，删除被替代的旧路径。
6. 交付前核对重复实现、依赖方向、原子提交、目录索引和实际文件状态，优先运行 `python3 tools/check_architecture.py` 及范围内允许的检查，并按 [任务完成记录](docs/agents/issue-tracker.md#实现与完成记录) 记录实际入口、验证结果及剩余限制。

# Project Skills

- **Backend design and directory maintenance:** Read [.agents/skills/backend-design/SKILL.md](.agents/skills/backend-design/SKILL.md) when designing the backend or adding, updating, moving, renaming, or deleting backend files, configuration, tests, workflows, or design documents. Use it to check whether the annotated engineering directory needs to be synchronized.
- **Git commit messages and branch names:** Read [.agents/skills/commit-message-helper/SKILL.md](.agents/skills/commit-message-helper/SKILL.md) when generating, reviewing, or rewriting Git commit messages or branch names.

- **Matt Pocock skills:** Browse [.agents/skills/](.agents/skills/) and read the relevant `SKILL.md` when its description matches the task. This collection is from [mattpocock/skills](https://github.com/mattpocock/skills).

- **MemOS Cloud integration:** Read [.agents/skills/memos-cloud/SKILL.md](.agents/skills/memos-cloud/SKILL.md) when integrating MemOS Cloud into an application or troubleshooting its API or SDK.
- **MemOS Cloud memory operations:** Read [.agents/skills/memos-cloud-server/SKILL.md](.agents/skills/memos-cloud-server/SKILL.md) when the user requests a cloud memory or knowledge-base operation, or has explicitly enabled MemOS Cloud memory for this chat.

Skill discovery is not permission to send data to MemOS Cloud. Keep credentials and unrelated conversation or repository content out of cloud requests.

## Agent skills

### Issue tracker

Issues and specs live as local Markdown files under `.scratch/`. See `docs/agents/issue-tracker.md`.

### Triage labels

This repo uses the default triage labels `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, and `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

This is a single-context repo with `GLOSSARY.md` at the root and ADRs under `docs/adr/`. See `docs/agents/domain.md`.
