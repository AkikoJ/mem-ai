# mem-ai
Basic MemOS AI memory system

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
