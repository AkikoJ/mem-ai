# Issue tracker: Local Markdown

Issues and specs for this repo live as markdown files in `.scratch/`.

## Conventions

- One feature per directory: `.scratch/<feature-slug>/`
- The spec is `.scratch/<feature-slug>/spec.md`
- Implementation issues are one file per ticket at `.scratch/<feature-slug>/issues/<NN>-<slug>.md`, numbered from `01`
- Triage state is recorded as a `Status:` line near the top of each issue file
- Comments and conversation history append to the bottom of the file under a `## Comments` heading

## When a skill says "publish to the issue tracker"

Create a new file under `.scratch/<feature-slug>/`, creating the directory if needed.

## When a skill says "fetch the relevant ticket"

Read the file at the referenced path. The user will normally pass the path or issue number directly.

## Wayfinding operations

Used by `/wayfinder`.

- Map: `.scratch/<effort>/map.md`
- Child ticket: `.scratch/<effort>/issues/NN-<slug>.md`
- Each child ticket has a `Type:` line and a `Status:` line
- A `Blocked by:` line lists unresolved dependencies
- The frontier consists of open, unblocked and unclaimed tickets
- Claiming sets `Status: claimed`
- Resolving appends an `## Answer` section and sets `Status: resolved`

## 实现与完成记录

执行任务时，以当前工作树 spec 的 Implementation Plan 链接定位 ticket，核对其直接依赖及完成记录。先从 [工程目录与功能定位索引](../backend-scaffold-design.md#30-ai-导航入口) 定位唯一负责模块和已有入口，再进入 ticket 的实现边界。`ready-for-agent` 表示规格可承接，执行前仍需确认依赖已完成。完成情况以当前代码、验证证据、目录索引和 ticket 的 `## Answer` 共同核验。

工程目录是实现导航的核心记录。新增功能、公共符号或协作文件前，先判断功能定位索引中是否已有负责人。已有负责人时扩展该入口并迁移调用方，需要新增负责人或改变模块归属时，在同一变更中更新目录树、功能索引、职责表和相关引用。单独新增一个相似文件不能代替归属判断。

每张实现 ticket 使用 `## 实现边界` 记录复用对象、本次扩展入口和职责分工。这里引用工程设计中的公共约定，实际符号尚未落地时按模块职责定位，首个实现切片交付后补充具体路径。

开始实现前搜索相关符号、路由、表、错误码和测试，阅读已有调用方。发现已有能力时扩展同一入口，需要替换实现时迁移当前调用方并清理旧路径。

完成实现后先运行 `python3 tools/check_architecture.py`，再追加 `## Answer`，记录下列内容，并按现有状态约定收敛任务。

- 实际公共符号和文件路径，以及后续 ticket 的调用方式。
- 复用或迁移的已有实现。确需新增同类实现时记录不同契约和具体原因。
- 执行过的检查及结果，未验证项目与剩余技术依赖。
- 涉及的职责、接口、目录和文档同步情况。

仅修改规格或规则时记录对应文档变更，任务实现状态随实际功能交付推进。
