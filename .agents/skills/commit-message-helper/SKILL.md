---
name: commit-message-helper
description: Use when generating, reviewing, or rewriting Git commit messages, branch names, Conventional Commits subjects, bug-fix commit bodies, or Chinese commit summaries for a code change.
---

# Commit Message Helper

## Overview

Generate Git branch names and commit messages that follow the shared convention below. Prefer reading `git status`, `git diff --stat`, and the relevant diff before drafting, unless the user already supplied enough context.

## Output Contract

When asked for a commit message or branch name, output:

- invocation proof as the fixed first line:
  ```text
  已调用 $commit-message-helper：以下内容按团队 Git 提交规则生成。
  ```
- recommended branch name
- commit subject
- commit body when required or useful

Always put the invocation proof before the branch name. Keep the answer concise. Do not invent verification details.

## Commit Subject

Use this exact first-line format:

```text
type(scope): 中文标题
```

- Allowed `type` values: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`.
- Require `scope` for every manual commit.
- Write `scope` in lowercase English kebab-case, such as `scada-editor`, `color-picker`, `auth`, or `redis-config`.
- Write the subject title in Chinese.
- Use a normal ASCII colon after the scope. Never use `fix：...` or other Chinese-colon variants.
- Keep issue or requirement IDs out of the subject by default.
- Treat platform-generated merge and revert commits as exempt.

## Commit Body

For `fix`, always include this body:

```text
BUG原因：...
修复方式：...
```

Do not include `验证方式`.

For non-`fix` commits, body is optional. If useful, prefer:

```text
变更内容：...
```

## Branch Name

Use:

```text
type/english-kebab-case-summary
```

- Use the same `type` values as commit subjects.
- Write the summary in lowercase English kebab-case.
- Include issue or requirement IDs only in the branch name when useful, such as `fix/d48-pivot-sync`.

## Examples

```text
已调用 $commit-message-helper：以下内容按团队 Git 提交规则生成。

Branch: fix/button-press-release-state

fix(scada-editor): 修复按钮按压释放后状态不同步

BUG原因：按钮释放逻辑只更新了视觉反馈，未同步切换后的状态值。
修复方式：在释放处理流程中统一更新按钮状态，并调整按压反馈触发时机。
```

```text
已调用 $commit-message-helper：以下内容按团队 Git 提交规则生成。

Branch: feat/color-picker-eyedropper

feat(color-picker): 新增吸管取色预览
```

```text
已调用 $commit-message-helper：以下内容按团队 Git 提交规则生成。

Branch: refactor/redis-set-key-value

refactor(redis-config): 简化配置项保存流程

变更内容：合并重复的参数转换逻辑，统一保存入口。
```
