# Issue tracker：GitHub

本仓库的问题与 PRD 使用 GitHub Issues 管理，使用 `gh` CLI 完成读写。

## 约定

- 创建 issue：`gh issue create --title "..." --body "..."`
- 查看 issue：`gh issue view <编号> --comments`
- 列出 issue：使用 `gh issue list`，并按状态、标签和 JSON 字段筛选
- 评论 issue：`gh issue comment <编号> --body "..."`
- 添加或移除标签：`gh issue edit <编号> --add-label "..."` 或 `--remove-label "..."`
- 关闭 issue：`gh issue close <编号> --comment "..."`

在仓库克隆目录中运行 `gh` 时，由远端自动推断仓库。

## Pull Request 作为 triage 入口

关闭。外部 Pull Request 不进入本仓库的 triage 队列。

## 工程 skill 的发布约定

当 skill 要求发布到 issue tracker 时，创建 GitHub issue；当要求获取相关 ticket 时，运行 `gh issue view <编号> --comments`。

## Wayfinding 约定

Wayfinding map 使用一个带有 `wayfinder:map` 标签的 issue；子任务使用关联的 GitHub sub-issue，或在不支持 sub-issue 时在正文中写明所属 map。阻塞关系优先使用 GitHub 原生 issue dependency。认领、解决和前沿查询按 Wayfinding skill 的约定执行。
