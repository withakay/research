---
name: ito-using-git-worktrees
description: Use when starting feature work that needs isolation from current workspace or before executing implementation plans - creates isolated git worktrees with smart directory selection and safety verification
---

<!-- ITO:START -->
<!--ITO:VERSION:0.1.31-->

# Using Git Worktrees

Use isolated worktrees for change work so the main/control checkout stays clean.


Worktrees are not configured for this project.

- Do NOT create git worktrees by default.
- Work in the current checkout.
- Only use worktrees when the user explicitly requests that workflow.


## Integration

Called by any workflow that needs an isolated workspace.

<!-- ITO:END -->
