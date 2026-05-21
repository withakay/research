---
name: ito-using-git-worktrees
description: Use when starting feature work that needs isolation from current workspace or before executing implementation plans - creates isolated git worktrees with smart directory selection and safety verification
---

<!-- ITO:START -->
<!--ITO:VERSION:0.1.30-->

# Using Git Worktrees

## Overview

Git worktrees create isolated workspaces that share the same repository, allowing work on multiple branches simultaneously.


Worktrees are not configured for this project.

- Do NOT create git worktrees by default.
- Work in the current checkout.
- Only use worktrees when the user explicitly requests that workflow.


## Integration

**Called by:**
- Any workflow that needs isolated workspace

<!-- ITO:END -->
