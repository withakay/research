<!-- ITO:START -->
<!--ITO:VERSION:0.1.30-->

<!-- ITO:PROJECT_SETUP:COMPLETE -->

# Project Context

## Purpose

Collection of unrelated mini-projects for exploratory research, prototyping, and small tools.

## Tech Stack

- Mixed language repository. Individual top-level project folders may use Rust, Go, C#, TypeScript, Python, or other local stacks.
- There is no shared root-level build system or package manager.

## Project Conventions

### Code Style

- Follow the conventions inside the specific mini-project being edited.
- When adding a new mini-project, create a descriptive lowercase kebab-case top-level folder.
- Prefer the smallest possible change that satisfies the request.

### Architecture Patterns

- No shared root-level libraries.
- Each top-level project folder is self-contained.
- Do not add monorepo tooling, cross-project workspaces, or root package managers unless explicitly requested.

### Testing Strategy

- No repository-wide coverage requirement.
- Use the build, test, lint, and formatting commands appropriate to the affected mini-project.
- Add focused tests when risk or behavior changes justify them.

### Git Workflow

- Work in the current checkout by default.
- Worktrees are not configured for this project; do not create them unless explicitly requested.
- Keep unrelated project folders untouched.

## Domain Context

This repository is intentionally heterogeneous. Apparent root-level absence of CI, shared dependencies, or unified tooling is expected.

## Important Constraints

- When editing an existing project, stay inside that project's folder unless root-level changes are explicitly requested.
- Do not rename or reorganize existing project folders without explicit instruction.
- Read `CLAUDE.md` before suggesting broad fixes to existing code because it may document intentional shortcuts.

## External Dependencies

External dependencies are project-specific and should be documented inside the relevant mini-project.

<!-- ITO:END -->
