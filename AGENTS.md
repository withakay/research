<!-- ITO:START -->
<!--ITO:VERSION:0.1.31-->

# Ito Instructions

Use `@/.ito/AGENTS.md` as the source of truth when work involves planning/proposals, new capabilities, breaking or architectural changes, major performance/security work, or any ambiguous request that needs Ito workflow guidance.

Project setup: run `/ito-project-setup` (or `ito agent instruction project-setup`) until `.ito/project.md` contains `<!-- ITO:PROJECT_SETUP:COMPLETE -->`.

Files under `.ito/`, `.opencode/`, `.github/`, and `.codex/` are Ito-managed and may be overwritten. Put project-specific guidance in `.ito/user-prompts/guidance.md`, `.ito/user-prompts/<artifact>.md`, or below this block.

Keep this block so `ito init --upgrade` can refresh managed content safely. To refresh only this managed section, run `ito init --upgrade`.

## Path Helpers

Use `ito path ...` for runtime absolute paths; do not hardcode machine-specific paths into committed files:

- `ito path project-root`
- `ito path worktree-root`
- `ito path ito-root`
- `ito path worktrees-root`
- `ito path worktree --main|--branch <name>|--change <id>`

## Worktree Workflow


Worktrees are not configured for this project.

- Do NOT create git worktrees by default.
- Work in the current checkout unless the user explicitly requests a worktree workflow.


<!-- ITO:END -->

# AGENTS.md — guidance for AI coding agents

This file is the primary reference for any AI agent (GitHub Copilot, CLI
agents, autonomous coding bots, etc.) working inside the **research**
repository.

## Repository summary

| Property | Value |
|---|---|
| Type | Collection of unrelated mini-projects |
| Languages | Rust · Go · C# · TypeScript · Python (and others) |
| Structure | One concept per top-level folder |
| Purpose | Exploratory research, prototyping, small tools |

## Key conventions

* **No shared root-level libraries.** Each project folder is self-contained.
* **No enforced build system.** Each project may use `cargo`, `go build`,
  `dotnet`, `npm`/`pnpm`/`yarn`, or plain `python` — whatever fits.
* **No test coverage requirement.** Tests are welcome but not mandatory.
* **No release pipeline.** There are no CI/CD workflows at the repo level
  (individual projects may add their own).

## What agents should do

* When adding a new mini-project, create a **new top-level folder** with a
  descriptive, lowercase, hyphen-separated name (e.g. `rust-http-toy`).
* When editing an existing project, stay inside that project's folder unless
  changes to root-level files (`.gitignore`, `README.md`, etc.) are explicitly
  requested.
* Prefer the smallest possible change that satisfies the request.
* Do **not** add root-level package managers, monorepo tooling (Nx, Turborepo,
  Cargo workspaces spanning all projects, etc.) unless explicitly asked.
* Do **not** rename or reorganise existing project folders without explicit
  instruction.

## Known limitations & rough edges

This repository documents known issues and intentional shortcuts in
[CLAUDE.md](CLAUDE.md).  Agents should read that file before suggesting "fix"
changes to existing code, because some apparent flaws are deliberate.

## Copilot-specific notes

GitHub Copilot should also read `.github/copilot-instructions.md`, which
mirrors the most important points from this file in the format Copilot expects.
