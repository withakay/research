<!-- ITO:START -->
<!--ITO:VERSION:0.1.30-->

# Ito Instructions

These instructions are for AI assistants working in this project.

Always open `@/.ito/AGENTS.md` when the request:

- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/.ito/AGENTS.md` to learn:

- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Project setup: run `/ito-project-setup` (or `ito agent instruction project-setup`) until `.ito/project.md` is marked `<!-- ITO:PROJECT_SETUP:COMPLETE -->`.

Note: Files under `.ito/`, `.opencode/`, `.github/`, and `.codex/` are installed/updated by Ito (`ito init`, `ito update`) and may be overwritten.
Add project-specific guidance in `.ito/user-prompts/guidance.md` (shared), `.ito/user-prompts/<artifact>.md` (artifact-specific), and/or below this managed block.

Keep this managed block so `ito init --upgrade` can refresh the managed instructions non-destructively.
To refresh only the Ito-managed content in this file, run: `ito init --upgrade`

## Path Helpers

Use `ito path ...` to get absolute paths at runtime (do not hardcode absolute paths into committed files):

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
