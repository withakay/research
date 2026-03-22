# Copilot instructions for the research repository

## What this repository is

A collection of **unrelated mini-projects** committed here for quick, easy
storage while researching ideas, playing with concepts, and building small
tools.  Projects may be written in Rust, Go, C#, TypeScript, Python, or any
other language.  The root folder contains only other folders, each representing
one independent mini-project.

## How to work here

* Each top-level folder is a self-contained project — do **not** create shared
  root-level libraries or monorepo tooling unless explicitly asked.
* When adding a new project, create a new top-level folder with a descriptive,
  lowercase, hyphen-separated name.
* When editing an existing project, stay inside that project's folder unless
  root-level files need updating.
* Prefer the **smallest change** that satisfies the request.
* Do **not** rename or reorganise existing project folders without explicit
  instruction.

## Important files

| File | Purpose |
|---|---|
| `agents.md` | Full guidance for AI agents working in this repo |
| `flawed.md` | Known issues and intentional shortcuts — read before "fixing" things |
| `.gitignore` | Covers Rust, Go, C#, TypeScript, and Python build artefacts |

## Style

* No enforced code style across the repo — match the style of the project you
  are editing.
* Tests are welcome but not required.
* A short `README.md` inside each project folder is appreciated.
