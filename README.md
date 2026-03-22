# research

A grab-bag repository for code produced while researching ideas, playing with
concepts, and building small tools.  It is intentionally **not** a polished
product; do not expect consistent structure, build systems, or release
pipelines.

## What lives here

Each top-level folder is an independent mini-project.  Projects may be written
in **Rust, Go, C#, TypeScript, Python**, or anything else that seemed
appropriate at the time.  There is no enforced language, framework, or folder
convention — the only rule is "one concept per folder".

## Purpose

* Quickly commit and save exploratory code without over-engineering it.
* Serve as a scratchpad for proofs-of-concept and prototypes.
* Preserve small utilities that are useful but do not warrant their own repo.

This is **not** a place for production code.  Expect rough edges, incomplete
tests, and abandoned experiments.

## For AI agents & automated tooling

See [agents.md](agents.md) for conventions and guidance specifically aimed at
AI coding agents (GitHub Copilot, etc.) working inside this repository.

## Contributing / working in this repo

1. Create a new top-level folder for your mini-project.
2. Keep the project self-contained — do not add shared libraries at the root.
3. A short `README.md` inside each project folder is appreciated but not
   required.
4. Check `.gitignore` — common build artefacts for Rust, Go, C#, TypeScript,
   and Python are already excluded.
