## Context
The repository intentionally avoids root-level monorepo tooling. This change confines the multi-project workspace to `durable-outbox-python`, where the split packages belong.

## Goals / Non-Goals
- Goals: use uv workspace membership for core and future provider packages; use `uv_build` for all pure Python packages; preserve current Python version and dev tooling.
- Non-Goals: create repository-root workspace tooling; extract providers in this change; publish packages.

## Decisions
- Decision: keep `durable-outbox-python/pyproject.toml` as both workspace root and the core package definition.
- Decision: set `[tool.uv.workspace] members = ["packages/*"]` before packages exist so later changes only add members.
- Decision: replace Hatchling with `uv_build` and configure `module-name = "durable_outbox"` / `module-root = ""` for the existing non-src layout.
- Alternative considered: create a separate workspace root package. Rejected because it adds an extra project layer without benefit for the PoC.

## Risks / Trade-offs
- `uv_build` defaults differ from Hatchling. Mitigate with build artifact tests that check README, license, package files, and typed marker inclusion.
- Workspace package additions will change lockfile content. Mitigate by using `uv sync --all-packages` in verification.

## Migration Plan
1. Update build backend and workspace metadata.
2. Regenerate `uv.lock` with the current dev group.
3. Update README and packaging tests.
4. Verify `uv build --all-packages` before downstream extraction changes.
