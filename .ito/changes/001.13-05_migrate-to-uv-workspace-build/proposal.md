# Change: Migrate Durable Outbox To Uv Workspace Build

## Why
The durable outbox PoC is about to split provider implementations into separate packages. A project-local uv workspace gives those packages shared development commands and lockfile management without adding repository-wide monorepo tooling.

## What Changes
- Configure `durable-outbox-python` as a uv workspace with package members under `packages/*`.
- Migrate the existing core `durable-outbox` package from Hatchling to `uv_build`.
- Keep workspace tooling scoped to `durable-outbox-python` and leave the repository root unchanged.
- Update README and packaging tests to use `uv sync --all-packages`, `uv build --all-packages`, and workspace-aware verification.

## Impact
- Affected specs: `durable-outbox-packaging`
- Affected code: `durable-outbox-python/pyproject.toml`, `durable-outbox-python/uv.lock`, `durable-outbox-python/README.md`, packaging tests
- Sequencing: implement before plugin extraction packages so later changes can add workspace members.
