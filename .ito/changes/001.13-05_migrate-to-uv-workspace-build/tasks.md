## 1. Implementation
- [ ] 1.1 Replace Hatchling build metadata with `uv_build` for the core package.
- [ ] 1.2 Add project-local uv workspace metadata with `packages/*` members.
- [ ] 1.3 Update README development commands for workspace sync and build.
- [ ] 1.4 Update packaging tests for `uv_build` and workspace-aware commands.
- [ ] 1.5 Regenerate `uv.lock`.

## 2. Verification
- [ ] 2.1 Run `uv sync --all-packages --group dev` from `durable-outbox-python`.
- [ ] 2.2 Run `uv run pytest` from `durable-outbox-python`.
- [ ] 2.3 Run `uv run ruff check .` and `uv run ruff format --check .`.
- [ ] 2.4 Run `uv run ty check`.
- [ ] 2.5 Run `uv build --all-packages`.
