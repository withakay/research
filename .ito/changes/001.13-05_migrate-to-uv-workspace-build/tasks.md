## 1. Implementation
- [x] 1.1 Replace Hatchling build metadata with `uv_build` for the core package.
- [x] 1.2 Add project-local uv workspace metadata with `packages/*` members.
- [x] 1.3 Update README development commands for workspace sync and build.
- [x] 1.4 Update packaging tests for `uv_build` and workspace-aware commands.
- [x] 1.5 Regenerate `uv.lock`.

## 2. Verification
- [x] 2.1 Run `uv sync --all-packages --group dev` from `durable-outbox-python`.
- [x] 2.2 Run `uv run pytest` from `durable-outbox-python`.
- [ ] 2.3 Run `uv run ruff check .` and `uv run ruff format --check .`.
- [ ] 2.4 Run `uv run ty check`.
- [ ] 2.5 Run `uv build --all-packages`.
