# Tasks for: 001-17_polish-package-docs

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

## Wave 1

- **Depends On**: None

### Task 1.1: Expand README

- **Files**: durable-outbox-python/README.md
- **Action**: Expand README with usage, extras, and verification commands.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 1.2: Add provider docs and license

- **Files**: durable-outbox-python/docs/**, durable-outbox-python/LICENSE
- **Action**: Add RPO=0 provider documentation and license file.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Add packaging build verification

- **Files**: durable-outbox-python/pyproject.toml
- **Action**: Add packaging/build verification.
- **Verify**: `uv build`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 2.2: Keep docs consistent with project config

- **Files**: durable-outbox-python/README.md, durable-outbox-python/pyproject.toml
- **Action**: Ensure docs remain consistent with pyproject configuration.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21
