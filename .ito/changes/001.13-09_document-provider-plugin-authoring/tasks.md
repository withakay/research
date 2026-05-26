<!-- ITO:START -->
# Tasks for: 001.13-09_document-provider-plugin-authoring

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

```bash
ito tasks status 001.13-09_document-provider-plugin-authoring
ito tasks next 001.13-09_document-provider-plugin-authoring
ito tasks start 001.13-09_document-provider-plugin-authoring 1.1
ito tasks complete 001.13-09_document-provider-plugin-authoring 1.1
```

______________________________________________________________________

## Wave 1

- **Depends On**: None

### Task 1.1: Add Plugin Authoring Documentation Tests

- **Files**: `durable-outbox-python/tests/test_packaging_docs.py`
- **Dependencies**: None
- **Action**: Add failing documentation tests that require a plugin authoring
  guide to mention sink/store entry point groups, factory loading, local path
  installs, registry installs, and provider contract verification.
- **Verify**: `cd durable-outbox-python && uv run pytest tests/test_packaging_docs.py -q`
- **Done When**: Tests fail before the guide exists and pass once the guide is complete.
- **Requirements**: durable-outbox-plugin-authoring:plugin-authoring-guide, durable-outbox-plugin-authoring:plugin-installation-modes, durable-outbox-plugin-authoring:plugin-verification
- **Updated At**: 2026-05-26
- **Status**: [x] complete

### Task 1.2: Write Plugin Authoring Guide

- **Files**: `durable-outbox-python/docs/plugin-authoring.md`
- **Dependencies**: Task 1.1
- **Action**: Document sink and store plugin package structure, factory
  signatures, entry point metadata, configuration handling, registry install
  flow, local path install flow, and verification commands.
- **Verify**: `cd durable-outbox-python && uv run pytest tests/test_packaging_docs.py -q`
- **Done When**: The guide satisfies the documentation tests and gives copyable
  examples for both sink and store plugin packages.
- **Requirements**: durable-outbox-plugin-authoring:plugin-authoring-guide, durable-outbox-plugin-authoring:plugin-installation-modes, durable-outbox-plugin-authoring:plugin-verification
- **Updated At**: 2026-05-26
- **Status**: [x] complete

### Task 1.3: Link Guide From Consumer Docs

- **Files**: `durable-outbox-python/README.md`, `durable-outbox-python/docs/providers.md`
- **Dependencies**: Task 1.2
- **Action**: Link the plugin authoring guide from consumer-facing plugin
  documentation and provider documentation.
- **Verify**: `cd durable-outbox-python && uv run pytest tests/test_packaging_docs.py -q`
- **Done When**: Users can discover the guide from README and provider docs.
- **Requirements**: durable-outbox-packaging:consumer-documentation
- **Updated At**: 2026-05-26
- **Status**: [x] complete

______________________________________________________________________

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Run Documentation And Plugin Gates

- **Files**: `durable-outbox-python/docs/plugin-authoring.md`, `durable-outbox-python/tests/test_packaging_docs.py`, plugin loader tests
- **Dependencies**: None
- **Action**: Run focused docs and plugin verification commands.
- **Verify**: `cd durable-outbox-python && uv run pytest tests/test_packaging_docs.py tests/test_plugin_api.py tests/test_plugins.py -q`
- **Done When**: Focused documentation and plugin tests pass.
- **Requirements**: durable-outbox-plugin-authoring:plugin-authoring-guide, durable-outbox-plugin-authoring:plugin-installation-modes, durable-outbox-plugin-authoring:plugin-verification
- **Updated At**: 2026-05-26
- **Status**: [ ] pending

### Task 2.2: Run Full Quality Gates

- **Files**: `durable-outbox-python`
- **Dependencies**: Task 2.1
- **Action**: Run full package tests, lint, format check, type check, and
  workspace build.
- **Verify**: `cd durable-outbox-python && uv run pytest && uv run ruff check . && uv run ruff format --check . && uv run ty check && uv build --all-packages`
- **Done When**: All full quality gates pass.
- **Requirements**: durable-outbox-plugin-authoring:plugin-verification, durable-outbox-packaging:consumer-documentation
- **Updated At**: 2026-05-26
- **Status**: [ ] pending
<!-- ITO:END -->
