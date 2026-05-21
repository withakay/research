# Tasks for: 001-12_improve-retry-metadata

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

## Wave 1

- **Depends On**: None

### Task 1.1: Add attempt count to claims

- **Files**: durable-outbox-python/eva_durable_outbox/core/model.py, durable-outbox-python/eva_durable_outbox/testing/fake_store.py
- **Action**: Add attempt count to claim metadata and fake store claims.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 1.2: Use attempt count in dispatcher

- **Files**: durable-outbox-python/eva_durable_outbox/core/dispatcher.py
- **Action**: Use attempt count in retry scheduling.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Add retry cap tests

- **Files**: durable-outbox-python/tests/test_core.py
- **Action**: Add retry cap and repeated-failure tests.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 2.2: Update provider contract

- **Files**: durable-outbox-python/eva_durable_outbox/testing/provider_contract.py
- **Action**: Update provider contract expectations for attempt metadata.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21
