# Tasks for: 001-13_expand-rpo0-failover-coverage

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

## Wave 1

- **Depends On**: None

### Task 1.1: Add partial-write repair matrix tests

- **Files**: durable-outbox-python/tests/test_adapters.py
- **Action**: Add partial-write repair matrix tests.
- **Verify**: `uv run pytest`
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

### Task 1.2: Hide prepared records from claims

- **Files**: durable-outbox-python/eva_durable_outbox/stores/blob_geo.py
- **Action**: Enforce prepared records are hidden from normal claims.
- **Verify**: `uv run pytest`
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Add replay predicate tests

- **Files**: durable-outbox-python/tests/test_failover_ordering_cleanup.py
- **Action**: Add replay predicate tests for PENDING, IN_FLIGHT, and SENT.
- **Verify**: `uv run pytest`
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

### Task 2.2: Add cleanup freeze lifecycle tests

- **Files**: durable-outbox-python/tests/test_failover_ordering_cleanup.py
- **Action**: Add cleanup freeze lifecycle tests.
- **Verify**: `uv run pytest`
- **Status**: [ ] pending
- **Updated At**: 2026-05-21
