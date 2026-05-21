# Tasks for: 001-14_productionize-ordering

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

## Wave 1

- **Depends On**: None

### Task 1.1: Define ordering lock protocols

- **Files**: durable-outbox-python/eva_durable_outbox/core/ordering.py
- **Action**: Define ordering coordinator and lock protocols.
- **Verify**: `uv run pytest`
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

### Task 1.2: Add fake lock tests

- **Files**: durable-outbox-python/tests/test_failover_ordering_cleanup.py
- **Action**: Implement fake lock client and contract tests.
- **Verify**: `uv run pytest`
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Implement Blob lock coordination

- **Files**: durable-outbox-python/eva_durable_outbox/stores/blob_geo.py
- **Action**: Implement Blob lock blob or lease coordination.
- **Verify**: `uv run pytest`
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

### Task 2.2: Add stale lock recovery tests

- **Files**: durable-outbox-python/tests/test_failover_ordering_cleanup.py
- **Action**: Add stale lock recovery tests.
- **Verify**: `uv run pytest`
- **Status**: [ ] pending
- **Updated At**: 2026-05-21
