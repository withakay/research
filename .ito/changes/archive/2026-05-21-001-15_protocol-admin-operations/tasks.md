# Tasks for: 001-15_protocol-admin-operations

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

## Wave 1

- **Depends On**: None

### Task 1.1: Define admin protocols

- **Files**: durable-outbox-python/eva_durable_outbox/operations.py
- **Action**: Define status, admin action, and audit protocols.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 1.2: Remove fake-store admin dependency

- **Files**: durable-outbox-python/eva_durable_outbox/operations.py
- **Action**: Refactor admin service away from fake-store dependency.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Emit operational metrics

- **Files**: durable-outbox-python/eva_durable_outbox/**
- **Action**: Emit standard metrics from dispatcher, replay, cleanup, and admin paths.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 2.2: Add audit and payload-opacity tests

- **Files**: durable-outbox-python/tests/test_kafka_operations.py
- **Action**: Add audit and payload-opacity tests.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21
