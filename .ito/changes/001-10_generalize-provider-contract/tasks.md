# Tasks for: 001-10_generalize-provider-contract

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

## Wave 1

- **Depends On**: None

### Task 1.1: Introduce contract fixture types

- **Files**: durable-outbox-python/eva_durable_outbox/testing/provider_contract.py
- **Action**: Introduce generic provider contract fixture types.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 1.2: Remove fake-store type checks

- **Files**: durable-outbox-python/eva_durable_outbox/testing/provider_contract.py
- **Action**: Run shared contract through the DurableOutboxStore protocol only.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Add optional provider hooks

- **Files**: durable-outbox-python/eva_durable_outbox/testing/provider_contract.py
- **Action**: Add optional hooks for cleanup, stale reclaim, failure injection, and status inspection.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 2.2: Run contract against stores

- **Files**: durable-outbox-python/tests/provider_contract/**
- **Action**: Run contract against fake store and adapter fake provider clients.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21
