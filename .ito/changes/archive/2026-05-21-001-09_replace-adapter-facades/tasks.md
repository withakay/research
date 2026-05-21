# Tasks for: 001-09_replace-adapter-facades

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

## Wave 1

- **Depends On**: None

### Task 1.1: Define provider client protocols

- **Files**: durable-outbox-python/durable_outbox/stores/**
- **Action**: Define internal provider client protocols for Blob, Cosmos, and SQL.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 1.2: Remove fake-store inheritance

- **Files**: durable-outbox-python/durable_outbox/stores/**
- **Action**: Make production adapters implement durable store behavior directly.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Implement provider lifecycle paths

- **Files**: durable-outbox-python/durable_outbox/stores/**
- **Action**: Implement provider-specific put, claim, retry, sent, replay, and cleanup paths.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 2.2: Add provider adapter tests

- **Files**: durable-outbox-python/tests/**
- **Action**: Add fake provider clients and adapter contract tests.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21
