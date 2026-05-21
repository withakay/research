# Tasks for: 001-16_add-failure-load-tests

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

## Wave 1

- **Depends On**: None

### Task 1.1: Add failure injection paths

- **Files**: durable-outbox-python/eva_durable_outbox/testing/**
- **Action**: Add failure injection for mark-sent, put, claim, and replay paths.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 1.2: Add ack-before-mark-sent test

- **Files**: durable-outbox-python/tests/**
- **Action**: Add ack-before-mark-sent no-loss test.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Add throughput test

- **Files**: durable-outbox-python/tests/**
- **Action**: Add fast fake-provider throughput test and pytest markers.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 2.2: Document verification commands

- **Files**: durable-outbox-python/docs/**
- **Action**: Document how to run slow and integration verification.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21
