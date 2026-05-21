# Tasks for: 001-08_add-operational-hardening

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

```bash
ito tasks status 001-08_add-operational-hardening
ito tasks next 001-08_add-operational-hardening
ito tasks start 001-08_add-operational-hardening 1.1
ito tasks complete 001-08_add-operational-hardening 1.1
```

______________________________________________________________________

## Wave 1

- **Depends On**: None

### Task 1.1: Add metrics and tracing hooks

- **Files**: durable_outbox/telemetry/*.py
- **Dependencies**: None
- **Action**: Implement metric names, labels, and tracing extension points.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Add metrics and tracing hooks is implemented and covered by focused tests.
- **Requirements**: durable-outbox-operations
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 1.2: Add status and admin service hooks

- **Files**: durable_outbox/core/status.py, failover.py
- **Dependencies**: Task 1.1
- **Action**: Expose state queries and controlled replay/repair operations.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Add status and admin service hooks is implemented and covered by focused tests.
- **Requirements**: durable-outbox-operations
- **Status**: [x] complete
- **Updated At**: 2026-05-21

______________________________________________________________________

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Write runbooks dashboards and alerts

- **Files**: durable-outbox-python/docs/operations/*.md
- **Dependencies**: None
- **Action**: Document failover, manual replay, alert triggers, and dashboard panels.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Write runbooks dashboards and alerts is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-operations
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 2.2: Add load and failure tests

- **Files**: tests/load/*, tests/failure_injection/*
- **Dependencies**: Task 2.1
- **Action**: Verify MVP throughput target and no-loss crash/failover scenarios.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Add load and failure tests is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-operations
- **Status**: [x] complete
- **Updated At**: 2026-05-21
