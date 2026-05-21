# Tasks for: 001-04_add-blob-rpo0-failover

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

```bash
ito tasks status 001-04_add-blob-rpo0-failover
ito tasks next 001-04_add-blob-rpo0-failover
ito tasks start 001-04_add-blob-rpo0-failover 1.1
ito tasks complete 001-04_add-blob-rpo0-failover 1.1
```

______________________________________________________________________

## Wave 1

- **Depends On**: None

### Task 1.1: Add dual-region acceptance path

- **Files**: eva_durable_outbox/stores/blob_geo.py
- **Dependencies**: None
- **Action**: Write PREPARED records to both regions and mark both accepted before success.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Add dual-region acceptance path is implemented and covered by focused tests.
- **Requirements**: durable-outbox-blob-rpo0
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

### Task 1.2: Implement repair loop

- **Files**: eva_durable_outbox/stores/blob_geo.py
- **Dependencies**: Task 1.1
- **Action**: Detect and converge missing or PREPARED regional records.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Implement repair loop is implemented and covered by focused tests.
- **Requirements**: durable-outbox-blob-rpo0
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

______________________________________________________________________

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Implement failover replay and cleanup freeze

- **Files**: eva_durable_outbox/core/failover.py, stores/blob_geo.py
- **Dependencies**: None
- **Action**: Replay TTL-valid accepted records including SENT and freeze cleanup.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Implement failover replay and cleanup freeze is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-blob-rpo0
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

### Task 2.2: Add RPO=0 contract tests

- **Files**: tests/provider_contract/test_failover*.py, tests/stores/test_blob_rpo0*.py
- **Dependencies**: Task 2.1
- **Action**: Verify dual-write success boundary, partial repair, replay predicate, and cleanup freeze.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Add RPO=0 contract tests is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-blob-rpo0
- **Status**: [ ] pending
- **Updated At**: 2026-05-21
