# Tasks for: 001-03_implement-blob-store-mvp

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

```bash
ito tasks status 001-03_implement-blob-store-mvp
ito tasks next 001-03_implement-blob-store-mvp
ito tasks start 001-03_implement-blob-store-mvp 1.1
ito tasks complete 001-03_implement-blob-store-mvp 1.1
```

______________________________________________________________________

## Wave 1

- **Depends On**: None

### Task 1.1: Implement blob serialization and naming

- **Files**: eva_durable_outbox/stores/blob_geo.py
- **Dependencies**: None
- **Action**: Create deterministic paths and encode/decode event records.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Implement blob serialization and naming is implemented and covered by focused tests.
- **Requirements**: durable-outbox-blob-store
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 1.2: Implement put and claim transitions

- **Files**: eva_durable_outbox/stores/blob_geo.py
- **Dependencies**: Task 1.1
- **Action**: Use idempotent create and ETag conditional metadata updates.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Implement put and claim transitions is implemented and covered by focused tests.
- **Requirements**: durable-outbox-blob-store
- **Status**: [x] complete
- **Updated At**: 2026-05-21

______________________________________________________________________

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Implement retry, stale reclaim, and cleanup

- **Files**: eva_durable_outbox/stores/blob_geo.py
- **Dependencies**: None
- **Action**: Add lifecycle methods required by DurableOutboxStore.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Implement retry, stale reclaim, and cleanup is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-blob-store
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 2.2: Run provider contract subset

- **Files**: tests/stores/test_blob*.py
- **Dependencies**: Task 2.1
- **Action**: Verify non-RPO=0 Blob behavior against provider contract excluding geo RPO=0 tests.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Run provider contract subset is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-blob-store
- **Status**: [x] complete
- **Updated At**: 2026-05-21
