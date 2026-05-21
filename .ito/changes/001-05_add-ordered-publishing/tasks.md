# Tasks for: 001-05_add-ordered-publishing

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

```bash
ito tasks status 001-05_add-ordered-publishing
ito tasks next 001-05_add-ordered-publishing
ito tasks start 001-05_add-ordered-publishing 1.1
ito tasks complete 001-05_add-ordered-publishing 1.1
```

______________________________________________________________________

## Wave 1

- **Depends On**: None

### Task 1.1: Define ordering interfaces and validation

- **Files**: eva_durable_outbox/core/ordering.py, model.py
- **Dependencies**: None
- **Action**: Add coordinator protocol and ordered event validation.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Define ordering interfaces and validation is implemented and covered by focused tests.
- **Requirements**: durable-outbox-ordering
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

### Task 1.2: Implement ordered dispatcher flow

- **Files**: eva_durable_outbox/core/dispatcher.py
- **Dependencies**: Task 1.1
- **Action**: Ensure same-key sequencing and different-key concurrency.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Implement ordered dispatcher flow is implemented and covered by focused tests.
- **Requirements**: durable-outbox-ordering
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

______________________________________________________________________

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Implement Blob key leases

- **Files**: eva_durable_outbox/stores/blob_geo.py
- **Dependencies**: None
- **Action**: Use lock blobs or leases for per-key coordination.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Implement Blob key leases is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-ordering
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

### Task 2.2: Add ordering contract tests

- **Files**: tests/provider_contract/test_ordering*.py
- **Dependencies**: Task 2.1
- **Action**: Verify sequential same-key publishing, blocking after failure, and concurrent different-key publishing.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Add ordering contract tests is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-ordering
- **Status**: [ ] pending
- **Updated At**: 2026-05-21
