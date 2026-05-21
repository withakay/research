# Tasks for: 001-06_implement-cosmos-adapter

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

```bash
ito tasks status 001-06_implement-cosmos-adapter
ito tasks next 001-06_implement-cosmos-adapter
ito tasks start 001-06_implement-cosmos-adapter 1.1
ito tasks complete 001-06_implement-cosmos-adapter 1.1
```

______________________________________________________________________

## Wave 1

- **Depends On**: None

### Task 1.1: Add Cosmos dependency and item mapper

- **Files**: pyproject.toml, eva_durable_outbox/stores/cosmos.py
- **Dependencies**: None
- **Action**: Define optional extra, item schema, and partition key logic.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Add Cosmos dependency and item mapper is implemented and covered by focused tests.
- **Requirements**: durable-outbox-cosmos-store
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

### Task 1.2: Implement put and claim

- **Files**: eva_durable_outbox/stores/cosmos.py
- **Dependencies**: Task 1.1
- **Action**: Use idempotent create and ETag conditional patch.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Implement put and claim is implemented and covered by focused tests.
- **Requirements**: durable-outbox-cosmos-store
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

______________________________________________________________________

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Implement replay cleanup and capability validation

- **Files**: eva_durable_outbox/stores/cosmos.py
- **Dependencies**: None
- **Action**: Add failover query, cleanup query, and RPO=0 config checks.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Implement replay cleanup and capability validation is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-cosmos-store
- **Status**: [ ] pending
- **Updated At**: 2026-05-21

### Task 2.2: Run Cosmos provider contract

- **Files**: tests/stores/test_cosmos*.py
- **Dependencies**: Task 2.1
- **Action**: Verify behavior with emulator/fake and config validation tests.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Run Cosmos provider contract is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-cosmos-store
- **Status**: [ ] pending
- **Updated At**: 2026-05-21
