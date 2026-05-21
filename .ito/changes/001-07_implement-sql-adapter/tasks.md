# Tasks for: 001-07_implement-sql-adapter

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

```bash
ito tasks status 001-07_implement-sql-adapter
ito tasks next 001-07_implement-sql-adapter
ito tasks start 001-07_implement-sql-adapter 1.1
ito tasks complete 001-07_implement-sql-adapter 1.1
```

______________________________________________________________________

## Wave 1

- **Depends On**: None

### Task 1.1: Add SQL schema and mapper

- **Files**: eva_durable_outbox/stores/sql.py, migrations/*
- **Dependencies**: None
- **Action**: Create table/index definitions and row mapping.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Add SQL schema and mapper is implemented and covered by focused tests.
- **Requirements**: durable-outbox-sql-store
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 1.2: Implement put claim and lifecycle transitions

- **Files**: eva_durable_outbox/stores/sql.py
- **Dependencies**: Task 1.1
- **Action**: Add idempotent insert, claim query, retry, sent, failed, and cleanup logic.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Implement put claim and lifecycle transitions is implemented and covered by focused tests.
- **Requirements**: durable-outbox-sql-store
- **Status**: [x] complete
- **Updated At**: 2026-05-21

______________________________________________________________________

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Implement RPO=0 SQL modes

- **Files**: eva_durable_outbox/stores/sql.py
- **Dependencies**: None
- **Action**: Add Azure SQL sync wait and Always On capability validation.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Implement RPO=0 SQL modes is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-sql-store
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 2.2: Run SQL provider contract

- **Files**: tests/stores/test_sql*.py
- **Dependencies**: Task 2.1
- **Action**: Verify idempotency, claim concurrency, replay, cleanup, and RPO=0 mode behavior.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Run SQL provider contract is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-sql-store
- **Status**: [x] complete
- **Updated At**: 2026-05-21
