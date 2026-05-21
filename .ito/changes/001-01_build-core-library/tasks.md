# Tasks for: 001-01_build-core-library

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

```bash
ito tasks status 001-01_build-core-library
ito tasks next 001-01_build-core-library
ito tasks start 001-01_build-core-library 1.1
ito tasks complete 001-01_build-core-library 1.1
```

______________________________________________________________________

## Wave 1

- **Depends On**: None

### Task 1.1: Create package skeleton and models

- **Files**: durable-outbox-python/eva_durable_outbox/core/*.py
- **Dependencies**: None
- **Action**: Add core dataclasses, enums, capability declarations, and common errors.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Create package skeleton and models is implemented and covered by focused tests.
- **Requirements**: durable-outbox-core
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 1.2: Define protocols and dispatcher

- **Files**: durable-outbox-python/eva_durable_outbox/core/store.py, sink.py, dispatcher.py, retry.py
- **Dependencies**: Task 1.1
- **Action**: Implement async protocols, dispatch loop, retry policy, and error classification.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Define protocols and dispatcher is implemented and covered by focused tests.
- **Requirements**: durable-outbox-core
- **Status**: [x] complete
- **Updated At**: 2026-05-21

______________________________________________________________________

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Add certification harness

- **Files**: durable-outbox-python/eva_durable_outbox/testing/*.py, tests/provider_contract/*
- **Dependencies**: None
- **Action**: Build fake store/sink and shared behavioral tests.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Add certification harness is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-core
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 2.2: Verify core behavior

- **Files**: durable-outbox-python/tests/**
- **Dependencies**: Task 2.1
- **Action**: Run pytest for core and provider contract tests.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Verify core behavior is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-core
- **Status**: [ ] pending
- **Updated At**: 2026-05-21
