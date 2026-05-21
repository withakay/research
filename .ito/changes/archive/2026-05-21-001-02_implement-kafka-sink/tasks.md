# Tasks for: 001-02_implement-kafka-sink

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

```bash
ito tasks status 001-02_implement-kafka-sink
ito tasks next 001-02_implement-kafka-sink
ito tasks start 001-02_implement-kafka-sink 1.1
ito tasks complete 001-02_implement-kafka-sink 1.1
```

______________________________________________________________________

## Wave 1

- **Depends On**: None

### Task 1.1: Add kafka optional dependency and config

- **Files**: durable-outbox-python/pyproject.toml, eva_durable_outbox/sinks/kafka.py
- **Dependencies**: None
- **Action**: Declare kafka extra and producer settings model.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Add kafka optional dependency and config is implemented and covered by focused tests.
- **Requirements**: durable-outbox-kafka-sink
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 1.2: Implement publish acknowledgement path

- **Files**: eva_durable_outbox/sinks/kafka.py
- **Dependencies**: Task 1.1
- **Action**: Produce records, await delivery callback, and return PublishResult.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Implement publish acknowledgement path is implemented and covered by focused tests.
- **Requirements**: durable-outbox-kafka-sink
- **Status**: [x] complete
- **Updated At**: 2026-05-21

______________________________________________________________________

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Add headers and tracing propagation

- **Files**: eva_durable_outbox/sinks/kafka.py, telemetry modules
- **Dependencies**: None
- **Action**: Inject event_id and optional trace headers.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Add headers and tracing propagation is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-kafka-sink
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 2.2: Test success and failure mapping

- **Files**: tests/sinks/test_kafka*.py
- **Dependencies**: Task 2.1
- **Action**: Cover ack success, retryable errors, non-retryable errors, and unsafe config validation.
- **Verify**: `python -m pytest` from `durable-outbox-python` once the package scaffold exists
- **Done When**: Test success and failure mapping is implemented and the relevant provider contract or focused tests pass.
- **Requirements**: durable-outbox-kafka-sink
- **Status**: [x] complete
- **Updated At**: 2026-05-21
