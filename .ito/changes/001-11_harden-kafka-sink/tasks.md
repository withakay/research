# Tasks for: 001-11_harden-kafka-sink

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`

## Wave 1

- **Depends On**: None

### Task 1.1: Add producer lifecycle

- **Files**: durable-outbox-python/eva_durable_outbox/sinks/kafka.py
- **Action**: Add real producer construction and close/flush lifecycle.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 1.2: Add timeout and poll loop

- **Files**: durable-outbox-python/eva_durable_outbox/sinks/kafka.py
- **Action**: Add delivery timeout and poll loop behavior.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Classify Kafka errors

- **Files**: durable-outbox-python/eva_durable_outbox/sinks/kafka.py
- **Action**: Implement Kafka error classification and trace header propagation.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21

### Task 2.2: Add Kafka sink tests

- **Files**: durable-outbox-python/tests/test_kafka_operations.py
- **Action**: Add focused Kafka sink tests.
- **Verify**: `uv run pytest`
- **Status**: [x] complete
- **Updated At**: 2026-05-21
