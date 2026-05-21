<!-- ITO:START -->
## Why

The Kafka sink currently validates a safe config and waits on delivery callbacks, but it does not yet own the full producer lifecycle or classify Kafka errors rigorously.

## What Changes

- Add real `confluent-kafka` producer construction behind the optional Kafka extra.
- Implement polling, delivery timeout, cancellation, close, and flush behavior.
- Classify Kafka errors into retryable and non-retryable durable outbox errors.
- Propagate event identity and tracing headers without mutating business payload.
- Add tests for callback success, timeout, transient broker failure, authorization/configuration failure, and close/flush behavior.

## Change Shape

- **Type**: feature
- **Risk**: medium
- **Stateful**: no
- **Public Contract**: code
- **Design Needed**: yes
- **Design Reason**: Async callback bridging and error classification affect at-least-once semantics.

## Capabilities

### Modified Capabilities

- `durable-outbox-kafka-sink`

## Impact

The Kafka sink becomes usable in an application process, with clear shutdown and deterministic failure behavior.
<!-- ITO:END -->
