<!-- ITO:START -->
## Why

The durable outbox needs a first real downstream sink for Durable Outbox Kafka Publisher integration. Kafka publication must preserve at-least-once semantics by acknowledging only after broker acknowledgement and by surfacing retryable failures correctly.

## What Changes

- Implement a MessageSink backed by confluent-kafka.
- Validate producer configuration for acks=all and idempotence by default.
- Publish event_id and tracing context in Kafka headers.
- Return partition, offset, and published timestamp only after acknowledgement.
- Map Kafka failures into retryable and non-retryable durable outbox errors.

## Change Shape

- **Type**: feature
- **Risk**: high
- **Stateful**: yes
- **Public Contract**: config
- **Design Needed**: yes
- **Design Reason**: Durable outbox behavior is stateful and contract-sensitive.

## Capabilities

### New Capabilities

- `durable-outbox-kafka-sink`: Implement Kafka Sink.

### Modified Capabilities

None.

## Impact

Adds optional kafka dependency surface and tests around publish acknowledgement behavior. Consumers must continue to dedupe by event_id.
<!-- ITO:END -->
