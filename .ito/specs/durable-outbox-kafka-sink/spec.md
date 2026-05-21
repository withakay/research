<!-- ITO:START -->
## Purpose
This specification documents the durable outbox capability after the archived implementation changes.

## Requirements

### Requirement: Hardened Kafka Publish Lifecycle
The Kafka sink SHALL manage producer construction, polling, delivery timeout, flush, and close behavior for acknowledged publishing.

#### Scenario: delivery callback never arrives
- **WHEN** publish exceeds the configured delivery timeout
- **THEN** the sink raises a retryable publish error

### Requirement: Kafka Failure Classification
The Kafka sink SHALL classify Kafka errors into retryable and non-retryable durable outbox errors based on provider error metadata and configuration context.

#### Scenario: topic authorization fails
- **WHEN** Kafka reports a deterministic authorization failure
- **THEN** the sink raises a non-retryable publish error

### Requirement: Trace Header Propagation
The Kafka sink SHALL include event identity and optional trace context headers while preserving supplied event headers.

#### Scenario: trace context exists
- **WHEN** an event is published
- **THEN** Kafka headers include event id and trace context
<!-- ITO:END -->
