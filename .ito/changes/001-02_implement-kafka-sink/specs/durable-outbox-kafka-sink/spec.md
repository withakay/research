<!-- ITO:START -->
## ADDED Requirements

### Requirement: Acknowledged Kafka Publish
The Kafka sink SHALL return PublishResult only after Kafka acknowledges the produced record.

#### Scenario: broker acknowledgement arrives
- **WHEN** Kafka delivery callback reports success
- **THEN** the sink returns partition, offset, and published_at metadata

### Requirement: Kafka Producer Safety Defaults
The Kafka sink SHALL require or apply producer settings compatible with at-least-once durable publishing, including acks=all and idempotent production unless explicitly overridden by documented non-certified mode.

#### Scenario: unsafe configuration is supplied
- **WHEN** acks is not all for certified mode
- **THEN** sink initialization fails with a configuration error

### Requirement: Event Identity Headers
The Kafka sink SHALL include event_id in published Kafka headers and SHALL preserve supplied event headers without inspecting business payload content.

#### Scenario: event is published
- **WHEN** the event has an event_id and headers
- **THEN** the Kafka record includes event_id plus the original headers

### Requirement: Failure Classification
The Kafka sink SHALL classify transient Kafka errors as retryable and deterministic configuration or authorization failures as non-retryable.

#### Scenario: Kafka is unavailable
- **WHEN** publish times out or receives a transient broker error
- **THEN** the sink raises a retryable publish error

<!-- ITO:END -->
