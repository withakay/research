<!-- ITO:START -->
## Purpose
This specification documents the durable outbox capability after the archived implementation changes.

## Requirements

### Requirement: Ack Before Mark Sent Failure Test
The test suite SHALL prove that when sink acknowledgement succeeds but `mark_sent` fails, the event remains replayable and may be duplicated but is not lost.

#### Scenario: mark sent fails after acknowledgement
- **WHEN** the dispatcher crashes or receives a retryable store failure after sink ack
- **THEN** the event is still claimable or replayable later

### Requirement: MVP Throughput Load Test
The package SHALL include a repeatable load test for at least 1000 messages per minute per topic using fake providers.

#### Scenario: load test runs
- **WHEN** the fake provider and sink are used
- **THEN** the dispatcher sustains the MVP throughput target without failed events
<!-- ITO:END -->
