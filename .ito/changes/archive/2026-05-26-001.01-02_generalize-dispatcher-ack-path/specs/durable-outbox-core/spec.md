## MODIFIED Requirements

### Requirement: Post-Acknowledgement Store Failure Handling
The dispatcher SHALL NOT convert a store failure after sink acknowledgement into a publish retry failure.

#### Scenario: mark sent fails after sink acknowledgement
- **WHEN** the sink acknowledges publication but `mark_sent` fails
- **THEN** the dispatcher records a post-ack store failure and leaves the claimed event for store-level stale-claim recovery

### Requirement: Sink-Agnostic Dispatcher Metrics
The dispatcher SHALL emit provider-independent publish metrics.

#### Scenario: a non-Kafka sink publishes an event
- **WHEN** dispatch runs
- **THEN** metrics are emitted with `outbox_publish_*` names rather than sink-specific names
