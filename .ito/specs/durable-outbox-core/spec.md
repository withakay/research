<!-- ITO:START -->
## ADDED Requirements

### Requirement: Core Data Model
The package SHALL expose typed models for outbox events, event status, publishing mode, accepted receipts, claimed events, publish results, and provider capabilities.

#### Scenario: event envelope is created
- **WHEN** an application constructs an OutboxEvent with metadata and opaque payload
- **THEN** the model preserves the payload bytes and validates only envelope-level fields

### Requirement: Store And Sink Protocols
The package SHALL expose async protocols for DurableOutboxStore and MessageSink so dispatching is independent of storage and downstream sink implementations.

#### Scenario: dispatcher is wired
- **WHEN** a store and sink implementation satisfy the protocols
- **THEN** the dispatcher can use them without provider-specific imports

### Requirement: At Least Once Dispatcher
The dispatcher SHALL mark events SENT only after downstream acknowledgement and SHALL return retryable failures to PENDING with a next attempt time.

#### Scenario: publish succeeds
- **WHEN** the sink acknowledges a claimed event
- **THEN** the dispatcher records SENT with publish metadata

#### Scenario: transient publish failure occurs
- **WHEN** the sink raises a retryable error
- **THEN** the dispatcher records retry metadata and makes the event eligible for later retry

### Requirement: Provider Certification Harness
The package SHALL include shared contract tests that every store adapter can run for idempotent acceptance, claiming, delivery, failover, cleanup, and ordering behavior where supported.

#### Scenario: fake store is certified
- **WHEN** the in-memory test store runs the provider contract suite
- **THEN** the contract verifies duplicate put, single-winner claim, retry, and mark-sent behavior

<!-- ITO:END -->
