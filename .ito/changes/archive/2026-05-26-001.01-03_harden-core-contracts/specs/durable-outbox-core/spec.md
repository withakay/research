## MODIFIED Requirements

### Requirement: Envelope Validation
The system SHALL reject event envelopes with missing identifiers, invalid ordering metadata, non-bytes payload or header values, naive datetimes, or an expiration that is not after creation.

#### Scenario: producer submits a naive datetime
- **WHEN** an outbox event is created with a naive `created_at` or `expires_at`
- **THEN** validation fails before the event can be stored

### Requirement: Store Method Arguments
Stores SHALL reject non-positive claim and replay limits before mutating provider state.

#### Scenario: dispatcher requests zero claims
- **WHEN** a store receives `claim_batch(limit=0)`
- **THEN** the store raises a validation error
