<!-- ITO:START -->
## Purpose
This specification documents the durable outbox capability after the archived implementation changes.

## Requirements

### Requirement: Blob Idempotent Acceptance
The Blob store SHALL persist accepted events at deterministic blob names derived from event_id and SHALL treat duplicate put for the same event_id as idempotent success.

#### Scenario: producer retries put
- **WHEN** the event blob already exists for event_id
- **THEN** the store returns an accepted receipt for the existing event without creating a duplicate

### Requirement: Blob Claiming
The Blob store SHALL allow only one dispatcher to claim a pending event using Blob conditional update semantics.

#### Scenario: two dispatchers claim one event
- **WHEN** both attempt to claim the same PENDING blob
- **THEN** only one claim succeeds and receives a claim token

### Requirement: Blob Retry And Reclaim
The Blob store SHALL return retryable failures to PENDING and SHALL reclaim stale IN_FLIGHT events after the configured claim timeout.

#### Scenario: dispatcher crashes after claim
- **WHEN** claimed_at is older than the stale timeout
- **THEN** a later claim attempt can reclaim the event

### Requirement: Blob Cleanup
The Blob store SHALL delete or archive SENT events only after expires_at plus safety margin and only when cleanup is not frozen.

#### Scenario: sent event expires
- **WHEN** cleanup is active and the safety margin has elapsed
- **THEN** the store removes or archives the event

<!-- ITO:END -->
