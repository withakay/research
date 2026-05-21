<!-- ITO:START -->
## Purpose
This specification documents the durable outbox capability after the archived implementation changes.

## Requirements

### Requirement: SQL Schema
The SQL store SHALL persist all outbox lifecycle metadata needed for idempotent acceptance, claiming, retries, sent metadata, failover replay, cleanup, and diagnostics.

#### Scenario: migration is applied
- **WHEN** durable_outbox_events is created
- **THEN** required indexes for pending, replay, and ordered queries exist

### Requirement: SQL Idempotent Acceptance
The SQL store SHALL insert by event_id and SHALL return idempotent success when the same event_id already exists for a compatible event.

#### Scenario: producer retries put
- **WHEN** the row already exists
- **THEN** the store returns an accepted receipt without inserting another row

### Requirement: SQL Claiming
The SQL store SHALL claim PENDING events with database locking semantics so concurrent publishers cannot claim the same row.

#### Scenario: two publishers claim concurrently
- **WHEN** the claim query runs with row locks
- **THEN** each row is returned to at most one publisher

### Requirement: SQL RPO0 Modes
The SQL adapters SHALL declare RPO=0 only when Azure SQL sync wait succeeds after commit or SQL Server Always On synchronous commit requirements are configured.

#### Scenario: Azure SQL sync wait times out
- **WHEN** put attempts to certify acceptance
- **THEN** put returns retryable failure rather than a successful RPO=0 receipt

<!-- ITO:END -->
