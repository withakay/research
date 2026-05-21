<!-- ITO:START -->
## Purpose
This specification documents the durable outbox capability after the archived implementation changes.

## Requirements

### Requirement: SQL Provider Client
SQL stores SHALL use typed provider client operations for inserts, row-lock claims, transitions, replay, cleanup, and RPO=0 sync waits.

#### Scenario: sync wait timeout
- **WHEN** Azure SQL sync wait fails after insert
- **THEN** put raises a retryable store error
<!-- ITO:END -->
