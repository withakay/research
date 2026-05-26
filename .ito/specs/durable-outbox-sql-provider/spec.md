## MODIFIED Requirements
### Requirement: SQL Provider Client
SQL store plugin implementations SHALL use typed provider client operations for inserts, row-lock claims, transitions, replay, cleanup, and RPO=0 sync waits.

#### Scenario: sync wait timeout
- **WHEN** Azure SQL sync wait fails after insert
- **THEN** put raises a retryable store error

#### Scenario: SQL plugin tests run without cloud credentials
- **WHEN** fake provider clients are supplied to the plugin package
- **THEN** provider-specific store logic can be exercised without live services
