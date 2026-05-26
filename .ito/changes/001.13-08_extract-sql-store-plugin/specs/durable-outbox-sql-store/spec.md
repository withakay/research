## MODIFIED Requirements
### Requirement: SQL Schema
The SQL store plugin SHALL persist all outbox lifecycle metadata needed for idempotent acceptance, claiming, retries, sent metadata, failover replay, cleanup, and diagnostics.

#### Scenario: migration is applied
- **WHEN** durable_outbox_events is created from the SQL plugin schema
- **THEN** required indexes for pending, replay, and ordered queries exist

### Requirement: SQL Idempotent Acceptance
The SQL store plugin SHALL insert by event_id and SHALL return idempotent success when the same event_id already exists for a compatible event.

#### Scenario: producer retries put
- **WHEN** the row already exists
- **THEN** the store returns an accepted receipt without inserting another row

### Requirement: SQL Claiming
The SQL store plugin SHALL claim PENDING events with database locking semantics so concurrent publishers cannot claim the same row.

#### Scenario: two publishers claim concurrently
- **WHEN** the claim query runs with row locks
- **THEN** each row is returned to at most one publisher

### Requirement: SQL RPO0 Modes
The SQL store plugin SHALL declare RPO=0 only when Azure SQL sync wait succeeds after commit or SQL Server Always On synchronous commit requirements are configured.

#### Scenario: Azure SQL sync wait times out
- **WHEN** put attempts to certify acceptance
- **THEN** put returns retryable failure rather than a successful RPO=0 receipt

## ADDED Requirements
### Requirement: SQL Store Plugin Package
SQL stores SHALL be distributed as an independent workspace package named `durable-outbox-sql-store` with import package `durable_outbox_sql_store`.

#### Scenario: package is installed
- **WHEN** application code imports `durable_outbox_sql_store`
- **THEN** it can construct SQL store classes directly

### Requirement: SQL Store Entry Points
The SQL store package SHALL register `azure-sql-sync` and `sql-always-on` entry points in the `durable_outbox.stores` group.

#### Scenario: Azure SQL store is loaded by configuration
- **WHEN** an application loads store plugin `azure-sql-sync` with valid configuration
- **THEN** the loader returns an Azure SQL sync outbox store

#### Scenario: Always On store is loaded by configuration
- **WHEN** an application loads store plugin `sql-always-on` with valid configuration
- **THEN** the loader returns a SQL Always On outbox store
