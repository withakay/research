<!-- ITO:START -->
## Why

Some consumers need durable outbox storage in relational infrastructure and may couple outbox acceptance with business transactions. The package needs SQL adapters that clearly separate certified RPO=0 modes from asynchronous failover configurations.

## What Changes

- Define SQL schema and migration for durable_outbox_events.
- Implement idempotent insert by event_id.
- Implement claim query with row locks and rowversion/claim token safety.
- Implement Azure SQL active geo-replication plus sp_wait_for_database_copy_sync mode.
- Implement SQL Server Always On synchronous-commit capability mode.
- Add replay, cleanup, and provider certification tests.

## Change Shape

- **Type**: feature
- **Risk**: high
- **Stateful**: yes
- **Public Contract**: config
- **Design Needed**: yes
- **Design Reason**: Durable outbox behavior is stateful and contract-sensitive.

## Capabilities

### New Capabilities

- `durable-outbox-sql-store`: Implement SQL Adapter.

### Modified Capabilities

None.

## Impact

Adds sqlalchemy and pyodbc optional dependency surface. Certified RPO=0 SQL acceptance may add latency through sync wait or synchronous commit requirements.
<!-- ITO:END -->
