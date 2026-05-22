# Durable Outbox Providers

RPO=0 is an adapter acceptance contract. A store may claim RPO=0 only when `put(event)` returns after the event is durably present at that adapter's documented failover durability boundary.

## Blob

Blob RPO=0 requires application-level dual writes:

1. Write a PREPARED record in Region A.
2. Write a PREPARED record in Region B.
3. Mark Region A accepted.
4. Mark Region B accepted.
5. Return success only after both regions are accepted.

Azure Storage GRS, RA-GRS, GZRS, or RA-GZRS alone is asynchronous replication and is not an RPO=0 acceptance boundary.

## Cosmos

Cosmos RPO=0 requires strong consistency, more than one region, and single-write configuration. Multi-write and session-consistency modes can still be useful, but they must not declare `rpo_zero_for_accepted_events=True` in certified mode.

## SQL

Azure SQL RPO=0 requires committing the outbox row and then completing `sp_wait_for_database_copy_sync` against the active secondary before returning success. SQL Server Always On RPO=0 requires synchronous commit with the required synchronized secondaries configured.

