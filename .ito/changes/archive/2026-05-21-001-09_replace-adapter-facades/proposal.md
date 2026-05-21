<!-- ITO:START -->
## Why

The current Blob, Cosmos, and SQL adapters are useful API sketches, but they inherit from the in-memory fake store. That can hide provider-specific durability, concurrency, serialization, timeout, and RPO=0 behavior behind tests that only exercise memory state.

## What Changes

- Replace fake-store inheritance in production adapter classes with real provider implementations.
- Implement Azure Blob deterministic object layout, metadata/tags, ETag/lease claim transitions, cleanup, and dual-region acceptance.
- Implement Cosmos DB create/read idempotency, partition-key mapping, conditional patch claims, replay queries, cleanup queries, and RPO=0 configuration validation.
- Implement SQL schema migration helpers, idempotent inserts, row-lock claims, rowversion-safe transitions, Azure SQL sync wait, and Always On capability validation.
- Keep fake/in-memory stores under `durable_outbox.testing` only.

## Change Shape

- **Type**: feature
- **Risk**: high
- **Stateful**: yes
- **Public Contract**: code
- **Design Needed**: yes
- **Design Reason**: Durable provider behavior depends on cloud/database concurrency and durability boundaries.

## Capabilities

### New Capabilities

- `durable-outbox-real-providers`: Production provider implementations for Blob, Cosmos, and SQL.

### Modified Capabilities

- `durable-outbox-blob-store`
- `durable-outbox-blob-rpo0`
- `durable-outbox-cosmos-store`
- `durable-outbox-sql-store`

## Impact

This turns provider modules from typed facades into usable adapters. Tests will require either local fakes that simulate provider APIs or integration credentials for provider-specific contract runs.
<!-- ITO:END -->
