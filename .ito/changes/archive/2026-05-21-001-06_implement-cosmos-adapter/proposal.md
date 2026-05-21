<!-- ITO:START -->
## Why

Cosmos DB provides a clean managed RPO=0 path when configured for strong consistency, multiple regions, and a single write region. The package needs a Cosmos adapter after the core and Blob MVP stabilize so users with small to medium event envelopes can choose this store.

## What Changes

- Implement Cosmos item shape and partition strategy for ordered and unordered modes.
- Implement idempotent create by event_id.
- Implement ETag conditional claim and status transitions.
- Implement failover replay and cleanup queries.
- Validate RPO=0 capability only for strong consistency, more than one region, and single-write configuration.

## Change Shape

- **Type**: feature
- **Risk**: high
- **Stateful**: yes
- **Public Contract**: config
- **Design Needed**: yes
- **Design Reason**: Durable outbox behavior is stateful and contract-sensitive.

## Capabilities

### New Capabilities

- `durable-outbox-cosmos-store`: Implement Cosmos Adapter.

### Modified Capabilities

None.

## Impact

Adds azure-cosmos optional dependency and Cosmos-specific provider tests. RU sizing and item size constraints must be documented.
<!-- ITO:END -->
