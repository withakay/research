<!-- ITO:START -->
## Why

Blob is the recommended first provider for the EVA-aligned MVP, but the single-region adapter should land before dual-region RPO=0 behavior. This gives the package a concrete store implementation for normal dispatch, claiming, retry, stale reclaim, and cleanup semantics.

## What Changes

- Implement deterministic event blob naming by event_id.
- Implement idempotent put and accepted event metadata.
- Implement ETag or lease-based claim and status transitions.
- Implement stale IN_FLIGHT reclaim and retry metadata.
- Implement logical cleanup for expired SENT records, with tags used only for discovery acceleration.

## Change Shape

- **Type**: feature
- **Risk**: high
- **Stateful**: yes
- **Public Contract**: config
- **Design Needed**: yes
- **Design Reason**: Durable outbox behavior is stateful and contract-sensitive.

## Capabilities

### New Capabilities

- `durable-outbox-blob-store`: Implement Single Region Blob Store MVP.

### Modified Capabilities

None.

## Impact

Adds azure-storage-blob optional dependency and a non-RPO=0 Blob provider that must declare rpo_zero_for_accepted_events=false.
<!-- ITO:END -->
