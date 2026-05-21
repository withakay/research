<!-- ITO:START -->
## Why

The current ordering behavior proves same-key blocking in memory, but production stores need leases or locks that survive process crashes and expire safely.

## What Changes

- Define ordering lock protocols independent of Blob.
- Implement Blob lock blobs or leases for per-topic+ordering-key coordination.
- Add lease expiry, stale recovery, and lock release behavior.
- Add contract tests for same-key sequencing, different-key concurrency, and crash recovery.

## Change Shape

- **Type**: feature
- **Risk**: high
- **Stateful**: yes
- **Public Contract**: code
- **Design Needed**: yes
- **Design Reason**: Ordered dispatch requires distributed coordination.

## Capabilities

### Modified Capabilities

- `durable-outbox-ordering`
- `durable-outbox-blob-store`

## Impact

Ordered publishing can be used by stateless dispatcher instances without relying on process-local state.
<!-- ITO:END -->
