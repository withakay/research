<!-- ITO:START -->
## Why

Blob needs a provider-client-backed adapter that can be certified without relying on test-store inheritance.

## What Changes

- Add typed Blob provider client protocols and fake provider client.
- Implement Blob and dual-region Blob store methods directly against provider clients.
- Add duplicate compatibility checks, conditional claim behavior, cleanup, failover replay, ordering locks, and repair matrix tests.

## Change Shape

- **Type**: feature
- **Risk**: high
- **Stateful**: yes
- **Public Contract**: code
- **Design Needed**: yes
- **Design Reason**: Blob correctness depends on provider concurrency and durability semantics.
<!-- ITO:END -->
