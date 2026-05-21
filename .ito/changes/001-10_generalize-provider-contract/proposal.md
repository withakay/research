<!-- ITO:START -->
## Why

The provider contract currently verifies behavior through a fake-store-specific shape. That makes it less useful as a certification harness for real Blob, Cosmos, SQL, and future stores.

## What Changes

- Make provider contract tests operate only through `DurableOutboxStore`.
- Add fixture hooks for event creation, state inspection, failure injection, clock control, and cleanup execution.
- Cover idempotent acceptance, single-winner claim, retry, mark-sent, stale reclaim, failover replay, cleanup freeze, and ordering where supported.
- Keep fake-store-specific assertions in fake-store tests, not in the shared contract.

## Change Shape

- **Type**: feature
- **Risk**: medium
- **Stateful**: yes
- **Public Contract**: tests
- **Design Needed**: yes
- **Design Reason**: The contract becomes the certification surface for every adapter.

## Capabilities

### Modified Capabilities

- `durable-outbox-core`
- `durable-outbox-provider-contract`

## Impact

Real adapters can prove they satisfy durable outbox semantics without copying fake-store assumptions.
<!-- ITO:END -->
