<!-- ITO:START -->
## Why

Some topics require per-key ordering while the package should retain high throughput for unrelated keys. This change adds the contracts and coordination needed to block later same-key events until earlier events are acknowledged or failed.

## What Changes

- Define ordering coordinator and per-key lock abstractions.
- Validate ordered events carry ordering_key and ordering_sequence where required.
- Implement per-key ordered dispatch behavior.
- Implement Blob lock lease coordination.
- Extend provider contract tests for same-key sequencing and different-key concurrency.

## Change Shape

- **Type**: feature
- **Risk**: high
- **Stateful**: yes
- **Public Contract**: config
- **Design Needed**: yes
- **Design Reason**: Durable outbox behavior is stateful and contract-sensitive.

## Capabilities

### New Capabilities

- `durable-outbox-ordering`: Add Ordered Publishing.

### Modified Capabilities

None.

## Impact

Adds optional ordered mode behavior to core and Blob store. Ordered hot keys trade throughput for deterministic per-key sequencing.
<!-- ITO:END -->
