<!-- ITO:START -->
## Why

The dispatcher currently computes retry delay without using the event's actual attempt count. That weakens backoff behavior under repeated failures and makes retry metadata less useful operationally.

## What Changes

- Add attempt metadata to claimed events or an equivalent dispatch context.
- Compute retry backoff from the store-provided attempt count.
- Add jitter and maximum retry policy hooks.
- Preserve deterministic non-retryable classification for poison events.
- Add tests for repeated retry growth, cap behavior, and retry metadata recording.

## Change Shape

- **Type**: feature
- **Risk**: medium
- **Stateful**: yes
- **Public Contract**: code
- **Design Needed**: yes
- **Design Reason**: Retry semantics are part of the durable outbox lifecycle contract.

## Capabilities

### Modified Capabilities

- `durable-outbox-core`

## Impact

Dispatchers become more predictable under prolonged sink outages and expose better attempt data for metrics and diagnostics.
<!-- ITO:END -->
