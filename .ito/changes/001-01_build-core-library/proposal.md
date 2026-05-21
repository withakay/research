<!-- ITO:START -->
## Why

The package needs a storage- and sink-agnostic core before any concrete provider can be implemented safely. This change establishes the accepted-event contract, lifecycle model, dispatcher behavior, retry handling, and provider certification surface used by every later adapter.

## What Changes

- Define core event, receipt, claim, status, publish-result, capability, and error models.
- Define asynchronous DurableOutboxStore and MessageSink protocols.
- Implement the dispatcher lifecycle for claim, publish, mark sent, retry, and fail transitions.
- Implement retry/backoff and retryable versus non-retryable error classification.
- Add a fake in-memory store, fake sink, failure injection helpers, and provider certification tests.

## Change Shape

- **Type**: feature
- **Risk**: high
- **Stateful**: yes
- **Public Contract**: config
- **Design Needed**: yes
- **Design Reason**: Durable outbox behavior is stateful and contract-sensitive.

## Capabilities

### New Capabilities

- `durable-outbox-core`: Build Core Durable Outbox Library.

### Modified Capabilities

None.

## Impact

Creates the reusable Python package skeleton under durable-outbox-python with core modules and testing utilities. No external cloud dependency is required for this phase.
<!-- ITO:END -->
