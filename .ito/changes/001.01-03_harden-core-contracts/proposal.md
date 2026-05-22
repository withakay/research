# Change: Harden core durable outbox contracts

## Why
The core API accepts ambiguous inputs that can produce provider-specific behavior: naive datetimes, non-positive claim limits, oversized payloads for stores that declare a maximum, and inconsistent duplicate-event exceptions.

## What Changes
- Reject naive datetimes and invalid ordered event metadata at the envelope boundary.
- Require positive claim/replay limits across stores.
- Enforce `OutboxCapabilities.max_payload_bytes` during `put`.
- Standardize incompatible duplicate events on `DuplicateEventConflictError`.

## Impact
- Affected specs: `durable-outbox-core`, `durable-outbox-provider-contract`
- Affected code: `durable_outbox.core`, `durable_outbox.stores`, provider contract tests
