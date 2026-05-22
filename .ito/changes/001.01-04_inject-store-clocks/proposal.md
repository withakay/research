# Change: Inject clocks into stores

## Why
Stores currently call `datetime.now(UTC)` directly, which makes lifecycle timestamps hard to test and can drift from dispatcher clocks during deterministic recovery tests.

## What Changes
- Add optional `Clock` injection to in-memory, Blob, Cosmos, and SQL stores.
- Use injected clocks for accepted, claimed, failed, and replay timestamps.
- Preserve default system-clock behavior for production callers.

## Impact
- Affected specs: `durable-outbox-core`, `durable-outbox-provider-contract`
- Affected code: all store implementations and tests
