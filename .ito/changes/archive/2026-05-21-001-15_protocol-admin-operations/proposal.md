<!-- ITO:START -->
## Why

Operational status and repair hooks currently depend on the fake store. Production services need protocol-backed admin APIs that can work across adapters and emit audit metadata without inspecting payloads.

## What Changes

- Define status query, manual replay, repair, and audit protocols.
- Move fake-store-specific admin behavior behind a test implementation.
- Add metrics emission from dispatcher, store operations, cleanup, and replay.
- Add authorization boundary documentation for hosting services.

## Change Shape

- **Type**: feature
- **Risk**: medium
- **Stateful**: yes
- **Public Contract**: code
- **Design Needed**: yes
- **Design Reason**: Admin operations mutate event lifecycle state and need audit boundaries.

## Capabilities

### Modified Capabilities

- `durable-outbox-operations`

## Impact

Operators can inspect and repair outbox state through stable package hooks independent of storage backend.
<!-- ITO:END -->
