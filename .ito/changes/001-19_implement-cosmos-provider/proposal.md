<!-- ITO:START -->
## Why

Cosmos needs a provider-client-backed adapter with explicit partitioning, ETag semantics, and RPO=0 certification checks.

## What Changes

- Add typed Cosmos provider client protocol and fake client.
- Implement put, claim, retry, sent, failed, replay, and cleanup through provider operations.
- Add duplicate compatibility checks and integration test documentation.
<!-- ITO:END -->
