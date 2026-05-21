<!-- ITO:START -->
## Why

SQL adapters need provider-client-backed row semantics, duplicate compatibility checks, and explicit RPO=0 acceptance boundaries.

## What Changes

- Add SQL provider client protocol and fake client.
- Implement lifecycle methods through row operations.
- Add duplicate compatibility, claim, replay, sync-wait, and integration documentation.
<!-- ITO:END -->
