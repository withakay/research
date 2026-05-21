<!-- ITO:START -->
## Approach

Use a typed Blob provider client protocol with a local fake client for deterministic tests. The production store implements the durable outbox protocol by reading and conditionally writing provider records.

## Verification Strategy

Run unit contract tests against the fake provider client and keep live Blob/Azurite integration tests behind explicit markers.
<!-- ITO:END -->
