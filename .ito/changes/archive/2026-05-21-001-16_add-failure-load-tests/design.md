<!-- ITO:START -->
## Approach

Use fake sinks and fake provider clients to deterministically trigger each failure window. Keep fast tests in the default suite and mark throughput or integration tests as optional.

## Contracts / Interfaces

- Failure injection can fail sink publish, store mark-sent, store put, and claim paths.
- Tests assert events remain replayable after ambiguous outcomes.
- Load tests measure sustained dispatch throughput without external services.

## Verification Strategy

Add pytest markers for `load` and `integration`. Default test suite remains fast and deterministic.
<!-- ITO:END -->
