<!-- ITO:START -->
## Context

The durable outbox package must provide certified storage behavior, not only typed interfaces. Production adapters must not rely on the fake store for correctness.

## Approach

Define a small internal provider client abstraction per backend, then implement each store protocol against that abstraction. Unit tests use deterministic fake provider clients; integration tests can bind real Azure SDK or SQL connections through optional dependency groups.

## Contracts / Interfaces

- Production adapters implement `DurableOutboxStore` directly.
- Fake stores remain test-only and are not base classes for production adapters.
- Adapter constructors validate certified mode before exposing `rpo_zero_for_accepted_events=True`.
- Provider-specific exceptions are mapped to durable outbox errors.

## Data / State

Adapters persist the same logical envelope: event, accepted flag, status, attempts, claim ownership, retry metadata, sent metadata, failover metadata, and diagnostics.

## Decisions

- Prefer explicit client protocols over importing cloud SDK types into core contracts.
- Keep optional dependencies isolated behind `project.optional-dependencies`.
- Use deterministic serialization for idempotency compatibility checks.

## Risks / Trade-offs

- Real provider behavior is harder to test locally, so each adapter gets a fake provider client and optional integration test marks.
- Azure and SQL SDK APIs may differ in async support; use adapter-specific wrappers rather than leaking SDK details.

## Verification Strategy

Run provider contract tests against fake provider clients, plus optional integration tests when credentials are configured.
<!-- ITO:END -->
