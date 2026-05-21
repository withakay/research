<!-- ITO:START -->
## Context

The original harness proves the fake store behaves correctly but cannot certify adapters that do not expose internal memory records.

## Approach

Create a `ProviderContract` helper with required store factory and optional capability hooks. The base contract uses only public store methods. Optional hooks add cleanup execution, internal state observation, and failure injection for providers that support them.

## Contracts / Interfaces

- Required: `store_factory() -> DurableOutboxStore`.
- Optional: `get_status(event_id)`, `force_claim_age(event_id, claimed_at)`, `cleanup(now)`, `repair(event_id)`.
- Contract assertions depend on public receipts, claims, and state transitions whenever possible.

## Data / State

State inspection is optional and typed. Providers that cannot expose direct state still validate externally observable behavior.

## Decisions

- Keep pytest helpers dependency-light.
- Use capability flags to skip unsupported optional behavior with explicit reasons.

## Verification Strategy

Run the generic contract against the in-memory store and every fake provider client introduced by real adapter work.
<!-- ITO:END -->
