# Design: Core Contract Hardening

## Decisions
- Keep validation local and explicit. Envelope validation remains in `OutboxEvent`; store-level limits are checked by a shared helper because max payload size is a store capability.
- Use one duplicate conflict exception for all providers so callers can write provider-independent idempotency handling.
- Validate limits at public store methods before provider state is mutated.

## Alternatives Considered
- Silently normalizing naive datetimes to UTC was rejected because it can hide producer clock bugs.
- Returning empty batches for `limit <= 0` was rejected because it hides caller bugs and can create misleading monitoring.
