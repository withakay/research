<!-- ITO:START -->
## Approach

Introduce an `OrderingCoordinator` protocol that claims eligible events while holding backend locks. Blob implements locks with lease-bearing lock blobs. Other providers can implement equivalent row/item locks later.

## Contracts / Interfaces

- Lock identity is environment, topic, and ordering key hash.
- A lock is held while one same-key event is in-flight.
- Expired locks can be reacquired after dispatcher failure.

## Verification Strategy

Use fake lock clients for deterministic unit tests and provider contract scenarios.
<!-- ITO:END -->
