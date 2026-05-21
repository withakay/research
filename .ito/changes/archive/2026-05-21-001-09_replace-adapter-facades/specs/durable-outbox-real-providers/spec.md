<!-- ITO:START -->
## ADDED Requirements

### Requirement: Production Adapters Do Not Inherit Test Stores
Production Blob, Cosmos, and SQL adapter classes SHALL implement durable store behavior directly rather than inheriting from the in-memory fake store.

#### Scenario: adapter module is imported
- **WHEN** production store classes are inspected
- **THEN** their implementation does not depend on `FakeOutboxStore` inheritance

### Requirement: Provider Client Abstractions
Each production adapter SHALL isolate provider SDK calls behind typed internal client protocols.

#### Scenario: unit tests run without cloud credentials
- **WHEN** fake provider clients are supplied
- **THEN** provider-specific store logic can be exercised without live services

### Requirement: Provider Error Mapping
Production adapters SHALL map provider transient, conflict, timeout, and deterministic errors into durable outbox error classes.

#### Scenario: provider write times out
- **WHEN** acceptance result is ambiguous
- **THEN** the adapter raises a retryable durable store error
<!-- ITO:END -->
