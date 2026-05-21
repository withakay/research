<!-- ITO:START -->
## Purpose
This specification documents the durable outbox capability after the archived implementation changes.

## Requirements

### Requirement: Backend Ordering Locks
Ordered dispatch SHALL coordinate same-key publishing with backend-backed locks or leases, not process-local memory.

#### Scenario: dispatcher crashes while holding a lock
- **WHEN** the lease expires
- **THEN** another dispatcher can acquire the key and continue publishing

### Requirement: Lock Scope
Ordering locks SHALL be scoped by environment, topic, and ordering key.

#### Scenario: different topics share an ordering key value
- **WHEN** both topics dispatch ordered events
- **THEN** they do not block each other
<!-- ITO:END -->
