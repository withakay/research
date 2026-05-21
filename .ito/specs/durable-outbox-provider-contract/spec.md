<!-- ITO:START -->
## Purpose
This specification documents the durable outbox capability after the archived implementation changes.

## Requirements

### Requirement: Protocol Only Contract
The shared provider contract SHALL exercise stores through the `DurableOutboxStore` protocol and SHALL NOT require fake-store internals.

#### Scenario: non-fake adapter is tested
- **WHEN** the adapter satisfies `DurableOutboxStore`
- **THEN** the base provider contract can run without inheritance checks

### Requirement: Optional Capability Hooks
The provider contract SHALL support optional hooks for cleanup, stale reclaim, failure injection, and state inspection.

#### Scenario: provider lacks an optional hook
- **WHEN** an optional behavior cannot be driven through public APIs
- **THEN** the contract skips that scenario with an explicit capability reason

### Requirement: Contract Coverage
The contract SHALL verify duplicate put, single-winner claim, retry, mark-sent, failover replay, cleanup freeze, and ordering behavior where supported.

#### Scenario: fake store is certified
- **WHEN** the fake store runs the generic contract
- **THEN** the contract passes without fake-store type checks
<!-- ITO:END -->
