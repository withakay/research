## ADDED Requirements

### Requirement: Deterministic Store Clocks
Stores SHALL allow callers to inject a clock for store-generated lifecycle timestamps.

#### Scenario: test injects a fixed clock
- **WHEN** a store accepts, claims, fails, or replays an event
- **THEN** the generated timestamp comes from the injected clock
