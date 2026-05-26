## MODIFIED Requirements

### Requirement: SQL Conditional State Transitions
SQL provider clients SHALL support compare-and-set replacement for claim and terminal state transitions.

#### Scenario: two store instances race to claim one event
- **WHEN** both instances use the same provider client
- **THEN** only one instance receives a claim for the event
