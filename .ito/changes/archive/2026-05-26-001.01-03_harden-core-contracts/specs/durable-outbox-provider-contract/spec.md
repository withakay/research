## MODIFIED Requirements

### Requirement: Provider-Independent Duplicate Handling
Providers SHALL reject incompatible duplicate `event_id` puts with `DuplicateEventConflictError`.

#### Scenario: duplicate event id has different envelope data
- **WHEN** a producer puts an event id that already exists with incompatible content
- **THEN** every provider raises the same duplicate-conflict exception type

### Requirement: Provider Capability Enforcement
Providers SHALL enforce declared maximum payload sizes before accepting events.

#### Scenario: payload exceeds provider maximum
- **WHEN** an event payload exceeds the store capability limit
- **THEN** the store rejects the put without accepting the event
