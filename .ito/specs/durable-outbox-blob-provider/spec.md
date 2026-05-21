<!-- ITO:START -->
## Purpose
This specification documents the durable outbox capability after the archived implementation changes.

## Requirements

### Requirement: Blob Provider Client
Blob stores SHALL use a typed provider client abstraction for record reads, writes, conditional updates, and deletes.

#### Scenario: duplicate incompatible put
- **WHEN** the same event id is written with incompatible envelope data
- **THEN** the store rejects it with a deterministic conflict error

### Requirement: Blob Local Certification
Blob provider behavior SHALL be certifiable with a fake provider client without cloud credentials.

#### Scenario: fake client contract runs
- **WHEN** provider contract tests run
- **THEN** claiming, retry, sent, replay, cleanup, and ordering behavior are verified
<!-- ITO:END -->
