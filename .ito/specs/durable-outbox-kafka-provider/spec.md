<!-- ITO:START -->
## Purpose
This specification documents the durable outbox capability after the archived implementation changes.

## Requirements

### Requirement: Durable Outbox CI
The repository SHALL include CI for durable-outbox-python that runs tests, Ruff, ty, and package build.

#### Scenario: pull request changes durable-outbox-python
- **WHEN** CI runs
- **THEN** all package verification commands are executed
<!-- ITO:END -->
