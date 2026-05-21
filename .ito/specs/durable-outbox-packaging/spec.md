<!-- ITO:START -->
## Purpose
This specification documents the durable outbox capability after the archived implementation changes.

## Requirements

### Requirement: Consumer Documentation
The package SHALL document installation, quickstart usage, adapter extras, RPO=0 caveats, and verification commands.

#### Scenario: new consumer reads README
- **WHEN** they follow the quickstart
- **THEN** they can create an event, put it in a store, and dispatch it through a sink

### Requirement: Packaging Completeness
The package SHALL include license metadata, typed package marker, and build verification.

#### Scenario: package is built
- **WHEN** `uv build` runs
- **THEN** the source and wheel artifacts include typed package metadata and license information
<!-- ITO:END -->
