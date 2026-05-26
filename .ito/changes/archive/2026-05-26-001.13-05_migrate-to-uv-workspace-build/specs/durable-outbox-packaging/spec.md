## MODIFIED Requirements
### Requirement: Consumer Documentation
The package SHALL document installation, quickstart usage, provider plugin installation, RPO=0 caveats, and workspace verification commands.

#### Scenario: new consumer reads README
- **WHEN** they follow the quickstart
- **THEN** they can create an event, put it in a store, and dispatch it through a sink

#### Scenario: contributor verifies workspace packages
- **WHEN** they follow the development commands
- **THEN** they can sync, test, lint, type-check, and build all durable outbox workspace packages with uv

### Requirement: Packaging Completeness
The package SHALL use `uv_build`, include license metadata, include typed package marker, and support workspace build verification.

#### Scenario: package is built
- **WHEN** `uv build --all-packages` runs
- **THEN** the source and wheel artifacts include typed package metadata and license information
