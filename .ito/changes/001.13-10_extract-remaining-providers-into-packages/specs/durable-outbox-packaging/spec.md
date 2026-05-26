<!-- ITO:START -->
## MODIFIED Requirements

### Requirement: Consumer Documentation
The package SHALL document installation, quickstart usage, abstract core
boundaries, provider package installation, provider plugin loading, plugin
authoring paths, RPO=0 caveats, and verification commands.

- **Requirement ID**: durable-outbox-packaging:consumer-documentation

#### Scenario: new consumer reads README
- **WHEN** they follow the quickstart
- **THEN** they can create an event, put it in a store, and dispatch it through a sink

#### Scenario: consumer installs provider plugins
- **WHEN** they need a sink or store plugin from a pip registry or local path
- **THEN** documentation shows how to install the plugin package and load it by name

#### Scenario: consumer migrates from core provider imports
- **WHEN** they previously imported concrete providers from the core namespace
- **THEN** documentation shows the replacement provider package install and
  import or plugin-loading path

#### Scenario: developer verifies workspace packages
- **WHEN** they follow the development commands
- **THEN** they can sync, test, lint, type-check, and build all durable outbox workspace packages with uv
<!-- ITO:END -->
