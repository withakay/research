## MODIFIED Requirements
### Requirement: Consumer Documentation
The package SHALL document installation, quickstart usage, provider plugin installation, RPO=0 caveats, and verification commands.

#### Scenario: new consumer reads README
- **WHEN** they follow the quickstart
- **THEN** they can create an event, put it in a store, and dispatch it through a sink

#### Scenario: consumer configures plugin providers
- **WHEN** they install a provider plugin package
- **THEN** documentation shows the plugin name and configuration path for loading it
