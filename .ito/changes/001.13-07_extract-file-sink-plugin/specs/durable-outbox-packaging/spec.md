## MODIFIED Requirements
### Requirement: Consumer Documentation
The package SHALL document installation, quickstart usage, provider plugin installation, RPO=0 caveats, and workspace verification commands.

#### Scenario: new consumer reads README
- **WHEN** they follow the quickstart
- **THEN** they can create an event, put it in a store, and dispatch it through a sink

#### Scenario: file sink user reads README
- **WHEN** they need local JSONL publishing
- **THEN** documentation shows how to install and load `durable-outbox-file-sink`
