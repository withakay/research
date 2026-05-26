<!-- ITO:START -->
## ADDED Requirements

### Requirement: First-Party Provider Package Matrix
The repository SHALL publish each first-party concrete sink and store as a
workspace package with package-local dependencies, typed exports, plugin entry
points, README documentation, and tests.

- **Requirement ID**: durable-outbox-provider-packages:first-party-provider-package-matrix

#### Scenario: provider packages are listed

- **WHEN** the workspace package matrix is inspected
- **THEN** file sink, Kafka sink, memory store, Blob store, Cosmos store, and SQL
  store are present as separate first-party provider packages

#### Scenario: provider package metadata is inspected

- **WHEN** package metadata for a first-party provider package is inspected
- **THEN** the package depends on `durable-outbox` for core contracts
- **THEN** provider-specific third-party dependencies are declared only by that
  provider package

### Requirement: Provider Package Plugin Registration
Every first-party provider package SHALL register its loadable sinks or stores
through the durable outbox plugin entry point groups.

- **Requirement ID**: durable-outbox-provider-packages:provider-package-plugin-registration

#### Scenario: first-party provider packages are installed

- **WHEN** an application asks for available sinks and stores
- **THEN** registered first-party provider names include file, kafka, memory,
  blob, cosmos, and sql as appropriate

#### Scenario: provider package is loaded by name

- **WHEN** a registered first-party provider is loaded with valid configuration
- **THEN** the returned object satisfies the corresponding durable outbox core
  protocol
<!-- ITO:END -->
