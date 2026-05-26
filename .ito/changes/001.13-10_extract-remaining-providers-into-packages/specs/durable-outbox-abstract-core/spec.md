<!-- ITO:START -->
## ADDED Requirements

### Requirement: Abstract Core Package
The core `durable-outbox` package SHALL contain only abstract durable outbox
contracts, orchestration, models, validation, plugin loading, telemetry, and
configuration primitives. It SHALL NOT ship concrete sink or store
implementations.

- **Requirement ID**: durable-outbox-abstract-core:abstract-core-package

#### Scenario: core package imports are inspected

- **WHEN** a consumer installs only `durable-outbox`
- **THEN** imports for core protocols, dispatcher, models, errors, retry,
  cleanup, failover, configuration, telemetry, and plugin loading succeed
- **THEN** imports for concrete Kafka, Blob, Cosmos, memory, SQL, and file
  provider implementations from the core namespace fail

#### Scenario: optional dependencies are inspected

- **WHEN** a consumer installs only `durable-outbox`
- **THEN** Kafka, Azure Blob, Azure Cosmos, and SQL driver dependencies are not
  required by the core package

### Requirement: No Core Provider Compatibility Modules
The core package SHALL NOT keep compatibility modules, forwarding imports, or
optional extras that silently load extracted provider implementations from the
old core namespace.

- **Requirement ID**: durable-outbox-abstract-core:no-core-provider-compatibility-modules

#### Scenario: old provider import path is used

- **WHEN** application code imports an extracted provider from an old
  `durable_outbox.sinks` or `durable_outbox.stores` module
- **THEN** the import fails with the normal Python import error for a missing
  module or symbol
- **THEN** the error does not install, import, or proxy to a provider package
<!-- ITO:END -->
