<!-- ITO:START -->
## Why

The core durable outbox package still ships concrete Kafka, Blob, Cosmos, and
memory implementations even though SQL and file providers now live in separate
workspace packages. Moving the remaining concrete sinks and stores into provider
packages makes the core package fully abstract, keeps optional dependencies out
of the default install, and gives each provider a clean release and test
boundary.

## What Changes

- **BREAKING**: Remove concrete sink and store implementations from the
  `durable_outbox` core package namespace instead of preserving compatibility
  modules.
- Add provider packages for the remaining concrete implementations:
  - `durable-outbox-kafka-sink`
  - `durable-outbox-memory-store`
  - `durable-outbox-blob-store`
  - `durable-outbox-cosmos-store`
- Register every extracted provider through the existing provider plugin API so
  applications load stores and sinks by plugin name.
- Keep existing SQL store and file sink packages as provider-package exemplars
  and align their metadata, tests, and docs with the new package matrix.
- Update package metadata, documentation, examples, and tests so core imports
  prove the core package is abstract.

## Change Shape

- **Type**: migration
- **Risk**: high
- **Stateful**: no
- **Public Contract**: config
- **Design Needed**: yes
- **Design Reason**: This is a breaking package topology and import-boundary
  change that affects public imports, optional dependencies, provider discovery,
  documentation, and workspace verification.

## Capabilities

### New Capabilities

- `durable-outbox-abstract-core`: core package boundaries after all concrete
  sinks and stores are extracted.
- `durable-outbox-provider-packages`: package matrix and plugin registration
  expectations for first-party provider packages.

### Modified Capabilities

- `durable-outbox-plugin-api`: provider discovery/loading must cover all
  first-party sink and store packages and reject old core provider imports.
- `durable-outbox-packaging`: installation and documentation must describe the
  abstract core package and first-party provider package matrix.

## Impact

- Core package imports under `durable_outbox.sinks.*`,
  `durable_outbox.stores.*`, and concrete exports from
  `durable_outbox.stores`/`durable_outbox.sinks` will be removed.
- Provider implementation files move from `durable_outbox` into package-local
  modules under `packages/*`.
- Workspace `pyproject.toml`, package metadata, lockfile state, Ruff, ty, and
  package build configuration must include the new packages.
- Tests must be updated to import concrete implementations from provider
  packages or load them through plugin entry points.
- Consumer documentation must include migration examples from old imports to
  package installs and plugin loading.
<!-- ITO:END -->
