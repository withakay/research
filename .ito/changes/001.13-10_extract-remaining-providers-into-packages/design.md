<!-- ITO:START -->
## Context

The workspace already contains provider packages for the file sink and SQL
store. The core package still contains concrete Kafka, memory, Blob, and Cosmos
implementations plus lazy exports under `durable_outbox.sinks` and
`durable_outbox.stores`. That leaves optional dependencies and provider code
coupled to the core distribution even though the plugin API now supports
package-local providers.

## Goals / Non-Goals

**Goals:**

- Make `durable-outbox` an abstract core package with no concrete sink or store
  implementations.
- Move every first-party concrete provider into a workspace package with
  package-local dependencies and plugin registration.
- Preserve concrete provider behavior through package-local imports and plugin
  loading.
- Treat old core provider imports as intentionally broken.
- Keep verification strict under Python 3.14, uv, Ruff, ty, and the existing
  test suite.

**Non-Goals:**

- Changing durable outbox protocol semantics, dispatcher behavior, storage
  state machines, or provider data formats.
- Renaming public provider classes beyond moving their import namespaces.
- Publishing packages to an external registry as part of this change.

## Approach

Create provider packages for the remaining concrete implementations:

| Package | Namespace | Moves From |
| --- | --- | --- |
| `durable-outbox-kafka-sink` | `durable_outbox_kafka_sink` | `durable_outbox.sinks.kafka` |
| `durable-outbox-memory-store` | `durable_outbox_memory_store` | `durable_outbox.stores.memory` |
| `durable-outbox-blob-store` | `durable_outbox_blob_store` | `durable_outbox.stores.azure_blob`, `durable_outbox.stores.blob_geo` |
| `durable-outbox-cosmos-store` | `durable_outbox_cosmos_store` | `durable_outbox.stores.cosmos`, `durable_outbox.stores.cosmos_azure` |

The core package retains `durable_outbox.core`, `durable_outbox.plugins`,
configuration, telemetry, operations, and consumer-facing protocol exports from
`durable_outbox.__init__`. The `durable_outbox.sinks` and
`durable_outbox.stores` concrete export modules are removed or reduced to
non-provider abstract namespace behavior only if import machinery requires
package markers.

## Contracts / Interfaces

- Provider packages depend on `durable-outbox` and export the same concrete
  classes from their package-local namespace.
- Provider packages register entry points in the existing durable outbox sink
  and store plugin groups.
- The plugin API remains the stable runtime loading contract.
- Old core provider imports are removed without deprecation shims because the
  change is explicitly breaking.

## Data / State

No persisted data format changes are planned. Blob, Cosmos, SQL, memory, Kafka,
and file provider behavior must remain compatible with existing tests after
imports move to provider package namespaces.

## Decisions

- **Break old imports instead of forwarding them.** This keeps the core package
  truly abstract and avoids reintroducing optional dependency coupling through
  compatibility modules.
- **Use package names that describe runtime role.** `*-sink` and `*-store`
  package names match the existing `durable-outbox-file-sink` and
  `durable-outbox-sql-store` packages.
- **Keep in-memory store outside core.** Even though it is useful for examples
  and tests, it is still a concrete store implementation and must live in a
  provider package for the core to be fully abstract.
- **Retain tests in the main workspace.** Tests can be reorganized or
  parametrized, but verification should continue to run through the top-level
  uv workspace so extracted packages are tested together.

## Risks / Trade-offs

- **Import breakage** -> Mitigation: document exact migration paths and update
  examples/tests to use provider packages or plugin loading.
- **Circular package dependencies** -> Mitigation: provider packages depend on
  core only; core never imports provider package modules.
- **Entry point drift** -> Mitigation: add tests that inspect provider metadata
  and load each first-party provider by name.
- **Strict typing gaps after moves** -> Mitigation: include all new namespaces
  in ty source configuration and run `uv run ty check`.

## Verification Strategy

- Write red tests that assert concrete provider imports from the core namespace
  fail and provider-package imports succeed.
- Add packaging tests for every new provider package and entry point.
- Run focused provider tests after each move.
- Run full workspace verification:
  - `uv run pytest`
  - `uv run ruff check .`
  - `uv run ruff format --check .`
  - `uv run ty check`
  - `uv build --all-packages`

## Migration / Rollback

Migration for consumers is source-level:

1. Add the required provider package dependency.
2. Replace old imports with provider-package imports or plugin loading.
3. Remove reliance on `durable-outbox[kafka]` or `durable-outbox[azure]`
   extras if they existed only to get concrete providers.

Rollback is to keep provider code in core and defer the abstract-core boundary,
but this would intentionally abandon the main goal of the change.

## Open Questions

- Should the testing fake store and fake sink remain under
  `durable_outbox.testing`, or should they move to a dedicated test-support
  package in a later change?
<!-- ITO:END -->
