<!-- ITO:START -->
# Tasks for: 001.13-10_extract-remaining-providers-into-packages

## Execution Notes

- **Tracking**: Use `ito tasks` CLI for status updates.
- **Status legend**: `[ ] pending` · `[>] in-progress` · `[x] complete` · `[-] shelved`
- **TDD**: For each provider extraction, add or update failing tests before
  moving implementation code.

```bash
ito tasks status 001.13-10_extract-remaining-providers-into-packages
ito tasks next 001.13-10_extract-remaining-providers-into-packages
ito tasks start 001.13-10_extract-remaining-providers-into-packages 1.1
ito tasks complete 001.13-10_extract-remaining-providers-into-packages 1.1
```

______________________________________________________________________

## Wave 1

- **Depends On**: None

### Task 1.1: Add Abstract Core Boundary Tests

- **Files**: `durable-outbox-python/tests/test_packaging_docs.py`, `durable-outbox-python/tests/test_plugins.py`
- **Dependencies**: None
- **Action**: Add red tests proving core provider imports fail, core optional dependencies are absent from default metadata, and provider loading depends on installed provider packages.
- **Verify**: `cd durable-outbox-python && uv run pytest tests/test_packaging_docs.py tests/test_plugins.py -q`
- **Done When**: Tests fail for the current core-ships-providers layout for the intended assertions.
- **Requirements**: durable-outbox-abstract-core:abstract-core-package, durable-outbox-abstract-core:no-core-provider-compatibility-modules, durable-outbox-plugin-api:provider-plugin-discovery
- **Updated At**: 2026-05-26
- **Status**: [x] complete

### Task 1.2: Add Provider Package Metadata Tests

- **Files**: `durable-outbox-python/tests/test_packaging_docs.py`, `durable-outbox-python/tests/test_plugin_api.py`
- **Dependencies**: None
- **Action**: Add red tests for the expected first-party provider package matrix, package dependencies, typed exports, and plugin entry points.
- **Verify**: `cd durable-outbox-python && uv run pytest tests/test_packaging_docs.py tests/test_plugin_api.py -q`
- **Done When**: Tests fail until Kafka, memory, Blob, and Cosmos provider packages exist and register plugins.
- **Requirements**: durable-outbox-provider-packages:first-party-provider-package-matrix, durable-outbox-provider-packages:provider-package-plugin-registration
- **Updated At**: 2026-05-26
- **Status**: [x] complete

______________________________________________________________________

## Wave 2

- **Depends On**: Wave 1

### Task 2.1: Extract Kafka Sink Package

- **Files**: `durable-outbox-python/packages/durable-outbox-kafka-sink/**`, `durable-outbox-python/durable_outbox/sinks/**`, `durable-outbox-python/tests/test_kafka*.py`, `durable-outbox-python/pyproject.toml`
- **Dependencies**: None
- **Action**: Move Kafka sink implementation and dependencies into `durable-outbox-kafka-sink`, register the sink plugin, and update imports/tests.
- **Verify**: `cd durable-outbox-python && uv run pytest tests/test_kafka_operations.py tests/test_plugin_api.py -q`
- **Done When**: Kafka tests import from the provider package or plugin loader, and old core Kafka imports fail.
- **Requirements**: durable-outbox-abstract-core:abstract-core-package, durable-outbox-provider-packages:first-party-provider-package-matrix, durable-outbox-provider-packages:provider-package-plugin-registration
- **Updated At**: 2026-05-26
- **Status**: [>] in-progress

### Task 2.2: Extract Memory Store Package

- **Files**: `durable-outbox-python/packages/durable-outbox-memory-store/**`, `durable-outbox-python/durable_outbox/stores/memory.py`, `durable-outbox-python/tests/**/*.py`, `durable-outbox-python/pyproject.toml`
- **Dependencies**: None
- **Action**: Move `MemoryOutboxStore` into `durable-outbox-memory-store`, register the store plugin, and update tests/examples to use the package namespace.
- **Verify**: `cd durable-outbox-python && uv run pytest tests/test_core.py tests/test_plugins.py -q`
- **Done When**: Memory store behavior is unchanged through package import/plugin loading, and old core memory imports fail.
- **Requirements**: durable-outbox-abstract-core:abstract-core-package, durable-outbox-provider-packages:first-party-provider-package-matrix, durable-outbox-provider-packages:provider-package-plugin-registration
- **Updated At**: 2026-05-26
- **Status**: [>] in-progress

### Task 2.3: Extract Blob Store Package

- **Files**: `durable-outbox-python/packages/durable-outbox-blob-store/**`, `durable-outbox-python/durable_outbox/stores/azure_blob.py`, `durable-outbox-python/durable_outbox/stores/blob_geo.py`, `durable-outbox-python/tests/test_adapters.py`, `durable-outbox-python/tests/test_failover_ordering_cleanup.py`, `durable-outbox-python/pyproject.toml`
- **Dependencies**: None
- **Action**: Move Blob store/provider client implementation into `durable-outbox-blob-store`, register the store plugin, and update Blob tests/imports.
- **Verify**: `cd durable-outbox-python && uv run pytest tests/test_adapters.py tests/test_failover_ordering_cleanup.py -q`
- **Done When**: Blob behavior is unchanged through package imports/plugin loading, provider-specific Azure dependencies are package-local, and old core Blob imports fail.
- **Requirements**: durable-outbox-abstract-core:abstract-core-package, durable-outbox-provider-packages:first-party-provider-package-matrix, durable-outbox-provider-packages:provider-package-plugin-registration
- **Updated At**: 2026-05-26
- **Status**: [ ] pending

### Task 2.4: Extract Cosmos Store Package

- **Files**: `durable-outbox-python/packages/durable-outbox-cosmos-store/**`, `durable-outbox-python/durable_outbox/stores/cosmos.py`, `durable-outbox-python/durable_outbox/stores/cosmos_azure.py`, `durable-outbox-python/tests/test_cosmos*.py`, `durable-outbox-python/pyproject.toml`
- **Dependencies**: None
- **Action**: Move Cosmos store/provider client implementation into `durable-outbox-cosmos-store`, register the store plugin, and update Cosmos tests/imports.
- **Verify**: `cd durable-outbox-python && uv run pytest tests/test_cosmos_azure.py tests/test_adapters.py -q`
- **Done When**: Cosmos behavior is unchanged through package imports/plugin loading, Azure Cosmos dependencies are package-local, and old core Cosmos imports fail.
- **Requirements**: durable-outbox-abstract-core:abstract-core-package, durable-outbox-provider-packages:first-party-provider-package-matrix, durable-outbox-provider-packages:provider-package-plugin-registration
- **Updated At**: 2026-05-26
- **Status**: [ ] pending

______________________________________________________________________

## Wave 3

- **Depends On**: Wave 2

### Task 3.1: Remove Core Provider Surfaces

- **Files**: `durable-outbox-python/durable_outbox/sinks/**`, `durable-outbox-python/durable_outbox/stores/**`, `durable-outbox-python/durable_outbox/__init__.py`, `durable-outbox-python/pyproject.toml`
- **Dependencies**: None
- **Action**: Remove concrete provider modules, lazy provider exports, provider extras, and provider implementation paths from core package metadata while retaining abstract core exports.
- **Verify**: `cd durable-outbox-python && uv run pytest tests/test_packaging_docs.py tests/test_plugins.py -q`
- **Done When**: Core package contains no concrete sink/store implementation modules or provider-specific dependency declarations.
- **Requirements**: durable-outbox-abstract-core:abstract-core-package, durable-outbox-abstract-core:no-core-provider-compatibility-modules, durable-outbox-plugin-api:breaking-provider-imports
- **Updated At**: 2026-05-26
- **Status**: [ ] pending

### Task 3.2: Update Documentation and Migration Guide

- **Files**: `durable-outbox-python/README.md`, `durable-outbox-python/docs/plugin-authoring.md`, `durable-outbox-python/docs/providers.md`, `durable-outbox-python/packages/*/README.md`
- **Dependencies**: None
- **Action**: Document the abstract core boundary, first-party provider packages, old-to-new import migration examples, plugin loading examples, and workspace verification commands.
- **Verify**: `cd durable-outbox-python && uv run pytest tests/test_packaging_docs.py -q`
- **Done When**: Consumer docs explain how to install and use each provider package without importing concrete providers from core.
- **Requirements**: durable-outbox-packaging:consumer-documentation
- **Updated At**: 2026-05-26
- **Status**: [ ] pending

______________________________________________________________________

## Wave 4

- **Depends On**: Wave 3

### Task 4.1: Run Full Workspace Verification

- **Files**: `durable-outbox-python/**`
- **Dependencies**: None
- **Action**: Run the full durable outbox verification suite and fix any packaging, lint, format, typing, or build regressions.
- **Verify**: `cd durable-outbox-python && uv run pytest && uv run ruff check . && uv run ruff format --check . && uv run ty check && uv build --all-packages`
- **Done When**: Full tests, Ruff, format check, ty, and all-package build pass.
- **Requirements**: durable-outbox-abstract-core:abstract-core-package, durable-outbox-provider-packages:first-party-provider-package-matrix, durable-outbox-provider-packages:provider-package-plugin-registration, durable-outbox-plugin-api:provider-plugin-discovery, durable-outbox-plugin-api:breaking-provider-imports, durable-outbox-packaging:consumer-documentation
- **Updated At**: 2026-05-26
- **Status**: [ ] pending
<!-- ITO:END -->
