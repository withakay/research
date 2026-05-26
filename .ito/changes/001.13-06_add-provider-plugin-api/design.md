## Context
The existing protocols already separate store and sink behavior. The missing piece is configuration-time discovery of packages that provide concrete factories.

## Goals / Non-Goals
- Goals: discover installed plugins by entry point; load named store/sink factories with plain mapping config; expose available plugin names; fail clearly on missing or invalid plugins.
- Non-Goals: implement a full dependency injection container; preserve old provider module imports; support remote plugin installation at runtime.

## Decisions
- Decision: use Python package entry points via `importlib.metadata.entry_points`.
- Decision: define two groups: `durable_outbox.stores` and `durable_outbox.sinks`.
- Decision: factories accept a `Mapping[str, object]` and return a protocol implementation.
- Decision: loader errors use existing `ConfigurationError` so callers do not need a new error taxonomy.
- Alternative considered: provider registry populated by imports. Rejected because it requires importing provider packages before configuration.

## Risks / Trade-offs
- Entry points are only visible for installed packages, not arbitrary source directories. Mitigate by using uv workspace installs for local development.
- Mapping-based config is less strongly typed than direct constructors. Mitigate by leaving validation in each plugin factory and keeping direct constructors available inside plugin packages.

## Migration Plan
1. Add core plugin discovery and loader tests with temporary entry points or monkeypatched metadata.
2. Document entry point groups and factory contracts.
3. Export loader functions from the core package as the new provider configuration path.
