<!-- ITO:START -->
## Context

Durable outbox now exposes plugin entry point groups for sinks and stores. The
bundled file sink and SQL store packages prove the pattern, but third-party
authors still need an explicit guide for creating their own packages and
applications need clear install examples for local-path and registry-installed
plugins.

## Goals / Non-Goals

**Goals:**

- Document the public plugin authoring contract for sink and store packages.
- Cover package metadata, entry points, factory signatures, configuration, and
  verification.
- Show application install paths for pip registry packages and local path
  packages.
- Add tests that keep documentation examples aligned with the real plugin API.

**Non-Goals:**

- Add new plugin runtime behavior.
- Add a cookiecutter/scaffold command.
- Define provider-specific cloud SDK behavior beyond the existing provider
  contract guidance.

## Approach

Add a dedicated documentation page, likely
`durable-outbox-python/docs/plugin-authoring.md`, and link it from README and
provider docs. Use compact examples that are complete enough to copy into a
package but avoid duplicating full provider implementations.

The guide should cover two audiences:

- Plugin authors creating a package with `durable-outbox` as a dependency.
- Application authors installing plugins from a registry or local path and
  loading them by configured name.

## Contracts / Interfaces

- Sink entry point group: `durable_outbox.sinks`
- Store entry point group: `durable_outbox.stores`
- Sink loader: `load_sink(name, config)`
- Store loader: `load_store(name, config)`
- Sink protocol: `MessageSink`
- Store protocol: `DurableOutboxStore`
- Store verification helper: `ProviderContract` / `run_provider_contract`

## Data / State

No data or persistent state changes.

## Decisions

- Use documentation tests instead of executable tutorial tests for every code
  block. Rationale: the guide must cover package boundaries and install modes
  that are better verified by string/metadata checks plus existing plugin loader
  tests.
- Keep local-path and registry examples both in the guide. Rationale: plugin
  authors often test locally before publishing, while application teams need
  deployment-ready registry installs.
- Keep the guide package-manager neutral where possible, but include `uv`
  examples because this repository uses `uv` as the supported development
  workflow.

## Risks / Trade-offs

- Documentation can drift from loader names or entry point groups. Mitigation:
  add packaging docs tests that assert the guide mentions the actual entry point
  groups, loader functions, and bundled plugin package names.
- Full runnable plugin examples could become too long. Mitigation: use minimal
  snippets and point to bundled plugin packages as reference implementations.

## Verification Strategy

- Add docs tests for the new guide.
- Run `uv run pytest tests/test_packaging_docs.py tests/test_plugin_api.py`.
- Run full durable-outbox package gates before completion:
  `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`,
  `uv run ty check`, and `uv build --all-packages`.

## Migration / Rollback

This is documentation-only. Rollback is removing the guide and links.

## Open Questions

None.
<!-- ITO:END -->
