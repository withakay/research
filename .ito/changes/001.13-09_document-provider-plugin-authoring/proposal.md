<!-- ITO:START -->
## Why

The plugin packaging work made sinks and stores extensible, but the project does
not yet give provider authors a complete path from local plugin code to a
registry-published package. Without that guidance, users can load the bundled
plugins but must infer entry point names, factory signatures, local path install
flows, and verification commands from source code.

## What Changes

- Add detailed documentation for creating new sink and store plugin packages.
- Document both local plugin workflows and packages installed from a pip
  registry or local path.
- Cover plugin factory contracts, entry point groups, `pyproject.toml` metadata,
  dependency declaration, configuration handling, and verification commands.
- Add docs tests that keep plugin authoring examples aligned with the actual
  loader API and workspace packaging conventions.
- No runtime behavior change is intended.

## Change Shape

- **Type**: feature
- **Risk**: low
- **Stateful**: no
- **Public Contract**: config
- **Design Needed**: yes
- **Design Reason**: The documentation defines the public plugin authoring
  contract and should explicitly choose the examples, install modes, and
  verification strategy before implementation.

## Capabilities

### New Capabilities

- `durable-outbox-plugin-authoring`: Documents the public contract for third
  parties creating sink and store plugins for durable outbox.

### Modified Capabilities

- `durable-outbox-packaging`: Extends consumer packaging documentation to cover
  local-path and registry-installed plugin packages.

## Impact

- Affected docs: `durable-outbox-python/docs/providers.md`, README, and any new
  plugin authoring guide under `durable-outbox-python/docs/`.
- Affected tests: documentation/packaging tests that verify examples mention
  current entry point groups, package names, install modes, and loader calls.
- Affected APIs: none; the proposal documents the existing plugin API.
<!-- ITO:END -->
