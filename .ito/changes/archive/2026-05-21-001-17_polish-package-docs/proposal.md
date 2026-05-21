<!-- ITO:START -->
## Why

The package is configured and tested, but the README and packaging metadata do not yet explain usage, optional extras, RPO=0 caveats, or contribution workflow.

## What Changes

- Expand README with quickstart, dispatcher example, adapter configuration examples, and test commands.
- Add RPO=0 documentation for Blob, Cosmos, and SQL modes.
- Add optional dependency install examples for Kafka, Azure, and SQL.
- Add license file matching `pyproject.toml`.
- Add packaging checks for build metadata and typed package marker.

## Change Shape

- **Type**: documentation
- **Risk**: low
- **Stateful**: no
- **Public Contract**: docs
- **Design Needed**: yes
- **Design Reason**: Documentation must distinguish certified from non-certified durability modes.

## Capabilities

### New Capabilities

- `durable-outbox-packaging`: Documentation and packaging polish for consumers.

## Impact

Consumers can evaluate and use the package without reading proposal internals.
<!-- ITO:END -->
