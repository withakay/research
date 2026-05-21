<!-- ITO:START -->
## Approach

Keep docs close to the package. The README covers installation and first use; focused docs cover RPO=0 providers, operations, and testing. Packaging checks run under `uv`.

## Contracts / Interfaces

- README commands match `pyproject.toml`.
- Docs state that RPO=0 is an adapter acceptance contract.
- License metadata has a corresponding `LICENSE` file.

## Verification Strategy

Run formatting/type/test checks and `uv build` once packaging metadata is complete.
<!-- ITO:END -->
