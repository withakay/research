## 1. Implementation
- [x] 1.1 Add plugin factory protocols and loader functions to core.
- [x] 1.2 Add `available_stores()` and `available_sinks()` helpers.
- [x] 1.3 Convert missing plugin, duplicate plugin, and invalid factory outcomes to `ConfigurationError`.
- [x] 1.4 Export the plugin API from `durable_outbox` or document its submodule import path.
- [x] 1.5 Document plugin package authoring and configuration examples.

## 2. Verification
- [ ] 2.1 Add tests for successful store and sink plugin loading.
- [ ] 2.2 Add tests for missing and invalid plugin configuration errors.
- [ ] 2.3 Run `uv run pytest`.
- [ ] 2.4 Run `uv run ruff check .` and `uv run ty check`.
