## 1. Implementation
- [x] 1.1 Create `packages/durable-outbox-file-sink` using `uv_build`.
- [x] 1.2 Move `FileSink` and helpers to `durable_outbox_file_sink`.
- [x] 1.3 Add `file` sink entry point and factory function.
- [x] 1.4 Remove old core file sink module and exports.
- [x] 1.5 Update tests, integration demos, README, and provider docs.

## 2. Verification
- [x] 2.1 Run file sink package tests through workspace pytest.
- [x] 2.2 Run plugin loader tests proving `load_sink("file", ...)` works.
- [x] 2.3 Run `uv run pytest`.
- [ ] 2.4 Run `uv run ruff check .` and `uv run ty check`.
- [ ] 2.5 Run `uv build --all-packages`.
