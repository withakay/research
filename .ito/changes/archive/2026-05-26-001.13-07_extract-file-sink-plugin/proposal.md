# Change: Extract File Sink Plugin

## Why
The file sink is useful for local integration testing but should not live in the storage- and sink-agnostic core package. Extracting it first proves the plugin shape with a small provider.

## What Changes
- Create workspace package `durable-outbox-file-sink` with import package `durable_outbox_file_sink`.
- Move `FileSink` implementation and tests out of `durable_outbox.sinks.file`.
- Register a `file` sink entry point in `durable_outbox.sinks`.
- Remove `FileSink` from the core package and core `durable_outbox.sinks` exports.
- Update docs and examples to install and load the file sink plugin explicitly.

## Impact
- Affected specs: `durable-outbox-file-sink-plugin`, `durable-outbox-packaging`, `durable-outbox-plugin-api`
- Affected code: file sink package, core package exports, tests, README/provider docs, integration tests that use `FileSink`
- Sequencing: depends on uv workspace setup and provider plugin API.
