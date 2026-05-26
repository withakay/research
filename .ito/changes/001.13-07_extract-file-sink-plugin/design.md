## Context
`FileSink` currently lives under `durable_outbox.sinks.file` and is used by tests and local integration demos. It has no external runtime dependencies, making it the lowest-risk first extraction.

## Goals / Non-Goals
- Goals: package the file sink independently; register it as plugin name `file`; keep its direct constructor available from `durable_outbox_file_sink`; update tests and docs.
- Non-Goals: preserve `durable_outbox.sinks.file`; change JSONL format or fsync semantics; extract Kafka in this change.

## Decisions
- Decision: distribution name is `durable-outbox-file-sink`; import package is `durable_outbox_file_sink`.
- Decision: plugin factory name is `file` and accepts path/fsync-related config matching the existing constructor.
- Decision: direct plugin package imports are allowed for tests and advanced users, while app configuration should prefer the core plugin loader.

## Risks / Trade-offs
- Existing tests and demos import the old module. Mitigate by updating all references in this change.
- The plugin package depends on core model/protocol types. Mitigate by declaring `durable-outbox` as a workspace dependency.

## Migration Plan
1. Add workspace package and move implementation.
2. Register entry point and factory.
3. Remove core export/import path.
4. Update tests, docs, and integration demo imports.
