## ADDED Requirements
### Requirement: File Sink Plugin Package
The file sink SHALL be distributed as an independent workspace package named `durable-outbox-file-sink` with import package `durable_outbox_file_sink`.

#### Scenario: package is installed
- **WHEN** application code imports `durable_outbox_file_sink`
- **THEN** it can construct `FileSink` directly

### Requirement: File Sink Entry Point
The file sink package SHALL register a `file` entry point in the `durable_outbox.sinks` group.

#### Scenario: file sink is loaded by configuration
- **WHEN** an application loads sink plugin `file` with a valid path
- **THEN** the loader returns a file sink that appends JSONL publish records

### Requirement: File Sink Semantics Preserved
The extracted file sink SHALL preserve existing JSONL encoding, Kafka-like partition/offset metadata, async close behavior, and fsync batching configuration.

#### Scenario: events are published to file
- **WHEN** two events are published through the extracted sink
- **THEN** the JSONL rows and returned offsets match the previous implementation behavior
