## ADDED Requirements
### Requirement: Provider Plugin Discovery
The core package SHALL discover store and sink provider factories from installed Python package entry points without importing provider implementation modules eagerly.

#### Scenario: plugin package is installed
- **WHEN** an application asks for available stores or sinks
- **THEN** the installed provider names are listed from entry point metadata

### Requirement: Provider Plugin Loading
The core package SHALL load a named store or sink factory from configuration and SHALL return objects that satisfy the existing durable outbox protocols.

#### Scenario: sink plugin is configured
- **WHEN** `file` is loaded from the sink plugin group with valid configuration
- **THEN** the returned object satisfies `MessageSink`

#### Scenario: store plugin is missing
- **WHEN** a configured store name has no installed entry point
- **THEN** loading fails with `ConfigurationError` that names the missing plugin

### Requirement: Breaking Provider Imports
The core package SHALL NOT preserve compatibility modules for provider implementations extracted into plugin packages.

#### Scenario: old provider module import is attempted
- **WHEN** code imports an extracted provider from the core package namespace
- **THEN** the import fails rather than silently loading compatibility code
