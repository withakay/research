<!-- ITO:START -->
## MODIFIED Requirements

### Requirement: Provider Plugin Discovery
The core package SHALL discover store and sink provider factories from
installed Python package entry points without importing provider implementation
modules eagerly. Discovery SHALL include all installed first-party provider
packages and SHALL NOT depend on provider modules being present in the core
package namespace.

- **Requirement ID**: durable-outbox-plugin-api:provider-plugin-discovery

#### Scenario: plugin package is installed

- **WHEN** an application asks for available stores or sinks
- **THEN** the installed provider names are listed from entry point metadata

#### Scenario: only core package is installed

- **WHEN** an application asks for available stores or sinks
- **THEN** no concrete first-party provider is reported unless its provider
  package is installed

### Requirement: Breaking Provider Imports
The core package SHALL NOT preserve compatibility modules for provider
implementations extracted into plugin packages. Extracted providers SHALL be
imported from their provider package namespace or loaded through the plugin API.

- **Requirement ID**: durable-outbox-plugin-api:breaking-provider-imports

#### Scenario: old provider module import is attempted

- **WHEN** code imports an extracted provider from the core package namespace
- **THEN** the import fails rather than silently loading compatibility code

#### Scenario: provider package import is used

- **WHEN** code imports an extracted provider from its provider package
  namespace
- **THEN** the import succeeds when that provider package is installed
<!-- ITO:END -->
