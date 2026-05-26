# Durable Outbox Plugin Packaging Ito Archive - 2026-05-26

This document records the completed durable outbox plugin packaging Ito changes
archived on 2026-05-26. The Ito archive and reconciled specs live on the
coordination branch, while this file provides the repo-tracked archive note on
`main`.

## Archived Changes

### `001.13-05_migrate-to-uv-workspace-build`

Migrated the durable outbox project to a uv workspace build with package-local
metadata and strict tool configuration.

Affected areas:
- Workspace packaging and lockfile management
- Package build metadata
- Strict typing and lint configuration

### `001.13-06_add-provider-plugin-api`

Added the public provider plugin API so external packages can register sinks
and stores through typed plugin descriptors.

Affected areas:
- Provider plugin protocols
- Plugin discovery and validation
- Public API exports

### `001.13-07_extract-file-sink-plugin`

Extracted the file sink into its own provider package while preserving
compatibility through explicit plugin registration.

Affected areas:
- File sink package metadata
- Sink plugin registration
- Plugin packaging tests

### `001.13-08_extract-sql-store-plugin`

Extracted SQL store/provider packaging so SQL support can ship and evolve as a
separate provider package.

Affected areas:
- SQL store package metadata
- SQL provider exports
- Provider package tests

### `001.13-09_document-provider-plugin-authoring`

Added documentation for creating local and external sink/store provider
plugins, including examples for editable installs, package entry points, and
registry distribution.

Affected areas:
- `docs/plugin-authoring.md`
- Provider package README links
- Documentation packaging checks

## Archive Evidence

Ito reported all five changes archived and synchronized to the coordination
branch. `ito list --completed --json` is empty after the archive, and
`ito list-archive --json` includes each change listed above.
