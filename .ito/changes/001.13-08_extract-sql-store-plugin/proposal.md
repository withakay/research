# Change: Extract SQL Store Plugin

## Why
SQL store support carries provider-specific schema, client protocols, and optional dependencies that should be independent from the core durable outbox package. Extracting it proves non-trivial store plugin packaging after the plugin API exists.

## What Changes
- Create workspace package `durable-outbox-sql-store` with import package `durable_outbox_sql_store`.
- Move SQL store classes, schema constants, configuration dataclasses, in-memory SQL test client, and tests into the plugin package.
- Register SQL store entry points for `azure-sql-sync` and `sql-always-on`.
- Remove SQL store modules and exports from the core package with no compatibility wrappers.
- Move SQL optional dependencies and SQL provider docs to the plugin package where appropriate.

## Impact
- Affected specs: `durable-outbox-sql-store`, `durable-outbox-sql-provider`, `durable-outbox-packaging`, `durable-outbox-plugin-api`
- Affected code: SQL store package, core package exports, provider contract tests, README/provider docs, lockfile
- Sequencing: depends on uv workspace setup and provider plugin API; should run after file sink extraction has proven the plugin path.
