## Context
`durable_outbox.stores.sql` currently contains SQL schema constants, store implementations, a client protocol, and in-memory client support used by tests. This is provider-specific and should become a separate installable package.

## Goals / Non-Goals
- Goals: package SQL stores independently; register plugin factories; keep provider contract coverage; remove SQL optional dependency from core.
- Non-Goals: change SQL lifecycle semantics; implement a real pyodbc client if not already present; keep old `durable_outbox.stores.sql` imports working.

## Decisions
- Decision: distribution name is `durable-outbox-sql-store`; import package is `durable_outbox_sql_store`.
- Decision: entry point names are `azure-sql-sync` and `sql-always-on`.
- Decision: direct constructors remain available from the plugin package for tests and custom wiring.
- Decision: SQL plugin owns `pyodbc` and any future SQL provider SDK dependencies.
- Alternative considered: keep SQL as a core extra. Rejected because the requested architecture is provider packages configured as plugins.

## Risks / Trade-offs
- SQL tests are deeply intertwined with shared adapter tests. Mitigate by moving SQL-specific tests to the package and keeping shared provider contract tests reusable.
- In-memory SQL client is useful for tests but provider-specific. Mitigate by exporting it from the plugin package testing surface rather than core.

## Migration Plan
1. Create the SQL workspace package with `uv_build`.
2. Move SQL implementation and SQL-focused tests.
3. Register plugin factories and update loader tests.
4. Remove core SQL imports and optional dependency metadata.
5. Verify provider contract coverage for both SQL modes.
