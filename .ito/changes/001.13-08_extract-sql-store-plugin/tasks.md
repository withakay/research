## 1. Implementation
- [x] 1.1 Create `packages/durable-outbox-sql-store` using `uv_build`.
- [x] 1.2 Move SQL store implementation, schema constants, config, client protocol, and in-memory SQL client into `durable_outbox_sql_store`.
- [x] 1.3 Add `azure-sql-sync` and `sql-always-on` store entry point factories.
- [ ] 1.4 Remove old core SQL module and store exports.
- [ ] 1.5 Move or split SQL-specific tests into the SQL plugin package.
- [ ] 1.6 Update README, provider docs, optional dependency metadata, and lockfile.

## 2. Verification
- [ ] 2.1 Run provider contract tests for `AzureSqlSyncOutboxStore` and `SqlAlwaysOnOutboxStore` from the plugin package.
- [ ] 2.2 Run plugin loader tests proving both SQL store names load successfully.
- [ ] 2.3 Run `uv run pytest`.
- [ ] 2.4 Run `uv run ruff check .` and `uv run ty check`.
- [ ] 2.5 Run `uv build --all-packages`.
