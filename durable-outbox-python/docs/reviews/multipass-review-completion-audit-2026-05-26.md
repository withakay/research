# Multipass Review Completion Audit

**Date:** 2026-05-26
**Scope:** `docs/reviews/multipass-review-2026-05-24.md`

This audit summarizes the final state after implementing the accepted findings
from the multipass review. Detailed decisions, red/green notes, and command
evidence live in `multipass-review-decisions-2026-05-25.md`.

## Objective Coverage

| Requirement | Evidence | Status |
|---|---|---|
| Review the multipass report in detail | Decision log batches 1-89 cover architecture, security, performance, quality, provider, replay, Aspire, and follow-up subagent findings. | Complete |
| Use subagents for deeper subsection review | SQL, Cosmos, and replay/Aspire subagents reviewed the remaining provider and replay sections; their findings drove batches 87-89. | Complete |
| Implement agreed fixes | Accepted local code findings were implemented across core, stores, sinks, provider clients, integration tests, docs, and demo tooling. | Complete |
| Use TDD/red-green where defects were found | Recent red evidence includes pyodbc idempotent upsert, SQL duplicate validation, Cosmos bounded claim paging, Aspire replay isolation, and Blob replay metadata scans. | Complete |
| Run strict quality gates | Latest full gates: `uv run pytest -q` -> 306 passed, 8 skipped; `uv run ruff check .`; `uv run ruff format --check .`; `uv run ty check`; `uv build`. | Complete |
| Produce decisions/fixes document | `docs/reviews/multipass-review-decisions-2026-05-25.md` records accepted findings, fixes, verification, and deferred external evidence. | Complete |
| Commit conventionally as work progresses | Latest commits include `fix(durable-outbox): harden provider claim completion`, `test(durable-outbox): certify aspire replay path`, and `perf(durable-outbox): stream blob replay metadata scans`. | Complete |

## Remaining Review IDs

### A-P0-1

Local implementation is complete for the reviewed scope:

- SQL stores require explicit clients instead of silently using in-memory
  defaults.
- `PyodbcSqlOutboxClient` implements persistence, idempotent insert-or-return
  upsert, cleanup freeze state, SQL durability checks, bounded candidate
  queries, atomic normal claims, and atomic failover replay claims.
- Cosmos stores require explicit clients instead of silently using in-memory
  defaults.
- `AzureCosmosOutboxClient` implements SDK-backed point operations, account
  validation, persisted partition registry, event index, bounded
  partition-scoped claim queries, paged replay iteration, cleanup candidates,
  and event-index repair.
- Opt-in live SQL and Cosmos test suites exist and cover the provider paths
  that require real services.

Residual evidence is external: live SQL Server/Azure SQL and Azure Cosmos tests
skip unless provider credentials and service endpoints are configured.

### P-P0-2

Closed locally. SQL normal claims use the pyodbc atomic `UPDATE ... OUTPUT`
path when available; replay claims also use a provider-native
`UPDATE ... OUTPUT INSERTED.*, DELETED.status AS source_status` path. The latest
provider hardening also made `upsert_new()` idempotent under concurrent insert
races.

### P-P0-5

Closed locally. Cosmos normal claim no longer performs a cross-partition
`list_records()` scan, and the Azure client now bounds claim query consumption
per known partition instead of reading every SDK query page. Replay has separate
paged iterator coverage.

### P-P1-1

Closed locally for built-in providers. `FailoverReplayer` consumes async replay
streams in bounded pages, SQL and Cosmos expose provider-native streaming paths,
and Blob replay now uses metadata-only listing before loading claimable records.
Aspire now certifies the local Azurite/Kafka topology with replay coverage.

Optional future optimization: a long-lived SQL replay cursor with explicit
backend batch-token rollback could reduce round trips for very large failovers,
but the current atomic replay claim path satisfies the reviewed correctness and
memory-safety concern.

## External Evidence Still Needed

The only remaining non-local evidence is live-provider execution:

- `DURABLE_OUTBOX_SQL_LIVE=1` with SQL Server/Azure SQL settings.
- `DURABLE_OUTBOX_COSMOS_LIVE=1` with Azure Cosmos settings.

Those gates are intentionally skipped in normal local runs and documented in
`docs/providers.md`.
