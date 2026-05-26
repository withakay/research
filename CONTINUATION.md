# Continuation Prompt

Continue the active goal:

> Review `durable-outbox-python/docs/reviews/multipass-review-2026-05-24.md` in detail, use subagents to review each subsection deeply, implement agreed fixes, make conventional commits as you go, and maintain `durable-outbox-python/docs/reviews/multipass-review-decisions-2026-05-25.md` with decisions, fixes, justifications, and verification.

Work in `/Users/jack/Code/withakay/research`. Stay inside `durable-outbox-python` unless explicitly needed. Use Python 3.14 with `uv`, Ruff, `ty`, strict typing, TDD, and conventional commits. Use subagents for deeper subsection review when useful.

Current state:

- Latest completed batch: Provider completion audit and bounded claims.
- Latest full gates:
  - `uv run pytest -q` -> `305 passed, 8 skipped`
  - `uv run ruff check .` -> passed
  - `uv run ruff format --check .` -> passed
  - `uv run ty check` -> passed
  - `uv build` -> passed
- Latest focused gates:
  - `uv run pytest tests/test_sql_pyodbc.py tests/test_cosmos_azure.py tests/test_failover_ordering_cleanup.py tests/integration/test_aspire_azurite_kafka.py -q` -> `75 passed, 3 skipped`
  - `uv run ruff check durable_outbox/stores/sql.py durable_outbox/stores/sql_pyodbc.py durable_outbox/stores/cosmos_azure.py tests/test_sql_pyodbc.py tests/test_cosmos_azure.py tests/integration/test_aspire_azurite_kafka.py` -> passed
  - `uv run ty check` -> passed

Recent implementation notes:

- `AzureCosmosOutboxClient` now exists as a lazy optional Azure SDK-backed
  client with snake_case JSON encode/decode, `_etag` conflict mapping,
  cleanup-freeze control items, `read_account()` validation, and bounded
  partition-scoped candidate queries for claim, failover replay, and cleanup.
  It persists observed partitions into a control-partition registry and loads
  that registry before candidate queries. It also writes a create-only
  control-partition event index for restart-safe event-id lookup and duplicate
  detection. It is intentionally not in the provider-contract matrix yet because
  live Azure Cosmos integration coverage is still open. It also exposes
  `repair_event_index()` for reserved-index/event-missing crash windows.
- Opt-in live provider certification tests now exist for SQL Server/pyodbc and
  Azure Cosmos. They skip in normal runs unless `DURABLE_OUTBOX_SQL_LIVE=1` or
  `DURABLE_OUTBOX_COSMOS_LIVE=1` plus provider connection settings are present.
  They now cover SQL atomic replay claims and Cosmos paged replay iteration.
  The SQL path also decodes real pyodbc row shapes via `cursor_description`.
- `PyodbcSqlOutboxClient` now exists as a lazy optional SQL provider slice for
  persistence primitives, SQL durability checks, cleanup freeze state, strict
  row encode/decode, and bounded candidate queries for normal claim, failover
  replay, and cleanup. Normal dispatcher claim now uses an optional SQL Server
  `UPDATE ... OUTPUT INSERTED.*` atomic claim path when the pyodbc client is
  wired. Pyodbc failover replay claims now also use a SQL Server
  `UPDATE ... OUTPUT` path that returns `DELETED.status AS source_status` for
  replay metrics and warnings.
- `FailoverReplayer` now fetches bounded replay pages, supports opt-in page publish concurrency, and passes already-seen event IDs to stores.
- `FailoverReplayer` now also consumes stores that expose
  `iter_failover_replay_candidates(...)` as an async iterator, collecting only
  bounded `replay_page_size` chunks before publishing.
- SQL and Cosmos stores now expose that streaming replay shape, so the replayer
  does not fall back to the legacy list-returning method for those built-in
  providers.
- Blob and dual-region Blob stores now also expose that streaming replay shape.
- `AzureCosmosOutboxClient` now exposes `iter_failover_replay_candidates()` and
  streams replay candidates from SDK query pages with a bounded cross-partition
  merge instead of materializing all known partition results before yielding.
- Store failover replay candidate methods accept `exclude_event_ids`.
- SQL and Cosmos normal claim paths delegate to `claim_batch_pending()`.
- SQL and Cosmos failover replay paths delegate to `list_failover_replay_candidates()` instead of `list_records()`.
- Blob records now use split storage for new writes: raw payload bytes in `payload_blob_name(event_id)` and mutable state JSON in `state_blob_name(event_id)`.
- Decisions and verification are documented in `durable-outbox-python/docs/reviews/multipass-review-decisions-2026-05-25.md`.
- Recent subagent review found and fixed two provider gaps: pyodbc
  `upsert_new()` now uses `UPDLOCK, HOLDLOCK` insert-or-return-existing
  semantics, and Azure Cosmos claim candidate reads are hard-bounded per
  partition instead of consuming every SDK query page.
- An Aspire/Azurite integration test now covers `FailoverReplayer` replaying
  both `PENDING` and previously `SENT` Blob events to `FileSink`.

Remaining direct review IDs:

- `A-P0-1`
- `P-P1-1`

Suggested next move:

1. Confirm a clean worktree and rerun the remaining-ID script.
2. Pick the next bounded item. Likely candidates are:
   - `A-P0-1`: run live-account SQL/Cosmos integration tests when credentials/services are available.
   - `P-P1-1`: built-in stores now expose replay streaming; remaining work is
     live-service certification plus optional deeper Blob provider streaming and
     SQL batch-token cursor optimization.
3. Treat remaining provider certification as evidence-gathering unless new
   review identifies a concrete code gap.
4. For every accepted finding: write or preserve red tests, implement, run focused gates, run full gates, update the decisions doc, then commit conventionally.

Aspire note:

- `ASPIRE_CONTAINER_RUNTIME=podman ./demos/scripts/run_aspire_azurite_kafka_demo.sh`
  currently starts the AppHost and reports healthy Blob/Kafka resources, but the
  Python integration resource exits 1. The latest wrapper log points at an
  Aspire CLI log named `cli_20260526T185048_740fd18b.log`, which shows rdkafka
  broker-down messages and does not include pytest stdout/stderr. Next useful
  fix is to improve demo resource-log capture or remediate Kafka
  readiness/connection-string handling.
