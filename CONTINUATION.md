# Continuation Prompt

Continue the active goal:

> Review `durable-outbox-python/docs/reviews/multipass-review-2026-05-24.md` in detail, use subagents to review each subsection deeply, implement agreed fixes, make conventional commits as you go, and maintain `durable-outbox-python/docs/reviews/multipass-review-decisions-2026-05-25.md` with decisions, fixes, justifications, and verification.

Work in `/Users/jack/Code/withakay/research`. Stay inside `durable-outbox-python` unless explicitly needed. Use Python 3.14 with `uv`, Ruff, `ty`, strict typing, TDD, and conventional commits. Use subagents for deeper subsection review when useful.

Current state:

- Latest completed batch: Azure Cosmos event index repair.
- Latest full gates:
  - `uv run pytest -q` -> `293 passed, 2 skipped`
  - `uv run ruff check .` -> passed
  - `uv run ruff format --check .` -> passed
  - `uv run ty check` -> passed
  - `uv build` -> passed
- Most recent commits:
  - `1ac5ed3 perf(durable-outbox): delegate replay candidate queries`
  - `06c369f perf(durable-outbox): page failover replay batches`
  - `053ca5d perf(durable-outbox): split blob payload and state writes`
  - `abf2108 perf(durable-outbox): delegate sql cosmos claim queries`

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
- `PyodbcSqlOutboxClient` now exists as a lazy optional SQL provider slice for
  persistence primitives, SQL durability checks, cleanup freeze state, strict
  row encode/decode, and bounded candidate queries for normal claim, failover
  replay, and cleanup. Normal dispatcher claim now uses an optional SQL Server
  `UPDATE ... OUTPUT INSERTED.*` atomic claim path when the pyodbc client is
  wired. Replay claim remains candidate-plus-CAS so rollback semantics are not
  weakened.
- `FailoverReplayer` now fetches bounded replay pages, supports opt-in page publish concurrency, and passes already-seen event IDs to stores.
- `FailoverReplayer` now also consumes stores that expose
  `iter_failover_replay_candidates(...)` as an async iterator, collecting only
  bounded `replay_page_size` chunks before publishing.
- Store failover replay candidate methods accept `exclude_event_ids`.
- SQL and Cosmos normal claim paths delegate to `claim_batch_pending()`.
- SQL and Cosmos failover replay paths delegate to `list_failover_replay_candidates()` instead of `list_records()`.
- Blob records now use split storage for new writes: raw payload bytes in `payload_blob_name(event_id)` and mutable state JSON in `state_blob_name(event_id)`.
- Decisions and verification are documented in `durable-outbox-python/docs/reviews/multipass-review-decisions-2026-05-25.md`.

Remaining direct review IDs:

- `A-P0-1`
- `P-P0-2`
- `P-P0-5`
- `P-P1-1`

Suggested next move:

1. Confirm a clean worktree and rerun the remaining-ID script.
2. Pick the next bounded item. Likely candidates are:
   - `A-P0-1`: add live-account integration tests for SQL/Cosmos provider clients when credentials/services are available.
   - `P-P0-2`: live SQL Server integration and any provider-native replay
     claim/rollback design after the pyodbc normal-claim atomic path.
   - `P-P0-5`: live Cosmos integration coverage for the partition registry,
     event index, conditional commits, and repair behavior.
   - `P-P1-1`: provider-native async replay iterators/cursors for SQL, Cosmos,
     or Blob. The replayer can consume the optional stream shape, but built-in
     stores still need backend-specific implementations.
3. Treat `A-P0-1`, `P-P0-2`, and `P-P0-5` as larger provider-client/query-track work; use subagents before implementing.
4. For every accepted finding: write or preserve red tests, implement, run focused gates, run full gates, update the decisions doc, then commit conventionally.
