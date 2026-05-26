# Continuation Prompt

Continue the active goal:

> Review `durable-outbox-python/docs/reviews/multipass-review-2026-05-24.md` in detail, use subagents to review each subsection deeply, implement agreed fixes, make conventional commits as you go, and maintain `durable-outbox-python/docs/reviews/multipass-review-decisions-2026-05-25.md` with decisions, fixes, justifications, and verification.

Work in `/Users/jack/Code/withakay/research`. Follow repo guidance: stay inside `durable-outbox-python` unless explicitly needed, use Python 3.14 with `uv`, Ruff, `ty`, strict typing, and conventional commits. Use subagents for deeper subsection review when useful.

Current state:

- Last completed and committed batch: `e2cc956 perf(durable-outbox): cache blob event fingerprints`.
- Recent good full gates after that batch:
  - `uv run pytest -q` -> `231 passed, 2 skipped`
  - `uv run ruff check .` -> passed
  - `uv run ruff format --check .` -> passed
  - `uv run ty check` -> passed
  - `uv build` -> passed
- Remaining direct review IDs in the decisions log after P-P1-3:
  - `A-P0-1`
  - `A-P3-1`
  - `P-P0-2`
  - `P-P0-4`
  - `P-P0-5`
  - `P-P1-1`
  - `P-P1-4`
  - `P-P1-6`
  - `P-P3-1`

Current WIP:

- P-P1-4 is in progress.
- Uncommitted work before this handoff added red/expectation tests only:
  - `tests/test_core.py` imports and tests an intended `InFlightOrderingIndex`.
  - `tests/test_adapters.py` adds `CountingCosmosClient`, `CountingSqlClient`, and `test_sql_and_cosmos_claim_reuses_claim_candidate_list`.
- These tests likely fail until implementation exists. Treat them as TDD scaffolding, not completed feature work.

Subagent guidance for P-P1-4:

- Preserve scoped ordering keys via `ordering_scope(event)`, not raw `ordering_key`.
- Only fresh `IN_FLIGHT` ordered events block a key. Stale claims, pending, sent, failed, and unordered events must not block.
- Within one `claim_batch`, a successful claim must immediately block later same-key candidates.
- Do not index before durable/CAS writes succeed; failed CAS/blob precondition must not leak a blocked key.
- Release/prune on every exit from `IN_FLIGHT`: sent, retryable pending, failed, admin replay, and stale timeout.
- Local indexes are safe for `MemoryOutboxStore` and useful as a Blob fast path only. Blob ordering locks remain authoritative.
- Avoid per-adapter local indexes as authoritative for SQL/Cosmos; for now, reuse already-fetched claim candidate lists to avoid duplicate `list_records()` scans, but do not introduce local cross-instance state that can miss claims from another store instance.

Suggested next steps:

1. Run the focused WIP tests to confirm the current red state:
   - `cd durable-outbox-python`
   - `uv run pytest tests/test_core.py::test_in_flight_ordering_index_tracks_releases_and_prunes_stale_claims tests/test_adapters.py::test_sql_and_cosmos_claim_reuses_claim_candidate_list -q`
2. Implement `InFlightOrderingIndex` in `durable_outbox/core/claim.py` for memory/blob-safe indexing.
3. Wire memory/blob carefully, preserving Blob lease authority.
4. For SQL/Cosmos, first make `_claim_from_candidates()` compute locked keys from the provided candidate list instead of calling `list_records()` a second time. Do not make a per-store local index authoritative for shared-client semantics.
5. Run focused adapter/provider tests, then full gates.
6. Update `docs/reviews/multipass-review-decisions-2026-05-25.md` with P-P1-4 decisions and verification.
7. Commit conventionally.
