# Continuation Prompt

Continue the active goal:

> Review `durable-outbox-python/docs/reviews/multipass-review-2026-05-24.md` in detail, use subagents to review each subsection deeply, implement agreed fixes, make conventional commits as you go, and maintain `durable-outbox-python/docs/reviews/multipass-review-decisions-2026-05-25.md` with decisions, fixes, justifications, and verification.

Work in `/Users/jack/Code/withakay/research`. Stay inside `durable-outbox-python` unless explicitly needed. Use Python 3.14 with `uv`, Ruff, `ty`, strict typing, TDD, and conventional commits. Use subagents for deeper subsection review when useful.

Current state:

- Latest completed batch: SQL/Cosmos claim query seam for P-P0-2/P-P0-5.
- Latest full gates for this batch:
  - `uv run pytest -q` -> `248 passed, 2 skipped`
  - `uv run ruff check .` -> passed
  - `uv run ruff format --check .` -> passed
  - `uv run ty check` -> passed
  - `uv build` -> passed
- Most recent commits:
  - `75a7f40 feat(durable-outbox): wire settings and trace propagation`
  - `b6188f4 perf(durable-outbox): avoid blob content scans while claiming`
  - `4fb031e feat(durable-outbox): bound cleanup work per tick`
  - `4a52984 perf(durable-outbox): index in-flight ordering keys`

SQL/Cosmos claim query seam notes:

- `SqlOutboxClient` and `CosmosOutboxClient` now expose `claim_batch_pending(limit, now, claim_timeout)`.
- SQL/Cosmos stores call that seam instead of calling `list_records()` directly for claim candidate selection.
- In-memory clients implement the seam over their dictionaries and include fresh ordered `IN_FLIGHT` blockers for ordering correctness.
- This is not the real backend work: `P-P0-2` and `P-P0-5` remain partially open until pyodbc/Cosmos SDK clients implement indexed provider queries.
- Decisions and verification are documented in `docs/reviews/multipass-review-decisions-2026-05-25.md`.

Remaining direct review IDs:

- `A-P0-1`
- `P-P0-2`
- `P-P0-5`
- `P-P1-1`
- `P-P3-1`

Suggested next move:

1. Confirm a clean worktree and rerun the remaining-ID script.
2. Pick the next bounded item. Likely candidates are:
   - `P-P1-1` failover replay streaming/concurrency, but check dependencies on provider pagination first.
   - `P-P3-1` encode/decode hot-path cleanup, likely small and independent.
3. Treat `A-P0-1`, `P-P0-2`, and `P-P0-5` as larger provider-client/query-track work; use subagents before implementing.
4. For every accepted finding: write or preserve red tests, implement, run focused gates, run full gates, update the decisions doc, then commit conventionally.
