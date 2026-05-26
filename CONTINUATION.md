# Continuation Prompt

Continue the active goal:

> Review `durable-outbox-python/docs/reviews/multipass-review-2026-05-24.md` in detail, use subagents to review each subsection deeply, implement agreed fixes, make conventional commits as you go, and maintain `durable-outbox-python/docs/reviews/multipass-review-decisions-2026-05-25.md` with decisions, fixes, justifications, and verification.

Work in `/Users/jack/Code/withakay/research`. Stay inside `durable-outbox-python` unless explicitly needed. Use Python 3.14 with `uv`, Ruff, `ty`, strict typing, TDD, and conventional commits. Use subagents for deeper subsection review when useful.

Current state:

- Latest completed batch: P-P3-1, Blob split payload/state layout.
- Latest full gates for this batch:
  - `uv run pytest -q` -> `250 passed, 2 skipped`
  - `uv run ruff check .` -> passed
  - `uv run ruff format --check .` -> passed
  - `uv run ty check` -> passed
  - `uv build` -> passed
- Most recent commits:
  - `abf2108 perf(durable-outbox): delegate sql cosmos claim queries`
  - `75a7f40 feat(durable-outbox): wire settings and trace propagation`
  - `b6188f4 perf(durable-outbox): avoid blob content scans while claiming`
  - `4fb031e feat(durable-outbox): bound cleanup work per tick`

P-P3-1 implementation notes:

- Blob records now use split storage for new writes: raw payload bytes in `payload_blob_name(event_id)` and mutable state JSON in `state_blob_name(event_id)`.
- `event_blob_name(event_id)` remains the legacy full-record path and is still readable.
- State transitions update only the small state JSON and do not reupload payload bytes.
- Legacy records migrate on first state update by writing payload plus state while keeping the legacy blob as fallback.
- Cleanup removes split state and payload blobs; legacy cleanup remains supported.
- Decisions and verification are documented in `docs/reviews/multipass-review-decisions-2026-05-25.md`.

Remaining direct review IDs:

- `A-P0-1`
- `P-P0-2`
- `P-P0-5`
- `P-P1-1`

Suggested next move:

1. Confirm a clean worktree and rerun the remaining-ID script.
2. Pick the next bounded item. Likely candidates are:
   - `P-P1-1` failover replay streaming/concurrency, but check dependencies on provider pagination first.
3. Treat `A-P0-1`, `P-P0-2`, and `P-P0-5` as larger provider-client/query-track work; use subagents before implementing.
4. For every accepted finding: write or preserve red tests, implement, run focused gates, run full gates, update the decisions doc, then commit conventionally.
