# Continuation Prompt

Continue the active goal:

> Review `durable-outbox-python/docs/reviews/multipass-review-2026-05-24.md` in detail, use subagents to review each subsection deeply, implement agreed fixes, make conventional commits as you go, and maintain `durable-outbox-python/docs/reviews/multipass-review-decisions-2026-05-25.md` with decisions, fixes, justifications, and verification.

Work in `/Users/jack/Code/withakay/research`. Stay inside `durable-outbox-python` unless explicitly needed. Use Python 3.14 with `uv`, Ruff, `ty`, strict typing, TDD, and conventional commits. Use subagents for deeper subsection review when useful.

Current state:

- Latest completed batch: P-P1-4, in-flight ordering key fast path.
- Latest full gates for P-P1-4:
  - `uv run pytest -q` -> `234 passed, 2 skipped`
  - `uv run ruff check .` -> passed
  - `uv run ruff format --check .` -> passed
  - `uv run ty check` -> passed
  - `uv build` -> passed
- Most recent commits:
  - `5a551b6 test(durable-outbox): capture ordering index expectations`
  - `e2cc956 perf(durable-outbox): cache blob event fingerprints`
  - `33db8ab perf(durable-outbox): avoid blob claim record clones`
  - `1314288 fix(durable-outbox): synchronize in-memory metrics`

P-P1-4 implementation notes:

- `InFlightOrderingIndex` was added in `durable_outbox/core/claim.py`.
- `MemoryOutboxStore` uses it as local authoritative process state.
- `BlobOutboxStore` uses it as a local fast path while keeping blob ordering locks authoritative.
- SQL/Cosmos do not use local authoritative indexes; `_claim_from_candidates()` reuses the already-fetched candidate list to avoid a second `list_records()` scan.
- Decisions and verification are documented in `docs/reviews/multipass-review-decisions-2026-05-25.md`.

Remaining direct review IDs:

- `A-P0-1`
- `A-P3-1`
- `P-P0-2`
- `P-P0-4`
- `P-P0-5`
- `P-P1-1`
- `P-P1-6`
- `P-P3-1`

Suggested next move:

1. Confirm a clean worktree and rerun the remaining-ID script.
2. Pick the next bounded item. Likely candidates are:
   - `P-P1-6` cleanup/replay batching/scheduler, possibly split into a smaller cleanup scheduler commit first.
   - `P-P1-1` failover replay streaming/concurrency, but check dependencies on provider pagination first.
3. Treat `A-P0-1`, `P-P0-2`, `P-P0-5`, and likely `P-P0-4` as larger provider-client/query-track work; use subagents before implementing.
4. For every accepted finding: write or preserve red tests, implement, run focused gates, run full gates, update the decisions doc, then commit conventionally.
