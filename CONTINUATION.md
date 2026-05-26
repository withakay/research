# Continuation Prompt

Continue the active goal:

> Review `durable-outbox-python/docs/reviews/multipass-review-2026-05-24.md` in detail, use subagents to review each subsection deeply, implement agreed fixes, make conventional commits as you go, and maintain `durable-outbox-python/docs/reviews/multipass-review-decisions-2026-05-25.md` with decisions, fixes, justifications, and verification.

Work in `/Users/jack/Code/withakay/research`. Stay inside `durable-outbox-python` unless explicitly needed. Use Python 3.14 with `uv`, Ruff, `ty`, strict typing, TDD, and conventional commits. Use subagents for deeper subsection review when useful.

Current state:

- Latest completed batch: P-P1-6, bounded cleanup scheduler.
- Latest full gates for P-P1-6:
  - `uv run pytest -q` -> `244 passed, 2 skipped`
  - `uv run ruff check .` -> passed
  - `uv run ruff format --check .` -> passed
  - `uv run ty check` -> passed
  - `uv build` -> passed
- Most recent commits:
  - `4a52984 perf(durable-outbox): index in-flight ordering keys`
  - `5a551b6 test(durable-outbox): capture ordering index expectations`
  - `e2cc956 perf(durable-outbox): cache blob event fingerprints`
  - `33db8ab perf(durable-outbox): avoid blob claim record clones`

P-P1-6 implementation notes:

- `DurableOutboxStore.cleanup_sent` now accepts optional `batch_size` and `max_per_tick`.
- `CleanupPolicy` now has `interval`, `batch_size`, and `max_per_tick`.
- `CleanupScheduler` in `durable_outbox/core/cleanup.py` provides `run_once()` and `run_until_stopped()`.
- Memory cleanup honors `max_per_tick`.
- Blob cleanup bounds candidates with `max_per_tick` and deletes in `batch_size` `asyncio.gather` chunks.
- SQL/Cosmos in-memory clients expose bounded cleanup candidate helpers; cleanup no longer calls `list_records()`.
- `failover_replay_candidates` pagination was intentionally deferred because it mutates claim state and rollback semantics.
- Decisions and verification are documented in `docs/reviews/multipass-review-decisions-2026-05-25.md`.

Remaining direct review IDs:

- `A-P0-1`
- `A-P3-1`
- `P-P0-2`
- `P-P0-4`
- `P-P0-5`
- `P-P1-1`
- `P-P3-1`

Suggested next move:

1. Confirm a clean worktree and rerun the remaining-ID script.
2. Pick the next bounded item. Likely candidates are:
   - `P-P1-1` failover replay streaming/concurrency, but check dependencies on provider pagination first.
   - `P-P3-1` encode/decode hot-path cleanup, likely small and independent.
   - `A-P3-1` remaining dead/aspirational settings/tracing cleanup after the cleanup-policy portion.
3. Treat `A-P0-1`, `P-P0-2`, `P-P0-5`, and likely `P-P0-4` as larger provider-client/query-track work; use subagents before implementing.
4. For every accepted finding: write or preserve red tests, implement, run focused gates, run full gates, update the decisions doc, then commit conventionally.
