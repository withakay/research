# Continuation Prompt

Continue the active goal:

> Review `durable-outbox-python/docs/reviews/multipass-review-2026-05-24.md` in detail, use subagents to review each subsection deeply, implement agreed fixes, make conventional commits as you go, and maintain `durable-outbox-python/docs/reviews/multipass-review-decisions-2026-05-25.md` with decisions, fixes, justifications, and verification.

Work in `/Users/jack/Code/withakay/research`. Stay inside `durable-outbox-python` unless explicitly needed. Use Python 3.14 with `uv`, Ruff, `ty`, strict typing, TDD, and conventional commits. Use subagents for deeper subsection review when useful.

Current state:

- Latest completed batch: A-P3-1, settings and trace propagation wiring.
- Latest full gates for A-P3-1:
  - `uv run pytest -q` -> `248 passed, 2 skipped`
  - `uv run ruff check .` -> passed
  - `uv run ruff format --check .` -> passed
  - `uv run ty check` -> passed
  - `uv build` -> passed
- Most recent commits:
  - `b6188f4 perf(durable-outbox): avoid blob content scans while claiming`
  - `4fb031e feat(durable-outbox): bound cleanup work per tick`
  - `4a52984 perf(durable-outbox): index in-flight ordering keys`
  - `5a551b6 test(durable-outbox): capture ordering index expectations`

A-P3-1 implementation notes:

- CleanupPolicy is no longer aspirational; it is consumed by `CleanupScheduler`.
- `OutboxSettings.from_env()` loads `DURABLE_OUTBOX_*` host settings and `cleanup_policy()` builds the scheduler policy.
- `TraceContext.traceparent()` encodes W3C traceparent values.
- `KafkaSink` and `KafkaSink.from_config()` accept an optional tracer and inject traceparent when the event did not already provide one.
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
