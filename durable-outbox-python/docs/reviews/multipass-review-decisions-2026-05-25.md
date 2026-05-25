# Multipass Review Decisions And Fixes

**Date:** 2026-05-25
**Scope:** `docs/reviews/multipass-review-2026-05-24.md`

This document records findings reviewed, decisions made, and fixes implemented
from the multipass review. It is intentionally incremental: each section should
name the reviewed finding IDs, the decision, the code change, and the
verification evidence.

## Batch 1: Validation, Ordering, Repair, And API Hygiene

### Findings Accepted

- **S-P0-1 / S-P2-1:** `claim_batch(limit=...)` and headers had no upper bound.
- **A-P1-2 / A-NEW-P1-6:** Memory, SQL, and Cosmos ordered claims scoped locks by
  raw ordering key only, so two topics using the same key blocked each other.
- **A-NEW-P1-1 / A-NEW-P1-4:** `repair_failed_to_pending` left stale retry/error
  fields behind, and memory repair raised `KeyError` for unknown events.
- **Q-P1-1:** `ClaimConflictError` and `RetryableStoreError` were not exported
  from the public `durable_outbox.core` API.
- **Q-P1-4:** The pytest config table was normalized to
  `[tool.pytest.ini_options]` for tool compatibility, even though current pytest
  accepted the previous table.

### Fixes Implemented

- Added `MAX_CLAIM_BATCH_LIMIT = 1000` in `core.validation` and applied it through
  the existing `require_positive_limit()` gate.
- Added header count and per-value byte caps in `OutboxEvent` construction:
  64 headers and 8192 bytes per value.
- Added `core.ordering.ordering_scope()` with versioned topic-aware scope
  `v1\0{topic}\0{ordering_key}`, then used it in memory, SQL, Cosmos, and Blob
  ordered-claim paths.
- Updated memory, Blob, SQL, and Cosmos repair paths to clear retry/error/claim
  state and reset `attempt_count` to `0`; memory repair is now a no-op for
  missing event IDs.
- Exported `ClaimConflictError` and `RetryableStoreError` from
  `durable_outbox.core`, and added top-level parity exports for core errors and
  `DispatchSummary`.
- Renamed `[tool.pytest]` to `[tool.pytest.ini_options]`.

### Verification

- `uv run pytest` -> 97 passed, 2 skipped
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run ty check`
- `uv build`

### Deferred Decisions

- Real SQL/Cosmos backends, dual-region role swap, failover replay reliability,
  Kafka hardening, Blob decode integrity, and admin replay remain valid findings
  but are larger follow-up batches.

