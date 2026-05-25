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

## Batch 2: Blob Integrity, Optional Azure Diagnostics, And Audit Metrics

### Findings Accepted

- **Q-P0-1:** missing Azure SDK imports raised raw `ModuleNotFoundError` instead
  of an install-actionable package error.
- **S-P2-2 / S-NEW-P2-2:** Blob reads trusted decoded content without checking the
  stored fingerprint metadata.
- **Q-P2-1 / S-NEW-P3-1:** Blob event decoding substituted current time for
  missing required timestamps, which could create misleading or long-lived
  records from corrupted content.
- **S-NEW-NIT-1:** admin action success metrics were incremented before audit
  writes completed, so an audit failure could still look like a successful
  operator action.

### Fixes Implemented

- Wrapped Azure Blob SDK import failures in a `RuntimeError` that tells callers
  to install `durable-outbox[azure]`.
- Verified `event_fingerprint` metadata against the decoded event on Blob load
  and refresh; mismatches now raise `RetryableStoreError`.
- Required `created_at`, `expires_at`, and publish-result `published_at` during
  Blob decode instead of silently substituting `datetime.now(UTC)`.
- Moved admin action success metrics after successful audit writes. Audit write
  failures now increment `outbox_admin_actions_total{result="audit_failed"}` and
  re-raise the audit error.

### Verification

- `uv run pytest` -> 101 passed, 2 skipped
- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run ty check`
- `uv build`
