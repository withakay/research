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

## Batch 3: Admin Replay And Store Protocol Completeness

### Findings Accepted

- **A-P1-3:** `DurableOutboxStore` omitted the cleanup and admin-action methods
  that production adapters already needed to support.
- **A-NEW-P0-2:** `AdminService.manual_replay()` called `replay_event`, but no
  production store implemented that method, so manual replay was dead code when
  stores were wired directly as admin actions.
- **A-NEW-P1-1 / A-NEW-P1-4:** repair actions needed consistent bool semantics
  for success vs missing/non-repairable events, in addition to clearing retry
  state.

### Fixes Implemented

- Added `cleanup_sent`, `repair_failed_to_pending`, and `replay_event` to the
  public `DurableOutboxStore` protocol.
- Updated memory, Blob, dual-region Blob, Cosmos, Azure SQL sync, and SQL Always
  On stores so `repair_failed_to_pending()` returns `True` only when a failed
  event was actually repaired and `False` for missing or non-failed events.
- Implemented `replay_event()` on all production stores. Replay sets an existing
  event back to `PENDING`, clears claim, retry, terminal publish, and error
  state, and preserves `attempt_count` so retry history remains visible.
- Mirrored dual-region Blob repair and replay updates into the secondary region
  only after the primary action succeeds.
- Extended the reusable provider contract and built-in adapter tests to cover
  admin replay, repair return values, and missing-event behavior.
- Updated `FailingStore` to continue satisfying `DurableOutboxStore` after the
  protocol became complete.

### Verification

- Focused red test run showed 19 failures for the missing protocol and replay
  methods before implementation.
- Focused green run:
  `uv run pytest tests/provider_contract/test_fake_store_contract.py tests/test_adapters.py::test_store_protocol_includes_admin_and_cleanup_contracts tests/test_adapters.py::test_provider_repair_failed_to_pending_clears_retry_state tests/test_adapters.py::test_provider_replay_event_requeues_sent_event tests/test_adapters.py::test_provider_admin_actions_return_false_for_missing_event -q`
  -> 20 passed
- `uv run pytest -q` -> 117 passed, 2 skipped
- `uv run ruff check .`
- `uv run ty check`

## Batch 4: Error Message Bounds And Store-Update Observability

### Findings Accepted

- **S-NEW-P1-2:** dispatcher paths passed raw `str(exc)` into store
  `last_error` fields, allowing long broker or driver messages to overflow SQL
  storage and leave events stuck `IN_FLIGHT`.

### Fixes Implemented

- Added a 512-byte stored error-message cap in `OutboxDispatcher` before
  retryable or non-retryable publish errors are written to stores.
- Added `outbox_error_messages_truncated_total{topic,error_type}` when a stored
  error message is shortened.
- Wrapped `mark_pending_after_retryable_failure()` and `mark_failed()` store
  updates so failures increment
  `outbox_store_update_failures_total{topic,operation,error_type}` and are
  reflected in `DispatchSummary.store_update_failed` instead of escaping the
  dispatcher loop without observability.
- Emitted the same generic store-update failure metric for post-ack
  `mark_sent()` failures, preserving the existing specific
  `outbox_mark_sent_failures_total` metric.
- Increased the SQL DDL `last_error` column to `NVARCHAR(2048)` as headroom
  above the application cap.

### Verification

- Focused red tests showed the long error was stored at 1520 bytes and retry/
  failed-state failure injection was unsupported before implementation.
- Focused green run:
  `uv run pytest tests/test_core.py::test_dispatcher_truncates_retryable_error_message_before_store_update tests/test_core.py::test_dispatcher_observes_retry_state_update_failure tests/test_core.py::test_dispatcher_observes_failed_state_update_failure tests/test_core.py::test_dispatcher_does_not_mark_pending_after_post_ack_store_failure -q`
  -> 4 passed

## Batch 5: Topic Validation And Prometheus Label Escaping

### Findings Accepted

- **S-NEW-P1-1:** producer-controlled topics were accepted without a Kafka-style
  grammar check and then used as metric labels. The Prometheus exposition helper
  escaped newline, quote, and backslash but not carriage return or other C0
  controls.

### Fixes Implemented

- Added a strict topic regex to `OutboxEvent`:
  `^[A-Za-z0-9._-]{1,249}$`.
- Updated Prometheus label escaping to render `\r` as `\\r` and every other C0
  control as `\\xNN`, while preserving the existing escaping for newline,
  quotes, and backslashes.

### Verification

- Focused red tests showed invalid topics with carriage returns, slashes, and
  excessive length were accepted, and raw control characters appeared in
  Prometheus output before implementation.
- Focused green run:
  `uv run pytest tests/test_core.py::test_event_rejects_invalid_topic_names tests/test_operations.py::test_collecting_metrics_adapter_exports_prometheus_text -q`
  -> 5 passed
