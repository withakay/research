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

## Batch 6: RPO=0 Capability Startup Gates

### Findings Accepted

- **A-P1-4:** `OutboxCapabilities.require_rpo_zero()` existed but was never
  called, so a pipeline that intended to require RPO=0 could silently run on a
  non-RPO=0 store.

### Fixes Implemented

- Added `require_rpo_zero` to `OutboxDispatcher`, defaulting to `False` for
  compatibility and calling `store.capabilities.require_rpo_zero()` when enabled.
- Added `require_rpo_zero` to `FailoverReplayer`, defaulting to `True` because
  failover replay against a non-RPO=0 store is incoherent. Tests can explicitly
  opt out.

### Verification

- Focused red tests showed the dispatcher option and failover replayer option
  were missing before implementation.
- Focused green run:
  `uv run pytest tests/test_core.py::test_dispatcher_can_require_rpo_zero_store tests/test_failover_ordering_cleanup.py::test_failover_replayer_requires_rpo_zero_by_default tests/test_failover_ordering_cleanup.py::test_failover_replayer_can_opt_out_of_rpo_zero_validation -q`
  -> 3 passed

## Batch 7: Failover Replay Error Accounting

### Findings Accepted

- **A-P0-3:** a single publish or store error during failover replay raised out
  of `replay_once()`, preventing the replayer from reporting partial progress
  or the failed-event count.

### Fixes Implemented

- Added `ReplaySummary.errored`.
- Updated `FailoverReplayer` to catch per-event publish and `mark_sent` errors,
  increment `outbox_failover_replay_failures_total{topic,error_type}`, continue
  replaying remaining candidates, and return both replayed and errored counts.
- Kept cleanup frozen after partial replay. This preserves the explicit
  operator workflow: call `complete_replay()` only after the recovery watermark
  has been met.

### Verification

- Focused red test showed `FailoverReplayer` lacked metrics support and errored
  accounting before implementation.
- Focused green run:
  `uv run pytest tests/test_failover_ordering_cleanup.py::test_replay_continues_after_publish_failure_and_keeps_cleanup_frozen tests/test_failover_ordering_cleanup.py::test_failover_replay_uses_failover_started_at_not_now tests/test_failover_ordering_cleanup.py::test_failover_replay_selects_live_pending_in_flight_and_sent_records -q`
  -> 5 passed

### Deferred

- Persistent cleanup-freeze state across process restarts remains open and
  should be implemented with the store-level freeze marker work.

## Batch 8: Dual-Region Active Role Routing

### Findings Accepted

- **A-P0-2:** `DualRegionBlobOutboxStore` routed claim, replay, and terminal
  updates through the original primary only, leaving no library path for the
  secondary region to take over.
- **A-NEW-NIT-1:** dual-region `put()` returned the primary region's
  `accepted_at`, even though the RPO=0 acceptance instant is the later of the
  two regional accepts.

### Fixes Implemented

- Added an explicit `active_region` role to `DualRegionBlobOutboxStore`, with
  `use_region()`, `promote_secondary()`, and `promote_primary()` controls.
- Routed `claim_batch`, `mark_sent`, retry/failure updates,
  `failover_replay_candidates`, cleanup, repair, and manual replay through the
  active region, mirroring terminal/admin updates into the standby region.
- Kept dual-region `put()` writing both regions and changed its receipt
  `accepted_at` to the maximum accepted timestamp across primary and secondary.

### Verification

- Focused red tests showed there was no secondary promotion API before
  implementation.
- Focused green run:
  `uv run pytest tests/test_adapters.py::test_dual_region_blob_can_promote_secondary_for_dispatch tests/test_adapters.py::test_dual_region_blob_failover_replay_uses_active_secondary tests/test_adapters.py::test_dual_region_blob_accepts_only_after_both_regions -q`
  -> 3 passed

## Batch 9: Dual-Region Prepared Reconciliation

### Findings Accepted

- **A-NEW-P0-1:** dual-region prepared-only records were invisible to failover
  replay, so a process failure during the prepare/accept sequence could leave
  recoverable records stranded.

### Fixes Implemented

- Added `DualRegionBlobOutboxStore.list_prepared_event_ids()` to scan both
  regions for `accepted=False` records.
- Added `reconcile_prepared()` to repair every prepared record through the
  existing `repair_prepared()` path.
- Ran prepared reconciliation at the start of
  `DualRegionBlobOutboxStore.failover_replay_candidates()` so replay candidates
  include records that stopped mid dual-write.

### Verification

- Focused red test showed there was no prepared-listing API before
  implementation.
- Focused green run:
  `uv run pytest tests/test_adapters.py::test_dual_region_blob_reconciles_prepared_records_before_failover_replay tests/test_adapters.py::test_dual_region_blob_failover_replay_uses_active_secondary tests/test_adapters.py::test_dual_region_blob_repairs_partial_write_matrix -q`
  -> 6 passed

## Batch 10: SQL And Cosmos CAS Retry Semantics

### Findings Accepted

- **A-NEW-P1-2:** SQL and Cosmos admin repair paths and state updates could
  surface a single optimistic version conflict as an operator-facing exception
  instead of retrying boundedly.

### Fixes Implemented

- Added bounded `_cas_update()` helpers to Cosmos and SQL stores.
- Routed `mark_sent`, retryable failure updates, failed updates, repair, and
  manual replay through the CAS helpers.
- Preserved claim-token validation on claimed-event updates and changed repeated
  CAS races into `RetryableStoreError` after three failed attempts.

### Verification

- Focused red tests showed a conflict-once client raised `ClaimConflictError`
  from repair and mark-sent before implementation.
- Focused green run:
  `uv run pytest tests/test_adapters.py::test_sql_and_cosmos_repair_retry_cas_conflict tests/test_adapters.py::test_sql_and_cosmos_mark_sent_retry_cas_conflict -q`
  -> 4 passed

## Batch 11: Kafka Certified TLS And Header Filtering

### Findings Accepted

- **S-P1-1:** certified Kafka mode validated `acks=all` and idempotence but did
  not reject plaintext transport.
- **S-P1-2:** `OutboxEvent` headers were forwarded to Kafka without a
  defence-in-depth block list for common secret-bearing headers.

### Fixes Implemented

- Added `security.protocol=SASL_SSL` to certified Kafka defaults and rejected
  certified configs whose protocol is not `SSL` or `SASL_SSL`.
- Left plaintext available only when callers explicitly construct
  `KafkaProducerConfig(..., certified_mode=False)`.
- Added a default blocked header prefix list for `authorization`, cookies,
  proxy authorization, API keys, and `x-auth-*` headers at the `OutboxEvent`
  validation boundary.
- Marked local/Aspire Kafka usage in integration wiring as non-certified because
  those test brokers are plaintext by design.

### Verification

- Focused red tests showed plaintext certified Kafka configs and
  `Authorization` headers were accepted before implementation.
- Focused green run:
  `uv run pytest tests/test_kafka_operations.py::test_kafka_config_rejects_plaintext_in_certified_mode tests/test_kafka_operations.py::test_kafka_config_allows_plaintext_when_not_certified tests/test_kafka_operations.py::test_outbox_event_rejects_sensitive_headers tests/test_kafka_operations.py::test_kafka_sink_from_config_uses_real_producer_factory_hook tests/test_kafka_operations.py::test_kafka_sink_preserves_trace_headers_and_adds_event_identity -q`
  -> 5 passed

## Batch 12: Async-Safe JSONL Audit Writes

### Findings Accepted

- **S-P1-3:** `JsonlAuditSink.record()` performed file append, flush, and fsync
  directly on the asyncio event-loop thread.

### Fixes Implemented

- Moved the blocking JSONL append/flush/fsync work into `asyncio.to_thread`.
- Kept the existing async lock around the write call so audit records remain
  ordered per sink instance.

### Verification

- Focused red test showed `os.fsync` ran on the event-loop thread before
  implementation.
- Focused green run:
  `uv run pytest tests/test_operations.py::test_jsonl_audit_sink_runs_fsync_off_event_loop_thread tests/test_operations.py::test_jsonl_audit_sink_appends_fsynced_records -q`
  -> 2 passed

## Batch 13: Explicit Production Adapter Clients

### Findings Accepted

- **A-P0-5:** production adapter constructors silently defaulted to process-local
  in-memory clients while reporting production-looking capability names.

### Fixes Implemented

- Changed Blob, dual-region Blob, Cosmos, Azure SQL sync, and SQL Always On
  production constructors to require explicit client objects.
- Added `.for_testing()` factories for each adapter to preserve concise tests
  and clearly name in-memory capabilities with `InMemory*` store names.
- Updated test call sites to use `.for_testing()` when they intentionally use
  in-memory clients, leaving explicit-client tests unchanged.

### Verification

- Focused red tests showed constructors accepted missing clients and no
  `.for_testing()` factories existed before implementation.
- Focused green run:
  `uv run pytest tests/test_adapters.py tests/test_failover_ordering_cleanup.py tests/test_core.py -q`
  -> 110 passed

## Batch 14: Blob-Backed Ordering Locks

### Findings Accepted

- **A-P0-4:** Blob ordered mode used a process-local ordering lock by default,
  allowing horizontally scaled publishers to claim the same ordering key from
  stale local snapshots.

### Fixes Implemented

- Added `BlobOrderingLockBackend`, which coordinates leases through conditional
  lock-blob writes on the shared blob client.
- Changed `BlobOutboxStore` to default to the blob-backed lock backend for
  explicit blob clients.
- Exported `BlobOrderingLockBackend` from `durable_outbox.stores` and
  documented the cross-process ordered-mode requirement in `docs/providers.md`.

### Verification

- Focused red test showed a stale second publisher could claim the next ordered
  event for the same key before implementation.
- Focused green run:
  `uv run pytest tests/test_failover_ordering_cleanup.py::test_blob_ordering_lock_blocks_stale_second_publisher tests/test_failover_ordering_cleanup.py::test_blob_ordering_recovers_stale_lock_after_lease_expiry tests/test_adapters.py::test_store_package_exports_are_importable -q`
  -> 3 passed

## Batch 15: Backend-Persisted Cleanup Freeze

### Findings Accepted

- **A-P1-1:** cleanup freeze state was kept only on the store instance, so a
  restarted cleanup worker could delete sent records while failover replay was
  still active.

### Fixes Implemented

- Persisted Blob cleanup freeze state as a marker blob in the same backend.
- Added cleanup-freeze marker operations to SQL and Cosmos client protocols and
  in-memory client implementations.
- Added injectable shared cleanup state to the memory store so tests and fake
  stores can model cross-instance freeze behavior.
- Updated cleanup paths to read the backend marker before deleting expired sent
  records.

### Verification

- Focused red tests showed second store instances over the same Blob, Cosmos,
  and SQL backends deleted expired sent records despite a freeze from the first
  instance before implementation.
- Focused green run:
  `uv run pytest tests/test_adapters.py::test_cleanup_freeze_survives_backend_reopen tests/test_adapters.py::test_memory_cleanup_freeze_can_use_shared_state -q`
  -> 4 passed

## Batch 16: Dual-Region Cleanup Delete Ordering

### Findings Accepted

- **A-NEW-P1-3:** dual-region cleanup deleted the active region before the
  standby region, so a standby delete failure could leave the active copy
  already removed and make failover recovery weaker.

### Fixes Implemented

- Changed dual-region cleanup to delete expired sent records from the standby
  region first, then the active region.
- Added a regression test that injects a standby delete failure and verifies the
  active copy remains available.

### Verification

- Focused red test showed active records were deleted before a standby cleanup
  failure surfaced.
- Focused green run:
  `uv run pytest tests/test_adapters.py::test_dual_region_cleanup_preserves_active_when_standby_delete_fails tests/test_adapters.py::test_provider_claim_retry_sent_failed_replay_and_cleanup_freeze -q`
  -> 4 passed

## Batch 17: Per-Event Durability Witnesses

### Findings Accepted

- **A-NEW-P1-5:** `AcceptedReceipt.rpo_zero` was only a copy of static store
  capabilities and did not provide auditable per-event durability evidence.

### Fixes Implemented

- Added additive `AcceptedReceipt.durability_witness: tuple[str, ...]`.
- Populated witnesses for memory, Blob, dual-region Blob, Cosmos, Azure SQL
  sync, and SQL Always On stores.
- Documented that operators should prefer the witness over the compatibility
  boolean for per-event audit evidence.

### Verification

- Focused green run:
  `uv run pytest tests/test_adapters.py::test_accept_receipts_include_durability_witness -q`
  -> 6 passed

## Batch 18: Replay Idempotency Signals And Consumer Helper

### Findings Accepted

- **A-NEW-P1-7:** failover replay can republish previously sent events, but the
  library did not make the consumer dedupe contract visible or testable.

### Fixes Implemented

- Added `ClaimedEvent.source_status` and populated it for failover replay
  candidates so the replayer can identify previously sent events.
- Added a warning and `outbox_failover_sent_replays_total{topic}` metric when
  failover replay republishes a `SENT` event.
- Added `durable_outbox.consumer.EventDeduper`, a small `(topic, event_id)`
  dedupe helper for consumers and tests.
- Documented that Kafka idempotence is producer-session scoped and consumers
  must dedupe replayed events by `event_id`.

### Verification

- Focused green run:
  `uv run pytest tests/test_failover_ordering_cleanup.py::test_failover_replay_warns_when_republishing_sent_event tests/test_consumer_dedupe.py -q`
  -> 3 passed

## Batch 19: Dual-Region Mirror Update Repair

### Findings Accepted

- **A-P2-2 / effective P1 after role-swap:** standby mirror updates were a
  single write after the active region was already updated, so transient standby
  failures could leave the future active region stale.

### Fixes Implemented

- Added three-attempt retry around dual-region mirror updates.
- Added a pending mirror repair set plus `pending_mirror_event_ids()` and
  `reconcile_mirror_updates()` APIs.
- Ran mirror reconciliation before failover replay candidate selection.

### Verification

- Focused red test showed a single transient standby write failure escaped
  before implementation.
- Focused green run:
  `uv run pytest tests/test_adapters.py::test_dual_region_mirror_retries_transient_standby_update_failure tests/test_adapters.py::test_dual_region_mirror_queues_reconciliation_after_repeated_failure -q`
  -> 2 passed

## Batch 20: Dispatcher Publish Concurrency And Kafka Poll Offload

### Findings Accepted

- **P-P0-1:** dispatcher publish/mark loops were strictly sequential even for
  independent claimed events.
- **P-P1-2 / Q-P2-6:** Kafka `poll()` ran synchronously on the asyncio event-loop
  thread.

### Fixes Implemented

- Added `OutboxDispatcher(concurrency=...)` with a bounded semaphore and
  per-claim `_publish_one()` worker while preserving existing per-event error,
  retry, metrics, and summary accounting.
- Offloaded Kafka `produce()` and `poll()` calls via `asyncio.to_thread()`.

### Verification

- Focused red tests showed `OutboxDispatcher` had no concurrency option and
  Kafka `poll()` ran on the event-loop thread before implementation.
- Focused green run:
  `uv run pytest tests/test_core.py::test_dispatcher_publishes_claimed_events_concurrently tests/test_core.py::test_dispatcher_marks_sent_after_sink_ack tests/test_core.py::test_dispatcher_returns_retryable_failure_to_pending tests/test_core.py::test_dispatcher_does_not_mark_pending_after_post_ack_store_failure tests/test_kafka_operations.py::test_kafka_sink_poll_does_not_run_on_event_loop_thread tests/test_kafka_operations.py::test_kafka_sink_polls_until_delivery_ack tests/test_kafka_operations.py::test_kafka_sink_returns_result_after_ack_and_adds_event_id_header -q`
  -> 7 passed

## Batch 21: Duplicate Diagnostics And Prepared Accept Timestamps

### Findings Accepted

- **A-NEW-P2-1:** prepared-record repair could overwrite an existing
  `accepted_at` timestamp.
- **A-NEW-P2-2:** duplicate event conflicts did not identify the divergent
  envelope field.

### Fixes Implemented

- Preserved existing Blob prepared `accepted_at` values when re-accepting a
  repaired prepared record.
- Added a shared duplicate-diagnostics helper that reports the first divergent
  envelope field while redacting payload/header values.
- Applied the duplicate helper to memory, Blob, Cosmos, and SQL stores.

### Verification

- Focused red tests showed prepared accept overwrote timestamps and duplicate
  conflict messages did not include `topic` before implementation.
- Focused green run:
  `uv run pytest tests/test_adapters.py::test_blob_accept_prepared_preserves_existing_accepted_at tests/test_adapters.py::test_blob_put_rejects_incompatible_duplicate tests/test_adapters.py::test_provider_put_rejects_incompatible_duplicate tests/test_core.py::test_duplicate_put_rejects_incompatible_event_envelope -q`
  -> 6 passed

## Batch 22: Claim Token Handling Hardening

### Findings Accepted

- **S-NEW-P1-3:** claim-token-shaped UUIDs could leak through stored dispatcher
  error messages, and store ownership checks used normal string equality.

### Fixes Implemented

- Redacted UUID-shaped values from stored dispatcher error messages before
  truncation and persistence.
- Added a shared constant-time claim token comparison helper.
- Applied constant-time token checks to memory, Blob, Cosmos, SQL, and Blob
  ordering lock release ownership paths.

### Verification

- Focused red test showed stored error messages preserved UUID-shaped tokens.
- Focused green run:
  `uv run pytest tests/test_security.py -q`
  -> 3 passed

## Batch 23: Dual-Region Records Snapshot

### Findings Accepted

- **A-P2-1:** `DualRegionBlobOutboxStore.records` exposed a mutable alias to
  the active region's internal record dictionary.

### Fixes Implemented

- Replaced the dual-region mutable records alias with a dynamic read-only
  mapping snapshot.
- Returned cloned `StoredEvent` values from the snapshot so callers cannot
  mutate active-region records through inspection APIs.
- Removed internal alias refresh assignments from region switching, writes, and
  prepared repair.

### Verification

- Focused green run:
  `uv run pytest tests/test_adapters.py::test_dual_region_records_view_is_read_only_snapshot tests/test_adapters.py::test_dual_region_blob_prepared_records_are_hidden_from_claims tests/test_adapters.py::test_dual_region_blob_can_promote_secondary_for_dispatch -q`
  -> 3 passed

## Batch 24: Blob Metadata Safety

### Findings Accepted

- **S-NEW-P2-1:** Blob metadata values included producer-controlled `event_id`
  and operator-controlled environment strings without a backend metadata safety
  check.
- **S-NEW-P2-1 extension:** Blob cleanup freeze wrote the raw operator reason
  into metadata even though the reason can be safely stored in the marker JSON
  body.

### Fixes Implemented

- Added `enforce_metadata_safe()` for printable ASCII metadata values with a
  length bound.
- Applied metadata-safe validation to `OutboxEvent.event_id`, Blob store
  environments, and Blob metadata generation.
- Stopped writing cleanup freeze reasons to Blob metadata; `_cleanup_is_frozen`
  now reads the reason from marker content and falls back safely for malformed
  markers.

### Verification

- Focused red tests showed unsafe event IDs and Blob environments were accepted.
- Focused green run:
  `uv run pytest tests/test_security.py tests/test_adapters.py::test_blob_metadata_preserves_envelope_fields -q`
  -> 8 passed

## Batch 25: Admin Metadata Helper Cleanup

### Findings Accepted

- **A-NIT-1:** `AdminEventMetadata.as_pending()` existed only to support local
  test doubles and was not part of the production admin surface.

### Fixes Implemented

- Removed `AdminEventMetadata.as_pending()`.
- Updated operation and Kafka admin test doubles to use explicit
  `dataclasses.replace(...)` calls so test-only state transitions stay local to
  tests.

### Verification

- Focused green run:
  `uv run pytest tests/test_operations.py tests/test_kafka_operations.py -q`
  -> 22 passed

## Batch 26: Blob Ordering Lease Coherence

### Findings Accepted

- **A-P2-3:** Blob ordering locks relied on lease expiry for crash recovery, but
  the constructor allowed the ordering lock lease duration to drift away from
  the claim timeout without a renewal mechanism.

### Fixes Implemented

- Made Blob ordering lock lease duration derive from `claim_timeout` by default.
- Rejected explicit lease durations that differ from `claim_timeout` until lock
  renewal is supported.
- Reworked stale-lock recovery coverage to use a fake clock with matching claim
  timeout and lease duration instead of a zero-second lease.

### Verification

- Focused green run:
  `uv run pytest tests/test_failover_ordering_cleanup.py::test_blob_ordering_recovers_stale_lock_after_lease_expiry tests/test_failover_ordering_cleanup.py::test_blob_ordering_lock_lease_duration_must_match_claim_timeout tests/test_failover_ordering_cleanup.py::test_blob_ordering_lock_blocks_stale_second_publisher -q`
  -> 4 passed

## Batch 27: Admin Action Outcome Statuses

### Findings Accepted

- **Result-pattern exploration:** a generic Rust-style `Result[T, E]` would add
  too much ceremony to the Python store and sink protocols, but the admin action
  `bool` return path erased useful domain information.

### Fixes Implemented

- Added `AdminActionStatus` with `success`, `not_found`, and `wrong_state`
  outcomes.
- Changed store/admin repair and replay contracts to return
  `AdminActionStatus` instead of a bare `bool`.
- Updated memory, Blob, dual-region Blob, Cosmos, SQL, test adapters, and
  provider contracts so missing events and wrong-state repairs are observable
  without using exceptions.
- Kept validation, publish, and ambiguous store-update failures exception-based
  because those paths must remain loud and hard to ignore.

### Verification

- Focused green runs:
  `uv run pytest tests/test_operations.py tests/test_adapters.py::test_provider_repair_failed_reports_wrong_state_for_non_failed_event tests/test_adapters.py::test_provider_admin_actions_return_not_found_for_missing_event tests/provider_contract/test_fake_store_contract.py -q`
  -> 20 passed
- `uv run pytest tests/test_kafka_operations.py -q` -> 16 passed
- `uv run pytest tests/test_app.py -q` in `durable-outbox-fastapi` -> 2 passed

## Batch 28: Provider Dependency Floors

### Findings Accepted

- **S-NEW-P2-3:** optional provider dependency lower bounds lagged behind the
  reviewed lockfile versions, leaving consumers free to install much older
  Azure, Kafka, and SQL client packages.

### Fixes Implemented

- Raised optional dependency floors to the currently reviewed provider versions:
  `aiohttp>=3.13.5`, `azure-storage-blob>=12.29.0`,
  `azure-cosmos>=4.15.0`, `confluent-kafka>=2.14.0`, and `pyodbc>=5.3.0`.
- Refreshed both uv lockfiles so editable `durable-outbox` metadata reflects
  the new floors.
- Added a uv-based Dependabot configuration scoped to `/durable-outbox-python`
  and `/durable-outbox-fastapi`.
- Added packaging tests that pin the reviewed floors and assert both package
  directories are covered by Dependabot.

### Verification

- Current PyPI metadata and existing lockfiles agree on the raised floor
  versions for the five optional provider packages.
- Focused green run:
  `uv run pytest tests/test_packaging_docs.py -q`
  -> 5 passed

## Batch 29: Azure Optional Dependency Errors

### Findings Accepted

- **Q-P0-1:** Azure optional SDK imports should fail as package configuration
  errors with an actionable install hint, not as raw `ModuleNotFoundError` or a
  generic runtime failure. The initial fix only covered
  `azure.storage.blob.aio`; conditional Blob writes and deletes also import
  `azure.core` for match conditions.

### Fixes Implemented

- Added a shared Azure module import helper that raises `ConfigurationError`
  with the `durable-outbox[azure]` install hint.
- Used the helper for both `AzureBlobClient.from_connection_string()` and Blob
  ETag match-condition imports.
- Extended Azure Blob tests to assert both optional dependency paths report
  `ConfigurationError`.

### Verification

- Focused green run:
  `uv run pytest tests/test_azure_blob_and_file_sink.py -q`
  -> 4 passed
- Full package gates:
  `uv run pytest -q` -> 180 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 30: Retry Backoff Jitter

### Findings Accepted

- **P-P2-4:** retry scheduling used deterministic exponential backoff with no
  jitter, so a Kafka or storage outage could cause synchronized retry waves when
  many events fail in the same dispatch window.

### Fixes Implemented

- Added `RetryPolicy.jitter`, defaulting to `0.1`, and applied bounded
  multiplicative jitter after exponential backoff and max-delay capping.
- Added an injectable `Random` instance for deterministic tests and deployments
  that need reproducible retry scheduling.
- Added validation for invalid jitter and multiplier values.
- Updated exact-backoff tests to opt into `jitter=0.0`.

### Verification

- Focused green run:
  `uv run pytest tests/test_core.py -q`
  -> 30 passed
- Full package gates:
  `uv run pytest -q` -> 184 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 31: Blob Refresh Cache Eviction

### Findings Accepted

- **P-P2-5:** Blob `_refresh_records()` updated records that still existed in
  storage but never evicted records deleted by cleanup or another process,
  allowing long-lived store instances to grow stale local mirrors and ETag
  caches.

### Fixes Implemented

- Tracked event IDs observed during a successful Blob listing refresh.
- Removed local `records` and `_record_etags` entries that were absent from the
  latest backend listing.
- Added a red/green regression test that deletes a Blob object through a shared
  client and verifies a reopened/refreshed store drops the stale local state.

### Verification

- Focused red run reproduced the stale-cache bug:
  `uv run pytest tests/test_adapters.py::test_blob_refresh_evicts_records_deleted_from_backend -q`
  -> failed before implementation.
- Focused green run:
  `uv run pytest tests/test_adapters.py::test_blob_refresh_evicts_records_deleted_from_backend -q`
  -> 1 passed
- Full package gates:
  `uv run pytest -q` -> 185 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 32: Kafka Error Classification Hardening

### Findings Accepted

- **S-P3-2:** Kafka delivery errors were classified as non-retryable when an
  otherwise unknown error message contained substrings such as
  `authorization`, `invalid config`, or `invalid topic`. That can turn
  transient infrastructure text into terminal outbox failures.

### Fixes Implemented

- Removed message-substring fallback classification.
- Kept explicit `error.retriable()` handling and the known broker error-name
  allowlist for terminal errors.
- Added a regression test proving a known `TOPIC_AUTHORIZATION_FAILED` remains
  non-retryable while unknown text mentioning authorization stays retryable.

### Verification

- Focused red run reproduced the substring-classification bug:
  `uv run pytest tests/test_kafka_operations.py::test_kafka_sink_treats_unknown_message_text_as_retryable -q`
  -> failed before implementation.
- Focused green run:
  `uv run pytest tests/test_kafka_operations.py::test_kafka_sink_classifies_authorization_errors_as_non_retryable tests/test_kafka_operations.py::test_kafka_sink_treats_unknown_message_text_as_retryable -q`
  -> 2 passed
- Full package gates:
  `uv run pytest -q` -> 186 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 33: Azure Blob Put Response ETags

### Findings Accepted

- **Q-P2-3:** `AzureBlobClient.put_blob()` performed a get-after-put round trip
  after every upload just to read the ETag, even though Azure Blob upload
  responses include the ETag.

### Fixes Implemented

- Returned the `BlobObject` from the upload request content, request metadata,
  and upload response ETag.
- Added strict response-shape handling that raises `BlobPreconditionFailedError`
  if an upload response does not expose an ETag.
- Added a focused Azure client test proving `put_blob()` performs no property
  read or download after upload.

### Verification

- Focused red run reproduced the extra readback:
  `uv run pytest tests/test_azure_blob_and_file_sink.py::test_azure_blob_client_put_uses_upload_response_without_readback -q`
  -> failed before implementation with one property read.
- Focused green run:
  `uv run pytest tests/test_azure_blob_and_file_sink.py -q`
  -> 5 passed
- Full package gates:
  `uv run pytest -q` -> 187 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 34: Package Metadata Hardening

### Findings Accepted

- **Q-P3-3:** package metadata was sparse for an installable typed library,
  making project links, search keywords, and target runtime characteristics less
  discoverable.

### Fixes Implemented

- Added project URLs for homepage, documentation, repository, and issues.
- Added durable-outbox, Azure, Kafka, RPO=0, and transactional-outbox keywords.
- Added relevant AsyncIO, OS-independent, database, and distributed-computing
  classifiers.
- Added a packaging regression test that verifies URLs, keywords, classifiers,
  and the local `py.typed` marker.

### Verification

- Focused green run:
  `uv run pytest tests/test_packaging_docs.py -q`
  -> 6 passed
- Full package gates:
  `uv run pytest -q` -> 188 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.
- Wheel inspection:
  `uv run python - ...` -> `durable_outbox/py.typed` present in the built wheel.

## Batch 35: Cosmos Bucket Hashing

### Findings Accepted

- **Q-P3-2:** Cosmos unordered partition selection converted an entire SHA-256
  hex digest to an integer just to take a modulo bucket.

### Fixes Implemented

- Added `_hash_bucket()` that converts the first eight digest bytes with
  `int.from_bytes(..., "big")`.
- Kept ordered partition keys on the existing full hash string so existing
  ordered key names remain stable.
- Added a deterministic test that locks the unordered bucket for a known event
  ID.

### Verification

- Focused green run:
  `uv run pytest tests/test_adapters.py::test_cosmos_unordered_partition_key_uses_stable_bucket tests/test_adapters.py::test_cosmos_partition_key_colocates_ordered_events -q`
  -> 2 passed
- Full package gates:
  `uv run pytest -q` -> 189 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 36: Frozen Dataclass Normalization Comments

### Findings Accepted

- **Q-P3-4:** `OutboxEvent` and `PublishResult` use `object.__setattr__` in
  `__post_init__`, which is intentional but looked like accidental mutation of
  frozen dataclasses.

### Fixes Implemented

- Added comments explaining that the assignments normalize caller-owned mutable
  mappings while preserving the frozen public dataclass contract.
- Deferred a factory-based redesign because the current normalization keeps the
  public constructor ergonomic and the finding only requested a lower-effort
  clarification.

### Verification

- Focused green run:
  `uv run pytest tests/test_core.py -q`
  -> 30 passed
- Full package gates:
  `uv run pytest -q` -> 189 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 37: Azure Blob Download Size Bound

### Findings Accepted

- **S-P2-2:** `AzureBlobClient.get_blob()` fetched properties and then called
  `download.readall()` with no content-size guard, so a corrupt or unexpected
  blob could force an unbounded in-memory read.

### Fixes Implemented

- Added `MAX_BLOB_DOWNLOAD_BYTES`, currently 16 MiB, at the Azure Blob client
  boundary.
- Checked the advertised `size` or `content_length` property before starting
  the download.
- Added a post-read length check as a fallback for clients that do not expose a
  usable size property.
- Added a regression test proving oversized advertised blobs fail before
  `download_blob()`/`readall()` is invoked.

### Verification

- Focused red run first failed at collection because no max-size constant or
  implementation existed.
- Focused green run:
  `uv run pytest tests/test_azure_blob_and_file_sink.py::test_azure_blob_client_rejects_oversized_blob_before_download -q`
  -> 1 passed
- Full package gates:
  `uv run pytest -q` -> 190 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 38: Header Aggregate Bounds

### Findings Accepted

- **S-P2-1:** header count and per-value size limits were already present, but
  header names and aggregate header bytes were not bounded explicitly.

### Fixes Implemented

- Added `MAX_HEADER_NAME_BYTES = 256`.
- Added `MAX_HEADER_TOTAL_BYTES = 64 * 1024`.
- Enforced both bounds during `OutboxEvent` header freezing before the mapping
  is exposed as immutable.
- Added focused tests for oversized header names, aggregate header bytes, and
  the existing per-value bound.

### Verification

- Focused green run:
  `uv run pytest tests/test_core.py::test_event_rejects_oversized_header_name tests/test_core.py::test_event_rejects_oversized_header_total tests/test_core.py::test_event_rejects_oversized_header_value -q`
  -> 3 passed
- Full package gates:
  `uv run pytest -q` -> 192 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 39: Dual-Region Mirror Observability

### Findings Accepted

- **A-P2-2:** dual-region Blob standby mirror updates had retry and queued
  reconciliation behavior, but repeated mirror drift did not emit metrics or
  logs that operators could alert on.

### Fixes Implemented

- Added an optional `MetricsAdapter` to `DualRegionBlobOutboxStore`.
- Incremented `outbox_blob_mirror_update_failures_total` for each failed
  standby mirror update attempt, labelled by active region, standby region, and
  error type.
- Incremented `outbox_blob_mirror_updates_queued_total` when an event is queued
  for later mirror reconciliation.
- Logged a warning with event and region context when reconciliation is queued.
- Extended the repeated-failure mirror test to assert both counters.

### Verification

- Focused green run:
  `uv run pytest tests/test_adapters.py::test_dual_region_mirror_queues_reconciliation_after_repeated_failure -q`
  -> 1 passed
- Full package gates:
  `uv run pytest -q` -> 192 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 40: Strict Blob Record Decode Types

### Findings Accepted

- **S-P3-1:** Blob record decoding still accepted or coerced invalid JSON field
  types, including string booleans, boolean attempt counts, non-string claim
  tokens, non-string header values, and non-string publish-result metadata.

### Fixes Implemented

- Added field-specific Blob decode helpers for required/optional strings,
  integers, booleans, mappings, timestamps, status values, publishing modes,
  base64 byte fields, headers, and publish-result metadata.
- Converted low-level bad-base64 and malformed timestamp errors into
  `RetryableStoreError` with the affected field name.
- Stopped silently coercing corrupt JSON scalar values with `str(...)`,
  `int(...)`, `bool(...)`, or `dict(...)`.
- Added representative decode regression tests for invalid record, event, and
  publish-result field types.

### Verification

- Focused red run showed all five invalid-type cases were accepted or leaked a
  lower-level base64 error before implementation.
- Focused green run:
  `uv run pytest tests/test_adapters.py::test_blob_decode_rejects_invalid_field_types tests/test_adapters.py::test_blob_decode_requires_created_and_expires_timestamps -q`
  -> 6 passed
- Full package gates:
  `uv run pytest -q` -> 197 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 41: Failover Replay Candidate Rollback

### Findings Accepted

- **A-P2-4:** `failover_replay_candidates()` claimed records while building a
  returned list. If candidate construction was interrupted by cancellation or
  another exception, records already claimed in that call could remain
  `IN_FLIGHT` until claim timeout.

### Fixes Implemented

- Added rollback for interrupted candidate construction in memory, Blob, Cosmos,
  Azure SQL sync, and SQL Always On stores.
- Restored in-memory/Blob records from cloned originals when an interruption
  happens before the method returns.
- Restored SQL and Cosmos records with CAS replacement when a later interruption
  occurs after earlier replay claims were persisted.
- Added a provider-level regression test using an interrupting clock across all
  five store families.

### Verification

- Focused red run showed all five store families leaked `IN_FLIGHT` state before
  implementation.
- Focused green run:
  `uv run pytest tests/test_adapters.py::test_provider_failover_replay_candidates_rolls_back_on_interruption -q`
  -> 5 passed
- Full package gates:
  `uv run pytest -q` -> 202 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 42: Optional SDK Typing Gates

### Findings Accepted

- **Q-P1-5:** Azure and Kafka adapters still used broad `Any` around optional
  SDK integration points, making strict type checking less useful near provider
  boundaries.

### Fixes Implemented

- Added `TYPE_CHECKING` imports for Azure Blob `ContainerClient` and
  `confluent_kafka.Producer` so optional SDK names are visible to type checkers
  without runtime imports.
- Added a structural Azure container-client protocol so tests and custom
  container clients remain supported without weakening runtime dependency
  boundaries.
- Tightened Kafka producer factory casting to the optional SDK producer type.
- Added a typed bytes guard for Azure download content, converting non-bytes
  SDK responses into `RetryableStoreError`.

### Verification

- Focused green run:
  `uv run pytest tests/test_azure_blob_and_file_sink.py tests/test_kafka_operations.py -q`
  -> 23 passed
- Full package gates:
  `uv run pytest -q` -> 202 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 43: Error And Outcome Semantics Documentation

### Findings Accepted

- **Result-pattern follow-up:** a broad generic Rust-style `Result[T, E]` would
  reduce Python ergonomics across the store and sink protocols, but the repo
  needed an explicit policy so future changes preserve the useful distinction
  already introduced by `AdminActionStatus`.

### Fixes Implemented

- Documented the boundary in `docs/operations.md`: expected operator branches
  use `AdminActionStatus`; invalid input, ambiguous durability, provider setup,
  publish classification, claim conflicts, and duplicate conflicts remain
  typed exceptions.
- Added a packaging/docs regression test so the operation guide keeps naming
  the status outcomes and public error taxonomy.

### Verification

- Focused red run:
  `uv run pytest tests/test_packaging_docs.py::test_operations_docs_describe_error_and_outcome_policy -q`
  -> failed because `docs/operations.md` did not yet describe the policy.
- Focused green run:
  `uv run pytest tests/test_packaging_docs.py::test_operations_docs_describe_error_and_outcome_policy -q`
  -> 1 passed
- Full package gates:
  `uv run pytest -q` -> 203 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 44: Canonical Data Model Documentation

### Findings Accepted

- **Q-P2-5:** SQL columns, Blob metadata/JSON, and the proposed Cosmos JSON
  shape used inconsistent timestamp and field-name conventions, leaving future
  provider serialization choices underspecified.

### Fixes Implemented

- Added `docs/data-model.md` mapping canonical Python field names to Blob,
  Cosmos JSON, and SQL renderings.
- Chose snake_case `*_at_epoch_ms` for future queryable JSON timestamp fields,
  while documenting why SQL keeps typed `*_utc` columns and current Blob JSON
  keeps ISO-8601 `*_at` keys.
- Updated the proposal's Cosmos item shape from camelCase to snake_case names
  such as `schema_id`, `publishing_mode`, and `created_at_epoch_ms`.
- Added a packaging/docs regression test that asserts the mapping document
  exists and the proposal no longer advertises the old camelCase timestamp key.

### Verification

- Focused red run:
  `uv run pytest tests/test_packaging_docs.py::test_data_model_docs_map_canonical_fields_to_adapter_renderings -q`
  -> failed because `docs/data-model.md` did not exist.
- Focused green run:
  `uv run pytest tests/test_packaging_docs.py::test_data_model_docs_map_canonical_fields_to_adapter_renderings -q`
  -> 1 passed
- Full package gates:
  `uv run pytest -q` -> 204 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 45: Dual-Region Put Phase Parallelism

### Findings Accepted

- **P-P0-3:** dual-region Blob `put()` performed four sequential network-shaped
  operations: prepare primary, prepare secondary, accept primary, accept
  secondary.

### Fixes Implemented

- Changed `DualRegionBlobOutboxStore.put()` to run the two prepare operations
  concurrently, then run the two accept operations concurrently after both
  prepares have completed.
- Preserved the correctness barrier: no accept starts before both regions have
  reached the prepared phase.
- Added a regression test that coordinates both regions and fails if either
  phase is executed serially.

### Verification

- Focused red run:
  `uv run pytest tests/test_adapters.py::test_dual_region_blob_put_runs_prepare_and_accept_phases_concurrently -q`
  -> failed because primary prepare waited for secondary prepare, which never
  started under the sequential implementation.
- Focused green run:
  `uv run pytest tests/test_adapters.py::test_dual_region_blob_put_runs_prepare_and_accept_phases_concurrently -q`
  -> 1 passed
- Adapter and package gates:
  `uv run pytest tests/test_adapters.py -q` -> 101 passed;
  `uv run pytest -q` -> 205 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 46: Top-Level Public API Polish

### Findings Accepted

- **Q-P0-2:** the top-level `durable_outbox` package exported most obvious
  primitives but still missed `MessageSink`, `RetryPolicy`, and package version
  metadata, while the README quickstart taught imports from `durable_outbox.core`.

### Fixes Implemented

- Re-exported `MessageSink`, `RetryPolicy`, and `__version__` from
  `durable_outbox`.
- Kept `__version__` aligned with installed package metadata, with a source-tree
  fallback matching the current project version.
- Updated the README quickstart to import `OutboxDispatcher` and `OutboxEvent`
  from the top-level package.
- Added a packaging/docs regression test for the top-level public API surface.

### Verification

- Focused red run:
  `uv run pytest tests/test_packaging_docs.py::test_top_level_package_exports_obvious_public_api -q`
  -> failed because `MessageSink` was not in `durable_outbox.__all__`.
- Focused green run:
  `uv run pytest tests/test_packaging_docs.py::test_top_level_package_exports_obvious_public_api -q`
  -> 1 passed
- Full package gates:
  `uv run pytest -q` -> 206 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.

## Batch 47: Dual-Write Failure Contract Coverage

### Findings Accepted

- **A-NEW-P2-3:** the RPO=0 dual-write contract needed stronger tests for
  secondary-region failures during `put()`.

### Fixes Implemented

- Added a secondary-accept failure regression test that proves
  `DualRegionBlobOutboxStore.put()` raises and does not return an RPO=0 receipt
  when the secondary accept write fails.
- Fixed `BlobOutboxStore._accept_prepared()` so it clones the cached prepared
  record before mutation and installs the accepted state only after the
  conditional blob write succeeds.
- Preserved the backend prepared record when the secondary accept write fails,
  leaving partial-write repair/retry paths with an accurate local and persisted
  view.

### Verification

- Focused red run:
  `uv run pytest tests/test_adapters.py::test_dual_region_blob_put_does_not_cache_secondary_accept_failure -q`
  -> failed because the secondary in-memory cache showed `accepted=True` after
  the accept write raised.
- Focused green run:
  `uv run pytest tests/test_adapters.py::test_dual_region_blob_put_does_not_cache_secondary_accept_failure tests/test_adapters.py::test_dual_region_blob_put_runs_prepare_and_accept_phases_concurrently tests/test_adapters.py::test_dual_region_blob_accepts_only_after_both_regions -q`
  -> 3 passed
- Adapter and package gates:
  `uv run pytest tests/test_adapters.py -q` -> 102 passed;
  `uv run pytest -q` -> 207 passed, 2 skipped;
  `uv run ruff check .` -> all checks passed;
  `uv run ruff format --check .` -> 51 files already formatted;
  `uv run ty check` -> all checks passed;
  `uv build` -> source distribution and wheel built successfully.
