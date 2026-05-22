# Durable Outbox Architecture Review

Date: 2026-05-22

## Scope

This review covered the durable outbox package across API, dispatcher, store, provider, operations, testing, packaging, and Ito specification layers.

## Review Findings

### Core API Boundary

The event envelope is the only common input across providers, so it must reject ambiguous values before provider-specific code sees them. The review found that naive datetimes, non-positive claim limits, oversized provider payloads, and incompatible duplicate events were not consistently rejected across stores.

Change proposal: `001.01-03_harden-core-contracts`

Implemented improvements:

- Timezone-aware datetime validation for `OutboxEvent`.
- Shared positive-limit validation for claim and replay APIs.
- Shared provider max-payload enforcement.
- Provider-independent `DuplicateEventConflictError` for incompatible duplicate puts.

### Dispatcher Acknowledgement Boundary

The dispatcher is sink-agnostic, but its metrics were Kafka-specific. More importantly, a failure in `mark_sent()` after a successful sink acknowledgement was handled like a retryable publish failure. That incorrectly crosses the at-least-once acknowledgement boundary.

Change proposal: `001.01-02_generalize-dispatcher-ack-path`

Implemented improvements:

- Generic `outbox_publish_*` metrics.
- Explicit `outbox_mark_sent_failures_total` metric.
- `DispatchSummary.store_update_failed` for post-ack store failures.
- Post-ack store failures leave the event `IN_FLIGHT` for stale-claim or failover recovery rather than marking it pending as if publish failed.

### Deterministic Lifecycle Time

Stores used direct system time calls for accepted, claimed, failed, and replay timestamps. This made provider behavior harder to test and could diverge from dispatcher clocks.

Change proposal: `001.01-04_inject-store-clocks`

Implemented improvements:

- Optional `Clock` injection for memory, Blob, dual-region Blob, Cosmos, and SQL stores.
- Store lifecycle timestamps now use injected clocks while preserving system-clock defaults.

### Provider Concurrency Semantics

Blob already modeled ETag-style conditional writes. Cosmos and SQL provider clients did not model compare-and-set updates, so shared-client tests could not prove single-winner claims under stale snapshots.

Change proposal: `001.10-01_add-provider-cas-contracts`

Implemented improvements:

- Versioned Cosmos and SQL stored records.
- Conditional `replace(..., expected_version=...)` provider protocols.
- In-memory provider clients return snapshots rather than shared mutable records.
- Claim, replay, repair, and terminal transitions route through conditional replace.
- Shared-client single-winner tests for Cosmos and SQL.

## Validation Evidence

Commands run from `durable-outbox-python`:

- `uv run pytest`: 83 passed
- `uv run ruff check .`: passed
- `uv run ruff format --check .`: passed
- `uv run ty check`: passed
- `uv build`: built sdist and wheel

Commands run from the repository root:

- `ito validate --changes --strict`: all 4 active change proposals valid
- `ito list`: all 4 review-driven changes complete

## Residual Risks

The package still uses in-memory fake provider clients for local certification. Live Azure Blob, Cosmos DB, SQL, and Kafka integration jobs remain credential-gated production hardening work.
