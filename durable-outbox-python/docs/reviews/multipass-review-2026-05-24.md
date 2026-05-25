# Durable Outbox — Multipass Review

**Date:** 2026-05-24
**Reviewer:** Claude (orchestrated multipass with specialist subagent fan-out)
**Scope:** `durable-outbox-python` package at version 0.1.0 (alpha)
**Method:** Six passes — aims comprehension, architecture, security, performance, code quality, synthesis. Architecture and security passes performed inline after subagent stalls; performance and code-quality passes performed by background subagents (full reports preserved verbatim in their respective sections).

---

## 1. Aims and validation

### 1.1 Stated aims (from `docs/durable-outbox-rpo0-proposal.md` and `README.md`)

A reusable Python 3.14 durable outbox library providing:

1. Storage- and sink-agnostic core (`DurableOutboxStore`, `MessageSink` protocols).
2. **At-least-once** publishing with per-`event_id` idempotency. Consumers dedupe.
3. **Explicit RPO=0 capability declarations** — RPO=0 is a write-acknowledgement contract (`put()` returns success only after the event is durable at the adapter's failover boundary), not a storage-product label.
4. Adapter set: dual-region Azure Blob (RPO=0 via app-level dual write), single-region Blob (non-RPO=0), Cosmos DB strong consistency, Azure SQL `sp_wait_for_database_copy_sync`, SQL Server Always On synchronous-commit.
5. Kafka sink with `acks=all`, `enable.idempotence=true`.
6. **Failover replay** of all accepted events where `expires_at >= failover_started_at` (TTL freezes at failover; includes `SENT` events).
7. **Cleanup freeze** during failover.
8. **Ordered and unordered modes** with per-key sequencing in ordered mode.
9. Stateless, horizontally scalable publisher instances.
10. Provider certification test harness validating the matrix in proposal §25.
11. Operational hooks: status, audit, Prometheus metrics, OpenTelemetry headers, admin replay.
12. Throughput target: **sustained 1000 msg/min/topic** with horizontal scaling headroom.

### 1.2 Aims validation

**The aims are coherent and well-grounded.** The four contracts (acceptance, failure/timeout, delivery, failover) in proposal §6 form a sound, internally consistent at-least-once model. The RPO=0-as-contract framing is correct and resolves the common confusion between Azure GRS/RA-GRS (asynchronous) and true zero-RPO acceptance. The capability matrix (§16) is accurate. The per-event-id idempotency requirement on `put()` correctly handles ambiguous timeouts. The failover replay predicate `expires_at >= failover_started_at` (not `now`) correctly accounts for TTL-freeze during recovery, and including `SENT` in the replay set is the right call for cross-cluster Kafka DR.

**Two aims-level concerns that don't appear in the proposal:**

- **Active/passive vs active/active in dual-region Blob.** The proposal §15.1 frames replay as "new region starts" — implying role-swap on disaster. The library needs a documented role/owner abstraction so the "new active region" can become the writer of `SENT` updates, otherwise the recovery procedure is unspecified and the implementation (see [A-P0-2]) will be fundamentally incomplete.
- **Cosmos strong-consistency RU economics.** Strong consistency in multi-region single-write Cosmos is RU-expensive (~2-3× session reads); the 1000 msg/min/topic target plus failover replay of N hours of TTL-valid history could be financially painful. The proposal §15.2 acknowledges this in pros/cons but doesn't quantify; an aim-level addition should be "publish a sizing guidance doc per adapter".

Otherwise: the aims are achievable, the design boundaries are well-chosen, and the work to satisfy them is bounded.

### 1.3 Design vs aims — overall verdict

The **core protocols and dispatcher match the aims well**. The dispatcher's at-least-once boundary (mark_sent failure leaves the event `IN_FLIGHT` for stale reclaim, rather than re-publishing) was correctly fixed in change `001.01-02_generalize-dispatcher-ack-path` and is the right call. Envelope validation (`OutboxEvent.__post_init__`), payload-size enforcement (`enforce_payload_size`), clock injection, and the CAS-versioned record protocols (`001.10-01_add-provider-cas-contracts`) are all sound.

**The aims are not yet fully delivered, however.** Specifically:

- The **SQL and Cosmos adapters declare `rpo_zero_for_accepted_events=True` but have no real backend implementation** — only in-memory test shims (`stores/sql.py:110`, `stores/cosmos.py:78`). The capability is a lie until pyodbc/azure-cosmos clients land. (See [A-P0-1].)
- The **dual-region Blob adapter has no failover role-swap** — both `claim_batch` and `failover_replay_candidates` read from the primary unconditionally (`stores/blob_geo.py:542, 585`). When the primary region is unavailable (the entire RPO=0 use case), the secondary cannot take over. (See [A-P0-2].)
- **Failover cleanup-freeze is not released on replay error**, leaving cleanup frozen forever after a single transient sink failure (`core/failover.py:18-31`). (See [A-P0-3].)
- The **default `OrderingLockBackend` in `BlobOutboxStore` is in-process only** (`stores/blob_geo.py:148-150`); two publishers will both acquire the same ordering-key lease and ordered mode is silently incorrect under horizontal scaling. (See [A-P0-4].)
- **Default clients are in-memory** across all adapters — a user constructing `BlobOutboxStore()` without a `client` argument gets a process-local store but with capabilities still claiming "BlobOutboxStore" (`stores/blob_geo.py:146`). Same for SQL/Cosmos. Silent data-loss footgun. (See [A-P0-5].)

These are concentrated in the dual-region Blob and ordered-mode paths — the very capabilities that distinguish this library from a trivial outbox.

### 1.4 Validation evidence

Independently re-run during this review:

- `uv run ty check` → **All checks passed**
- `uv run pytest --collect-only` → 87 tests collected
- `uv run ruff check . --statistics` → no output (clean)

The existing `docs/architecture-review.md` (2026-05-22) records prior closed findings; **all P0/P1 items in this report are new or scope-expanded relative to that document**.

---

## 2. How to read this report

- **Sections 3-6** are organised by review dimension. Each finding is **self-contained** so a parallel coding agent can pick it up without reading the others.
- **Finding IDs**: `[<dim>-<priority>-<seq>]` where `<dim>` is `A` (architecture), `S` (security), `P` (performance), `Q` (quality). Use these IDs in branch names and PR titles.
- **Priority guide**:
  - **P0** — correctness/durability violation, broken DR/RPO=0 claim, security boundary breach, silent data loss, blocks the throughput target on any plausible deployment.
  - **P1** — significant design or operability defect under load/failure; missing capability that materially undermines a stated aim.
  - **P2** — avoidable cost (perf, RU, network), contract not enforced where it should be, missing test gate.
  - **P3** — design smell, hardening, future-proofing.
  - **NIT** — cosmetic / micro-improvement.
- **Independence/Dependencies** notes are explicit for each finding to enable parallel work-packet planning.
- **Recommendations** are written so a coding agent without the full conversation context can execute them: name files, signatures, helpers, and (where useful) one-line acceptance tests.

### Suggested parallelisation pattern

Six independent work tracks (each pickable in parallel):

| Track | Findings | Notes |
|---|---|---|
| **T1 — DR correctness** | A-P0-2, A-P0-3, A-P1-1, A-P1-4, A-P2-2 | Dual-region role-swap + failover replay reliability |
| **T2 — Ordering correctness** | A-P0-4, A-P1-2, A-P2-3, A-P2-4 | Cross-process lease backend, topic-scoped ordering |
| **T3 — Real adapter backends** | A-P0-1, A-P0-5, Q-P0-1 | pyodbc SQL client, azure-cosmos client, real-default safety |
| **T4 — Dispatcher throughput** | P-P0-1, P-P1-2, P-P2-5 | Concurrent publish, Kafka loop yield, no jitter |
| **T5 — Storage scan elimination** | P-P0-2, P-P0-3, P-P0-4, P-P1-3, P-P1-4 | Replace `list_records` scans with predicate queries |
| **T6 — API surface + lint** | Q-P0-2, Q-P1-1, Q-P1-2, Q-P1-3, Q-P1-4, Q-P1-5 | Re-exports, docstrings, lint rule expansion, pytest config |

Security findings S-P0-1 through S-P3-3 cross-cut and should be folded into the relevant tracks per their `Independence` notes.

---

## 3. Architecture findings

### [A-P0-1] SQL and Cosmos adapters have no real backend — `rpo_zero_for_accepted_events=True` is a lie

**Where**: `durable_outbox/stores/sql.py:110-158` (`InMemorySqlOutboxClient` is the only client implementation), `durable_outbox/stores/sql.py:419-425` (capability declares RPO=0); `durable_outbox/stores/cosmos.py:78-119` (`InMemoryCosmosOutboxClient` only), `durable_outbox/stores/cosmos.py:144-151` (capability declares RPO=0 derived from `config.is_rpo_zero`).

**Problem**: The `SqlOutboxClient` and `CosmosOutboxClient` protocols are defined but no real `pyodbc` / `azure-cosmos` implementation ships. Tests run against in-memory shims that trivially "succeed" `wait_for_database_copy_sync` (`sql.py:152-155`) and trivially honour `version`-based CAS. The capability declared on `AzureSqlSyncOutboxStore`, `SqlAlwaysOnOutboxStore`, and `CosmosStrongOutboxStore` is therefore unverifiable — it can never be true in production because there is no production path. Worse, the SQL claim CTE specified in proposal §15.3 (`WITH (READPAST, UPDLOCK, ROWLOCK) ... OUTPUT inserted.*`) is not implemented in any client; `claim_batch` calls `list_records()` and sorts in Python (`sql.py:202-211`). Cosmos has the same shape — `claim_batch` calls `list_records()` which would be a cross-partition scan in real Cosmos (`cosmos.py:349-358`).

**Impact**: The library's headline differentiator (per-adapter RPO=0 declarations backed by real semantics) is not yet delivered for two of the three RPO=0 adapters. Anyone enabling `AzureSqlSyncOutboxStore` or `CosmosStrongOutboxStore` today gets in-memory storage and a fraudulent capability declaration. This blocks the proposal's MVP roadmap §31.

**Recommendation**: Two work items, parallelisable.

1. **SQL real client** (`stores/sql.py`):
   - Add `PyodbcSqlOutboxClient(SqlOutboxClient)` to a new module `stores/sql_pyodbc.py`. Implement `get`, `upsert_new` (using `MERGE` with `WHEN NOT MATCHED INSERT` and `WHEN MATCHED THEN SELECT existing`), `replace` (parameterised `UPDATE ... WHERE event_id=? AND row_version=?`), `claim_batch_pending` (new protocol method) using the proposal §15.3 CTE, `list_replay_candidates` (using `IX_outbox_replay`), `delete` (parameterised), `wait_for_database_copy_sync` (executes `EXEC sys.sp_wait_for_database_copy_sync @partner_server=?, @partner_database=?`), `synchronized_secondary_count` (queries `sys.dm_hadr_database_replica_states`).
   - Add `claim_batch_pending` and `list_replay_candidates` to `SqlOutboxClient` protocol so the base store delegates to them. Refactor `_SqlOutboxStoreBase.claim_batch` and `.failover_replay_candidates` to call them instead of `list_records()`.
   - Provide a `tests/integration/test_sql_pyodbc.py` gated on `DURABLE_OUTBOX_SQL_CONNECTION_STRING`.

2. **Cosmos real client** (`stores/cosmos.py`):
   - Add `AzureCosmosOutboxClient` to a new module `stores/cosmos_azure.py`. Use `azure.cosmos.aio.CosmosClient`. The protocol must accept the partition key as a first-class argument — change `get(event_id)` to `get(event_id, *, partition_key)` and have callers compute partition_key via `partition_key_for(event)` (already exists at `cosmos.py:337-341`). Add `list_claimable_within_partition(pk, *, limit, now)` to the protocol and call once per `topic#bucket` (driven by `config.unordered_buckets`) and per ordered-key partition.
   - Validate `config.is_rpo_zero` against the live account at startup (`async def validate_account()` calling `CosmosClient.read_account()` to check `consistency_level`, `read_regions`, `write_regions`).

3. **Until real clients land**, gate the certified-mode RPO=0 capability on `client` *type*:
```python
# In AzureSqlSyncOutboxStore.__init__:
if isinstance(self.client, InMemorySqlOutboxClient) and not config.allow_inmemory:
    raise ConfigurationError(
        "AzureSqlSyncOutboxStore with InMemorySqlOutboxClient is not RPO=0; "
        "set AzureSqlSyncConfiguration(allow_inmemory=True) for tests."
    )
```
Same in `CosmosStrongOutboxStore`. This is a 30-minute fix that closes the lie window today.

**Independence**: Step 3 (gate) is independent and ships immediately. Steps 1 and 2 (real clients) are independent of each other and of every other finding in this document, but share a benchmark harness with [P-P0-2] and [P-P0-5].

---

### [A-P0-2] `DualRegionBlobOutboxStore` has no failover role-swap — secondary cannot take over

**Where**: `durable_outbox/stores/blob_geo.py:489-649`. Specifically:
- `claim_batch` (line 542): unconditionally returns `await self.primary.claim_batch(limit=limit)`.
- `failover_replay_candidates` (line 585): unconditionally reads from `self.primary`.
- `mark_sent`, `mark_pending_after_retryable_failure`, `mark_failed` (lines 545-577): all write to `self.primary` then call `_mirror_terminal_update` to copy state to `self.secondary`.
- `self.records = self.primary.records` (lines 524, 533, 623): aliases the primary's in-memory mirror.

**Problem**: The whole point of dual-region writes is that a region can fail. The proposal §15.1 "Blob failover replay" begins with **"New region starts"** — implying the secondary becomes the new active region and starts claiming, publishing, and writing `SENT` updates. The current implementation provides no API for that role transition. If the primary blob account is unreachable, every method on `DualRegionBlobOutboxStore` raises. The secondary store is a write-only mirror that can never be claimed against; on disaster, accepted events sit in the secondary container until manual intervention.

**Impact**: P0 — the dual-region adapter does not actually support failover, only redundant acceptance. The RPO=0 acceptance contract is met (writes are visible in both regions before `put()` returns), but the **failover-replay contract is not met** because there's no path for "new region" to be anything other than the original primary. This is the single biggest gap between proposal and implementation.

**Recommendation**:

1. Introduce an explicit role abstraction:
```python
# durable_outbox/stores/blob_geo.py
class RegionRole(StrEnum):
    PRIMARY = "PRIMARY"
    SECONDARY = "SECONDARY"

class DualRegionBlobOutboxStore:
    def __init__(self, *, primary_client, secondary_client, ..., active_role: RegionRole = RegionRole.PRIMARY):
        self._active_role = active_role
        ...

    @property
    def _active(self) -> BlobOutboxStore:
        return self.primary if self._active_role is RegionRole.PRIMARY else self.secondary

    @property
    def _standby(self) -> BlobOutboxStore:
        return self.secondary if self._active_role is RegionRole.PRIMARY else self.primary

    async def promote_to(self, role: RegionRole) -> None:
        """Switch which region acts as the writer. Idempotent."""
        self._active_role = role
```

2. Every `claim_batch`, `mark_sent`, `mark_pending_after_retryable_failure`, `mark_failed`, `failover_replay_candidates`, `repair_failed_to_pending` must route through `self._active`. Mirror-update goes to `self._standby`.

3. Delete the `self.records = self.primary.records` aliases — they leak internal state and bake in the assumption.

4. Add an operational doc `docs/dual-region-failover-runbook.md` describing: detect primary unhealthy → call `await store.promote_to(RegionRole.SECONDARY)` on the publisher in the surviving region → `await FailoverReplayer.replay_once(failover_started_at=...)`.

5. Acceptance test in `tests/test_failover_ordering_cleanup.py` named `test_dual_region_failover_swaps_active_role`: stage events in both regions via real `put()`, then `await store.promote_to(SECONDARY)` and assert that subsequent claims come from the secondary container.

**Independence**: Independent of every other finding except A-P1-1 (failover replayer error handling), which it composes with. Should ship in the same PR as A-P1-1.

---

### [A-P0-3] `FailoverReplayer` leaves cleanup frozen forever on any error

**Where**: `durable_outbox/core/failover.py:18-31`

**Problem**:
```python
async def replay_once(self, *, failover_started_at, limit) -> ReplaySummary:
    await self.store.freeze_cleanup(reason="failover replay")
    candidates = await self.store.failover_replay_candidates(...)
    replayed = 0
    for claimed in candidates:
        result = await self.sink.publish(claimed.event)        # ← if this raises
        await self.store.mark_sent(claimed, result)            # ← or this raises
        replayed += 1
    return ReplaySummary(replayed=replayed)

async def complete_replay(self) -> None:
    await self.store.resume_cleanup()                          # ← never called
```
A single `RetryablePublishError` on any event raises out of `replay_once`, callers never reach `complete_replay`, `cleanup_frozen=True` persists across process restarts (it is in-memory state — even worse: it doesn't persist, so a fresh process won't know cleanup was supposed to stay frozen, which is its own bug — see [A-P1-1]).

**Impact**: P0. The freeze-no-resume path causes unbounded storage growth post-failover; the freeze-loss-on-restart path causes data loss (cleanup deletes events still needed for replay). The only existing test that exercises freeze survival (`tests/test_failover_ordering_cleanup.py::test_cleanup_freeze_survives_replay_failure_until_completion`) only checks that `freeze_cleanup` blocks `cleanup_sent` synchronously — it does NOT cover the error-during-replay path.

**Recommendation**:

1. Wrap the loop in try/finally that always resumes cleanup if the entire replay completes (success or partial), and emit a metric otherwise:
```python
async def replay_once(self, *, failover_started_at, limit) -> ReplaySummary:
    await self.store.freeze_cleanup(reason="failover replay")
    replayed = 0
    errored = 0
    candidates = await self.store.failover_replay_candidates(...)
    for claimed in candidates:
        try:
            result = await self.sink.publish(claimed.event)
            await self.store.mark_sent(claimed, result)
            replayed += 1
        except Exception:
            errored += 1
            # Leave event IN_FLIGHT; stale-claim reclaim will retry.
            self._logger.warning("failover replay publish failed", extra={"event_id": claimed.event.event_id})
    return ReplaySummary(replayed=replayed, errored=errored)

async def complete_replay(self) -> None:
    # Caller invokes this once the watermark is met. Idempotent.
    await self.store.resume_cleanup()
```
Note: `complete_replay` is called explicitly *after* the watermark is met across multiple `replay_once` invocations. The library should not auto-resume on partial completion.

2. **Persist freeze state**: `freeze_cleanup` writes a marker blob/record `outbox/v1/cleanup-frozen.json` containing `{reason, frozen_at, expected_resume_after}`. `resume_cleanup` deletes it. On store startup, the store reads the marker and rehydrates `self.cleanup_frozen`. This survives process restarts during long-running failover replays.

3. Add `ReplaySummary.errored: int` field.

4. New tests in `tests/test_failover_ordering_cleanup.py`:
   - `test_replay_continues_after_individual_publish_failure_and_increments_errored`
   - `test_freeze_state_persists_across_store_restart`

**Independence**: Independent. Recommend bundling with A-P0-2 (dual-region role swap) since both touch the failover code path.

---

### [A-P0-4] `BlobOutboxStore` default ordering lock backend is in-process — ordered mode silently broken under multiple publishers

**Where**: `durable_outbox/stores/blob_geo.py:148-150` (default `InMemoryOrderingLockBackend()`), `durable_outbox/core/ordering.py:34-65` (process-local dict).

**Problem**: The proposal §18 explicitly specifies "Blob: Per-key lock blob with Blob lease" for ordered-mode coordination across publisher instances. The library defines `OrderingLockBackend` as a protocol but ships only `InMemoryOrderingLockBackend` which holds leases in a Python dict. Two `BlobOutboxStore` instances (the explicit goal of stateless horizontal scaling per §2) will independently grant the same per-key lease and publish the same ordered key concurrently. The capability `supports_ordering=True` is therefore another silent lie under multi-publisher deployment.

**Impact**: P0 correctness for ordered mode. Producers asking for `PublishingMode.ORDERED` get unordered-with-extra-steps semantics. The bug is invisible in single-publisher tests (all current tests).

**Recommendation**:

1. Implement `AzureBlobOrderingLockBackend(OrderingLockBackend)` using Azure Blob Lease (15-60s renewable). New module `stores/blob_lease.py`:
```python
class AzureBlobOrderingLockBackend:
    def __init__(self, blob_client: BlobClientProtocol):
        self._client = blob_client

    async def acquire(self, *, lock_name, owner_token, now, lease_duration) -> OrderingLockLease | None:
        # 1. Ensure the lock blob exists (PUT if_none_match=*, ignore exists).
        # 2. Acquire a Blob lease with duration=lease_duration.seconds, propose lease_id=owner_token.
        # 3. On HTTP 409 LeaseAlreadyPresent → return None.
        # 4. Return OrderingLockLease(lock_name, owner_token, expires_at=now+lease_duration).

    async def release(self, lease) -> None:
        # Release the Blob lease by lease_id. Ignore 404.
```

2. Change `BlobOutboxStore.__init__` to **require** an explicit `ordering_lock_backend` when `supports_ordering=True` and the client is anything other than `InMemoryBlobClient`. Refuse the default in production:
```python
def __init__(self, *, client, ordering_lock_backend=None, ...):
    if ordering_lock_backend is None:
        if isinstance(self.client, InMemoryBlobClient):
            ordering_lock_backend = InMemoryOrderingLockBackend()
        else:
            ordering_lock_backend = AzureBlobOrderingLockBackend(self.client)
    self.ordering_lock_backend = ordering_lock_backend
```

3. Document the trade-off in `docs/providers.md`: ordered mode requires a cross-process lease backend; a non-Blob backend (e.g. Redis) can be plugged in via the protocol.

4. Acceptance test in `tests/test_adapters.py` named `test_blob_ordered_mode_blocks_second_publisher_with_blob_lease`: two `BlobOutboxStore` instances sharing the same `InMemoryBlobClient` (simulating shared Azure container) and `AzureBlobOrderingLockBackend(shared_client)` — publish two ordered events for the same key from both publishers, assert exactly one succeeds at a time.

**Independence**: Independent. Composes with [A-P0-5] (defaults safety) and [A-P1-2] (topic-scoped ordering keys).

---

### [A-P0-5] Adapter defaults are in-memory clients — silent data-loss footgun

**Where**: `durable_outbox/stores/blob_geo.py:146` (`self.client = client or InMemoryBlobClient()`), `stores/sql.py:171` (`self.client = client or InMemorySqlOutboxClient()`), `stores/cosmos.py:139` (`self.client = client or InMemoryCosmosOutboxClient()`), all four `_SqlOutboxStoreBase` subclasses, `DualRegionBlobOutboxStore.__init__`.

**Problem**: A user writing `BlobOutboxStore()` or `AzureSqlSyncOutboxStore()` or `CosmosStrongOutboxStore(CosmosConfiguration(...))` gets a fully functional in-memory store with `capabilities.store_name = "BlobOutboxStore"` (not "InMemoryBlobOutboxStore"). The data lives in process memory and disappears on restart. The library makes no warning, no log, no error — and the capability declarations actively mislead operators.

**Impact**: P0 data loss in any deployment where a developer follows the README quickstart and forgets to wire the real client. There is no `# WARNING: in-memory only` log line, no `ConfigurationError`, nothing.

**Recommendation**: Make the in-memory default explicit and gated:

1. Rename the default to a separate test factory:
```python
# stores/blob_geo.py
class BlobOutboxStore:
    def __init__(self, *, client: BlobClientProtocol, ...):
        if client is None:
            raise ConfigurationError(
                "BlobOutboxStore requires an explicit client. "
                "For tests, use BlobOutboxStore.for_testing(); "
                "for Azure use BlobOutboxStore(client=AzureBlobClient.from_connection_string(...))"
            )
        self.client = client
        ...

    @classmethod
    def for_testing(cls, **kwargs) -> BlobOutboxStore:
        return cls(client=InMemoryBlobClient(), **kwargs)
```

2. Same pattern in `_SqlOutboxStoreBase`, `CosmosStrongOutboxStore`, `DualRegionBlobOutboxStore`.

3. The capability `store_name` for `for_testing()` should be `"InMemoryBlobOutboxStore"` — wire a parameter through `BlobOutboxStore.__init__` so the test path uses a clearly non-production name.

4. Update all tests (`tests/test_adapters.py`, `tests/test_failover_ordering_cleanup.py`, etc.) to use `.for_testing()`.

5. Update README quickstart to use real `AzureBlobClient.from_connection_string(...)` or import `BlobOutboxStore.for_testing` explicitly.

**Independence**: Independent. Mechanical change with high test churn; isolate to its own PR for cleaner reviews. Pairs naturally with [Q-P1-1] (top-level `__init__.py` re-exports).

---

### [A-P1-1] `freeze_cleanup` state is process-local and lost on restart

**Where**: `durable_outbox/stores/blob_geo.py:154-155, 311-317`, `stores/cosmos.py:142-143, 308-314`, `stores/sql.py:174-175, 339-345`, `stores/memory.py:58-59, 195-201`.

**Problem**: Every adapter stores `cleanup_frozen: bool` and `cleanup_freeze_reason: str | None` as Python instance attributes. Crashing or restarting the publisher loses the freeze state. A separate cleanup worker (different process) does not see the freeze at all.

**Impact**: P1. After a failover begins and `freeze_cleanup` is called on publisher A, if publisher A crashes mid-replay, publisher B's cleanup loop will start deleting SENT events that should still be replayable. This is silent data loss during recovery — the worst possible time.

**Recommendation**: Persist the freeze marker in the store's backing media.

For Blob:
```python
# stores/blob_geo.py
_FREEZE_MARKER_BLOB = "outbox/v1/cleanup-frozen.json"

async def freeze_cleanup(self, *, reason: str) -> None:
    payload = json.dumps({"reason": reason, "frozen_at": self.clock.utcnow().isoformat()}).encode()
    await self.client.put_blob(_FREEZE_MARKER_BLOB, payload, metadata={}, if_none_match=False)
    self.cleanup_frozen = True

async def resume_cleanup(self) -> None:
    await self.client.delete_blob(_FREEZE_MARKER_BLOB)
    self.cleanup_frozen = False

async def _is_cleanup_frozen(self) -> bool:
    blob = await self.client.get_blob(_FREEZE_MARKER_BLOB)
    return blob is not None

async def cleanup_sent(self, *, now, safety_margin) -> int:
    if await self._is_cleanup_frozen():
        return 0
    ...
```
For SQL: single-row table `durable_outbox_freeze (reason NVARCHAR(256), frozen_at_utc DATETIME2)` queried with `SELECT COUNT(*)`.
For Cosmos: single document with id `cleanup-freeze` in a known partition.

Update protocol `DurableOutboxStore` to make `freeze_cleanup`/`resume_cleanup` semantics "durable across restarts" explicit in the docstring.

**Independence**: Independent. Strongly recommended in the same PR as [A-P0-3].

---

### [A-P1-2] Cosmos and SQL ordering-key lock scope omits topic — cross-topic ordering-key collision

**Where**: `durable_outbox/stores/cosmos.py:367-381` (`_in_flight_ordering_keys` uses `effective_ordering_key` without topic prefix); `stores/sql.py:384-398` (same); `stores/memory.py:238-252` (same). Blob version `stores/blob_geo.py:446-460` correctly uses `_ordering_scope` (line 677-681) which includes topic.

**Problem**: For two different topics with the same `ordering_key` (e.g. `"user-123"` on `"orders"` and on `"profiles"`), the Cosmos/SQL/memory adapters treat them as the same lock scope and serialise them unnecessarily. The Blob adapter correctly uses `(topic, ordering_key)` as the scope.

**Impact**: P1. Throughput degradation under multi-topic ordered workloads. Worse, the inconsistency between adapters means tests that pass for Blob may not catch the bug for SQL/Cosmos. Not a correctness violation (over-restriction, not under-restriction) but breaks the proposal's §18 "different keys can publish concurrently" if "different keys" includes "same key, different topic".

**Recommendation**: Promote `_ordering_scope` from `stores/blob_geo.py:677-681` to a public helper in `core/ordering.py`:
```python
# core/ordering.py
def ordering_scope(event: OutboxEvent) -> str | None:
    key = event.effective_ordering_key
    if key is None:
        return None
    return f"{event.topic}\0{key}"
```
Update all four adapters' `_in_flight_ordering_keys` to use `ordering_scope(record.event)` instead of `record.event.effective_ordering_key`. Parametrise existing test `test_blob_ordering_lock_scope_includes_topic` across all adapters (`memory`, `sql`, `cosmos`, `blob`, `dual_blob`).

**Independence**: Independent. Pure refactor.

---

### [A-P1-3] `DurableOutboxStore` protocol is missing `cleanup_sent` and `repair_failed_to_pending`

**Where**: `durable_outbox/core/store.py:13-48` (declares 8 methods), but all adapters implement two additional methods (`cleanup_sent`, `repair_failed_to_pending`) that the `operations.OutboxAdminActions` protocol depends on (`durable_outbox/operations.py:62-65`).

**Problem**: The public contract is incomplete. Anyone writing a new adapter from scratch by reading `DurableOutboxStore` won't implement `cleanup_sent`/`repair_failed_to_pending` and will silently fail to integrate with `AdminService` and any cleanup orchestrator. The `OutboxAdminActions` protocol in operations.py duck-types the missing methods.

**Impact**: P1 design integrity. New adapters will have to be discovered to be incomplete via runtime `AttributeError`.

**Recommendation**:

1. Add to `DurableOutboxStore` protocol (`core/store.py`):
```python
async def cleanup_sent(self, *, now: datetime, safety_margin: timedelta) -> int: ...
async def repair_failed_to_pending(self, *, event_id: str) -> None: ...
```

2. Consider whether `cleanup_sent` should accept a `batch_size` parameter for paginated cleanup (composes with [P-P1-4]).

3. Update `tests/provider_contract/test_fake_store_contract.py` to assert the full protocol is satisfied via `isinstance(store, DurableOutboxStore)` (Protocol structural check) — note: only meaningful with `@runtime_checkable`.

**Independence**: Independent. Touches every adapter file but is mechanical.

---

### [A-P1-4] `Capabilities.require_rpo_zero()` exists but is never called — no startup validation

**Where**: `durable_outbox/core/capabilities.py:14-19` defines `require_rpo_zero`; grep across the codebase shows no callers.

**Problem**: The proposal §31/§32 makes RPO=0 a deployment-time invariant for the RPO=0 adapters. Today, the dispatcher and replayer ingest any `DurableOutboxStore` without checking. A misconfigured `BlobOutboxStore` (single-region, `rpo_zero=False`) wired into a pipeline that *requires* RPO=0 will silently degrade durability.

**Impact**: P1. Configuration drift between intended and actual durability is undetectable at startup.

**Recommendation**:

1. Add an opt-in startup validation hook to `OutboxDispatcher`:
```python
class OutboxDispatcher:
    def __init__(self, store, sink, *, require_rpo_zero: bool = False, ...):
        if require_rpo_zero:
            store.capabilities.require_rpo_zero()
        ...
```

2. Add `FailoverReplayer.__init__(require_rpo_zero=True)` defaulting to `True` — failover replay against a non-RPO=0 store is incoherent.

3. Add a unit test asserting `OutboxDispatcher(BlobOutboxStore.for_testing(), ..., require_rpo_zero=True)` raises `ConfigurationError`.

**Independence**: Independent.

---

### [A-P2-1] `DualRegionBlobOutboxStore.records = self.primary.records` leaks internal state

**Where**: `durable_outbox/stores/blob_geo.py:524, 533, 623`.

**Problem**: The dual-region store mutates its own `records` attribute by aliasing the primary's mutable dict. This means external code reading `dual_store.records` sees only the primary's view; the secondary's records are invisible. The aliasing also bakes in single-region assumptions.

**Impact**: P2. Internal coupling and design smell. Will become a real bug when [A-P0-2] (role swap) lands.

**Recommendation**: Remove all three lines and `self.records: dict[str, StoredEvent] = {}` initialisation. If external code needs aggregated view, expose a method `async def records_view(self) -> Mapping[str, StoredEvent]` that returns a defensive copy. Even better: remove `records` from the public surface entirely — it was always an implementation detail leaking through the in-memory baseline.

**Independence**: Independent. Pairs with [A-P0-2].

---

### [A-P2-2] `_mirror_terminal_update` is best-effort with no retry, no metric, no log

**Where**: `durable_outbox/stores/blob_geo.py:631-649`.

**Problem**: After the primary records a terminal state (`SENT`/`PENDING`/`FAILED`), the secondary is updated via a single blob write. If that write fails, the secondary is silently behind the primary. There is no retry queue, no error metric, no log line, and the caller (which only ever sees primary success) receives no signal.

**Impact**: P2 for normal operation (mirror drift on transient errors), P0 if [A-P0-2] (role swap) lands without addressing this — the new active region would have stale `SENT` markers and would replay events that were already published.

**Recommendation**:

1. Wrap `_mirror_terminal_update` in a small retry (e.g. `tenacity` or hand-rolled 3 attempts with exponential backoff).
2. On exhaustion: log at WARNING, increment `outbox_mirror_failures_total{region="secondary"}`, queue the event_id for periodic reconciliation. Add `async def reconcile_mirror() -> int` that scans for divergence and re-mirrors.
3. Test: `test_mirror_failure_records_metric_and_does_not_block_primary`.

**Independence**: Should ship with [A-P0-2].

---

### [A-P2-3] Ordering coordinator does not bound stale-lease release latency

**Where**: `durable_outbox/stores/blob_geo.py:476-480` (`_release_ordering_lease`), `core/ordering.py:34-65`.

**Problem**: If a publisher crashes between `mark_sent` and `_release_ordering_lease`, the lease persists until `expires_at`. With default `ordering_lock_lease_duration = timedelta(minutes=5)`, the same key is blocked for 5 minutes after every crash. There's no mechanism to release the lease early when the holder dies — only TTL expiry.

**Impact**: P2. Ordered-key throughput cliffs after crashes. The default 5-minute window is reasonable for crashes but excessive for routine deploys.

**Recommendation**: Two complementary changes:

1. Add `async def release_by_event(self, event_id: str)` to the backend protocol — call it from the dispatcher in a `finally` clause around the publish step. The current `_release_ordering_lease` already does this via `_ordering_leases_by_event_id` mapping, but that mapping is per-process and lost on crash.

2. Make `ordering_lock_lease_duration` shorter by default (e.g. 30s) with explicit `lease_refresh_task` that renews the lease while the publish is in progress. This is the standard distributed-lease pattern.

3. Document: "ordering_lock_lease_duration should be longer than your worst-case publish latency but short enough that crash recovery isn't operationally painful".

**Independence**: Depends on [A-P0-4] (real lease backend).

---

### [A-P2-4] `failover_replay_candidates` is not idempotent if interrupted

**Where**: `durable_outbox/stores/blob_geo.py:269-309`, `stores/cosmos.py:265-306`, `stores/sql.py:296-337`, `stores/memory.py:158-193`.

**Problem**: Each adapter's `failover_replay_candidates` mutates records to `IN_FLIGHT` with a fresh `claim_token` as it builds the candidate list. If the call is interrupted (timeout, cancellation), the caller has no record of which events were claimed and there's no way to "release" the partial claim — the events sit `IN_FLIGHT` until `claim_timeout`.

**Impact**: P2. Failover throughput is bottlenecked by `claim_timeout` (5 min default) for any interrupted replay batch.

**Recommendation**:

1. Wrap the per-record claim in a try/except that releases the partially mutated state on cancellation:
```python
async def failover_replay_candidates(self, *, failover_started_at, limit):
    candidates: list[ClaimedEvent] = []
    try:
        for record in self._replay_eligible_records(failover_started_at):
            if len(candidates) >= limit:
                break
            claimed = await self._try_claim_for_replay(record)
            if claimed is not None:
                candidates.append(claimed)
    except (asyncio.CancelledError, Exception):
        await self._release_replay_claims(candidates)
        raise
    return candidates
```

2. Better: change the API to stream candidates (`AsyncIterator[ClaimedEvent]`) so the caller checkpoints each claim. Composes with [P-P1-2].

**Independence**: Composes with [P-P1-2].

---

### [A-P3-1] Dead/aspirational core types: `CleanupPolicy`, `OutboxSettings`, `Tracer`/`NoopTracer`

**Where**: `durable_outbox/core/cleanup.py` (just a dataclass, no consumer), `durable_outbox/config/settings.py` (defined but never imported), `durable_outbox/telemetry/tracing.py` (per Style report, exported but unused).

**Problem**: Three modules exist but nothing in the codebase calls them. The proposal §22 promises OpenTelemetry header propagation in the Kafka sink — not implemented.

**Impact**: P3. Misleads readers about what works. Increases maintenance surface.

**Recommendation**:

1. Either implement OTel header propagation in `sinks/kafka.py._headers` (extract span context, inject as W3C `traceparent`/`tracestate` headers) using `opentelemetry.propagators.textmap`, or remove `telemetry/tracing.py` and the `[otel]` extra until ready.

2. Either wire `CleanupPolicy` into a `CleanupRunner` that drives `cleanup_sent` on a schedule, or delete it.

3. Either give `OutboxSettings` a real role (env-var-driven configuration) or delete it.

**Independence**: Independent. Each can be a separate PR.

---

### [A-NIT-1] `AdminEventMetadata.as_pending()` is unused

**Where**: `durable_outbox/operations.py:33-34`.

**Recommendation**: Delete; `AdminService.repair_failed` doesn't use it.

**Independence**: Independent.

---

## 4. Security findings

### Threat model summary

The library sits on the producer side of an event pipeline. Relevant attackers:
- **Authenticated producers** sending malformed or oversized events (DoS, integrity).
- **Operators with admin replay access** abusing replay (integrity, audit gap).
- **Storage-tier attackers** with blob/Cosmos/SQL write access — already privileged but the library should fail safe against poisoned records.
- **Header injectors** — application code that builds `OutboxEvent.headers` from user input could leak secrets via Kafka/Blob metadata.

The library follows the right shape on the highest-risk paths (payload opacity, no logging of payload), but several enforcement holes exist.

---

### [S-P0-1] No upper bound on `claim_batch(limit=)` enables memory-exhaustion DoS

**Where**: `durable_outbox/core/validation.py:13-15` (`require_positive_limit` checks `limit < 1` only). Used by every store's `claim_batch` and `failover_replay_candidates`.

**Vulnerability**: A caller (the dispatcher loop or a misconfigured admin path) passing `limit=10_000_000` causes the in-memory store, SQL `list_records()`, Cosmos cross-partition scan, or Blob `list_blobs+get_blob` loop to materialise that many records into memory. The current in-memory and shim implementations have no guard. For Blob this is an O(limit) network amplification.

**Impact**: Availability. A privileged caller (compromised dispatcher config) can OOM the publisher and indirectly disrupt the entire pipeline. Failover replay with an unbounded `limit` would compound this during the most fragile operational moment.

**Recommendation**:

1. Add a configurable upper bound to validation:
```python
# core/validation.py
_DEFAULT_MAX_LIMIT = 10_000

def require_positive_limit(limit: int, *, field_name: str = "limit", maximum: int = _DEFAULT_MAX_LIMIT) -> None:
    if limit < 1:
        raise ValidationError(f"{field_name} must be positive")
    if limit > maximum:
        raise ValidationError(f"{field_name} exceeds {maximum}")
```

2. Threading the maximum through each store's `claim_batch` is overkill; the validator can read it from a module-level constant tunable via env var `DURABLE_OUTBOX_MAX_CLAIM_LIMIT`.

3. Document the cap in `docs/operations.md`.

**Independence**: Independent.

---

### [S-P1-1] `KafkaSink` does not enforce TLS / `SASL_SSL` in certified mode

**Where**: `durable_outbox/sinks/kafka.py:55-72` (`KafkaProducerConfig.validated()` validates `acks` and `enable.idempotence` only).

**Vulnerability**: Proposal §17 specifies `security.protocol = SASL_SSL` as part of the baseline. The library accepts `security.protocol = PLAINTEXT` without warning. Credentials and event payloads then traverse the network unencrypted.

**Impact**: Confidentiality + integrity of all events in transit. Realistic in misconfigured staging environments leaking to production.

**Recommendation**: In `KafkaProducerConfig.validated()` when `certified_mode=True`:
```python
allowed_protocols = {"SASL_SSL", "SSL"}
protocol = str(config.get("security.protocol", "")).upper()
if protocol not in allowed_protocols:
    raise ConfigurationError(
        f"certified Kafka sink requires security.protocol in {sorted(allowed_protocols)}; got {protocol or 'unset'}"
    )
```
Test: `test_kafka_config_rejects_plaintext_in_certified_mode`.

**Independence**: Independent.

---

### [S-P1-2] Headers are propagated to Kafka unchecked — secret leakage if caller header hygiene fails

**Where**: `durable_outbox/sinks/kafka.py:165-168` (every header on `OutboxEvent.headers` is forwarded to Kafka with the addition of `event_id`).

**Vulnerability**: Per proposal §23: "Metadata: Do not place secrets in metadata, tags, or headers." This is a caller obligation, but the library has no defence-in-depth check. An application that accidentally puts an `Authorization` header on an `OutboxEvent` will leak it to every Kafka consumer.

**Impact**: Confidentiality. Realistic given that web frameworks often pass through inbound headers wholesale.

**Recommendation**:

1. Add a default deny-list of header name prefixes in `OutboxEvent.__post_init__` (configurable via class-level constant):
```python
# core/model.py
_BLOCKED_HEADER_PREFIXES = ("authorization", "cookie", "set-cookie", "proxy-authorization", "x-api-key", "x-auth-")

def _freeze_headers(headers):
    ...
    for name, value in headers.items():
        lname = name.lower()
        if any(lname.startswith(p) for p in _BLOCKED_HEADER_PREFIXES):
            raise ValidationError(f"header name {name!r} is in the blocked-prefix list")
        ...
```

2. Make the list overridable via class-level configuration for callers who genuinely need (e.g. signed `X-Auth-Signature`) — but the default must protect the common case.

3. Test: `test_outbox_event_rejects_authorization_header`.

**Independence**: Independent.

---

### [S-P1-3] `JsonlAuditSink` does sync I/O on the asyncio loop

**Where**: `durable_outbox/operations.py:83-91`.

**Vulnerability**: Not a classical security bug — but `os.fsync` blocks the loop for 1-50 ms per audit record, starving every other coroutine. An admin endpoint that audits 100 actions in a burst freezes the publisher for several seconds. Combined with [P-P1-2] (Kafka loop blocking), audit + publish can interleave catastrophically.

**Impact**: Availability of the publisher process.

**Recommendation**: Wrap the file open/write/fsync chain in `await asyncio.to_thread(...)`:
```python
async def record(self, record):
    line = json.dumps(record.to_json_dict(), sort_keys=True, separators=(",", ":"))
    async with self._lock:
        await asyncio.to_thread(self._write_line_sync, line)

def _write_line_sync(self, line):
    self.path.parent.mkdir(parents=True, exist_ok=True)
    with self.path.open("a", encoding="utf-8") as f:
        f.write(f"{line}\n")
        f.flush()
        if self.fsync:
            os.fsync(f.fileno())
```

**Independence**: Independent.

---

### [S-P2-1] No header count / value size bound

**Where**: `durable_outbox/core/model.py:61-69` (`_freeze_headers` validates type only).

**Vulnerability**: A producer can submit an `OutboxEvent` with 10,000 headers or a single header value of 100 MB. The store will accept it (no `max_headers_bytes` capability), Blob/Cosmos will reject it at the metadata size limit (each has its own — Blob 8 KB per metadata header, Cosmos 8 KB per property), but the failure will be a `RetryableStoreError` from the adapter rather than a `ValidationError` at the boundary — meaning the caller will keep retrying.

**Impact**: Availability (retry storm) + confidentiality (oversized log lines from error messages).

**Recommendation**: Add to validation:
```python
# core/validation.py
_MAX_HEADER_COUNT = 64
_MAX_HEADER_VALUE_BYTES = 4096
_MAX_HEADER_NAME_BYTES = 256

def enforce_header_limits(headers):
    if len(headers) > _MAX_HEADER_COUNT:
        raise ValidationError(f"header count {len(headers)} exceeds {_MAX_HEADER_COUNT}")
    for name, value in headers.items():
        if len(name) > _MAX_HEADER_NAME_BYTES:
            raise ValidationError(f"header name length {len(name)} exceeds {_MAX_HEADER_NAME_BYTES}")
        if len(value) > _MAX_HEADER_VALUE_BYTES:
            raise ValidationError(f"header value length {len(value)} exceeds {_MAX_HEADER_VALUE_BYTES}")
```
Call from `OutboxEvent.__post_init__` after `_freeze_headers`.

**Independence**: Independent.

---

### [S-P2-2] `_decode_event` accepts blob content of unbounded size without an upper read cap

**Where**: `durable_outbox/stores/blob_geo.py:733-790`, `azure_blob.py:49-64` (`get_blob` calls `download.readall()`).

**Vulnerability**: A blob writer (privileged but compromisable) could upload a 10 GB record. `readall()` loads it into memory; `json.loads` on the decoded UTF-8 will allocate proportionally.

**Impact**: Availability of dispatcher. Defence-in-depth — already requires storage write access.

**Recommendation**: In `AzureBlobClient.get_blob`, check `properties.size` before `download.readall()` and reject above a threshold:
```python
if properties.size > _MAX_BLOB_BYTES:
    raise RetryableStoreError(f"blob {name!r} size {properties.size} exceeds {_MAX_BLOB_BYTES}")
```
Default 10 MB (well above realistic outbox records — payloads sit inside the JSON).

**Independence**: Independent.

---

### [S-P2-3] `event_fingerprint` is written into blob metadata — hash leakage is acceptable but bears documenting

**Where**: `durable_outbox/stores/blob_geo.py:669-674` (`_record_metadata` adds `event_fingerprint`), `_event_fingerprint` line 817-823 (sha256 of full event including payload).

**Vulnerability**: The metadata `event_fingerprint` is sha256 of the full event (including payload). Anyone with read access to the blob container can:
1. Read the fingerprint without reading the (potentially encrypted-at-rest) payload.
2. Test arbitrary candidate events against the fingerprint to confirm payload content (rainbow-table style, limited utility for high-entropy payloads).

This is consistent with the proposal's general "Treat payload as opaque" stance — sha256 is one-way — but is worth documenting.

**Impact**: Confidentiality, marginal. Realistic only for low-entropy payloads (e.g., known message templates with small variable parts).

**Recommendation**:

1. Document the fingerprint and its purpose in `docs/providers.md`.
2. For sensitive deployments, swap to a keyed MAC (HMAC-SHA256 with a deployment-secret) so external candidate-guessing is infeasible. Add `BlobOutboxStore(fingerprint_key: bytes | None = None)` — when set, use HMAC; when unset, retain current sha256.

**Independence**: Independent.

---

### [S-P3-1] `_decode_record` accepts arbitrary types in JSON fields without strict validation

**Where**: `durable_outbox/stores/blob_geo.py:733-751` (`data["claim_token"]` is typed as `Any`, passed to `StoredEvent.claim_token: str | None`).

**Vulnerability**: A corrupted/poisoned blob with `"claim_token": [1,2,3]` would yield a list-typed claim_token; downstream string operations would raise `TypeError`. Not an exploitable injection, but the dispatcher crashes mid-claim and the event is stuck `IN_FLIGHT`.

**Impact**: Availability/integrity defence-in-depth.

**Recommendation**: Validate each field shape explicitly in `_decode_record` (e.g. `claim_token = data["claim_token"]; if claim_token is not None and not isinstance(claim_token, str): raise RetryableStoreError(...)`). Or use `pydantic` only inside `_decode_record` if the dependency cost is acceptable.

**Independence**: Independent.

---

### [S-P3-2] `_classify_error` substring-matches error messages

**Where**: `durable_outbox/sinks/kafka.py:180-201`.

**Vulnerability**: `_is_non_retryable_error` falls back to substring matches like `"authorization" in normalized`. A Kafka error message containing the word "authorization" in a transient context (e.g., a future broker change) would be misclassified as terminal.

**Impact**: Integrity — wrongly `FAILED` events that should have been retried. Inverse risk: misclassifying terminal as retryable causes infinite loops.

**Recommendation**: Drop the substring fallback. Rely on `_NON_RETRYABLE_ERROR_NAMES` and `error.retriable()` (which confluent-kafka's `KafkaError` correctly implements). Document that unrecognised error types default to retryable, as is the existing behaviour.

**Independence**: Independent.

---

### [S-P3-3] No bound on `_event_fingerprint` payload re-serialisation cost

**Where**: `durable_outbox/stores/blob_geo.py:817-823` — for a maliciously large payload, fingerprinting allocates 1.33× the payload (base64 expansion). Repeated `put()` retries against the same large event would burn CPU.

**Impact**: Availability under adversarial producers.

**Recommendation**: Composes with [S-P2-1] header bound and [S-P2-2] blob size cap — once size limits exist, fingerprinting is bounded.

**Independence**: Composes with [S-P2-1], [S-P2-2].

---

## 5. Performance findings

> The Performance subagent completed successfully. The full report is preserved below verbatim with finding IDs added so cross-references resolve.

### Performance posture (subagent verbatim)

The library is in a **proof-of-correctness, not proof-of-performance** state. Every store adapter (memory, Cosmos, SQL, blob_geo) routes claim/replay/cleanup through a full `list_records()` / `_refresh_records()` scan and then a Python-side sort of every record in the store on every dispatcher tick. The SQL adapter ships a schema with the correct indexes but **does not use them** — there is no `claim_batch` SQL query, no `WITH (READPAST, UPDLOCK, ROWLOCK)` CTE, no per-row update path. The dual-region Blob adapter serialises four sequential network round trips per `put()` (≈4×p99 blob latency on the hot path) and also performs a full container `list_blobs` on every claim. The dispatcher publishes events one-at-a-time within a batch with no `asyncio.gather`, so per-worker throughput is bounded by `1 / (claim_scan + N × (publish_latency + mark_sent_latency))`. **At 1000 msg/min/topic in a single topic the in-memory backed simulation passes (test_failure_load.py line 37-47), but the real adapters will struggle**: Cosmos at strong consistency does cross-partition list-then-replace per claim (high RU), SQL has no actual claim SQL, and dual-region Blob acceptance latency is bounded below by ~200-400 ms (4×~50-100 ms p50). Failover replay materialises all candidates in memory and is dispatched serially. Workloads with 10k+ TTL-valid events, multiple ordering keys, or any meaningful event volume per topic per minute will be backpressured by claim-batch scans long before storage caps engage.

### [P-P0-1] Dispatcher publishes events strictly sequentially with no per-batch concurrency

**Where**: `durable_outbox/core/dispatcher.py:36-96`.

**Issue**: `run_once` iterates `claimed_events` and `await`s `sink.publish` then `store.mark_sent` for each event one at a time. For unordered events this throws away all available concurrency; for a Kafka sink with default `linger.ms=5` (`sinks/kafka.py:62`) every event waits for a delivery callback before the next `produce()` is called. Per-event latency is the floor for throughput.

**Impact**: Per-worker throughput ceiling = `1 / (sink.publish + mark_sent)`. With Kafka delivery callback round-trip ~5-20 ms + Cosmos/Blob `mark_sent` ~20-100 ms = 30-120 ms per event = **8-33 msg/sec per worker = 480-2000 msg/min per worker**. A single topic at 1000 msg/min on the SQL adapter (50 ms commit + 5 ms publish) hits ~1090 msg/min — at the edge. With dual-region Blob (`mark_sent` mirrors to secondary on every event, see finding below) you get ~5-8 msg/sec per worker. Horizontally scaling helps but only by adding workers, each claiming serially.

**Recommendation**: Split the batch into ordered and unordered groups (`one_per_ordering_key` already exists in `core/ordering.py:74`). For unordered events run publish concurrently with `asyncio.gather(*[_publish_one(c) for c in unordered_claims], return_exceptions=True)`. Add a `concurrency: int = 16` parameter to `OutboxDispatcher.__init__` and use `asyncio.Semaphore` to bound. Define `_publish_one(claimed)` containing the existing try/except/mark_sent block. Add a benchmark `tests/perf/test_dispatcher_throughput.py` measuring sustained `run_once` over 10k pre-staged events with `FakeSink` that sleeps 20 ms.

**Independence**: Independent. Unlocks the rest of the throughput story.

---

### [P-P0-2] SQL adapter has no real SQL — uses in-memory dict and table scan; proposed claim CTE not implemented

**Where**: `durable_outbox/stores/sql.py:110-158` (`InMemorySqlOutboxClient`), `sql.py:196-252` (`claim_batch` via `await self.client.list_records()`).

**Issue**: The `SqlOutboxClient` protocol only exposes `get`, `upsert_new`, `replace`, `list_records`, `delete`. Every `claim_batch` calls `list_records()` returning every row, sorts in Python, then issues a `replace()` per claim. The `SQL_SCHEMA` constant defines `IX_outbox_pending`, `IX_outbox_replay`, `IX_outbox_ordered` (lines 56-63) and proposal §15.3 specifies a `READPAST, UPDLOCK, ROWLOCK` CTE that atomically claims and returns rows in one round trip — none of this is wired up. No real backend exists, only the in-memory shim used by tests.

**Impact**: For N pending rows, claim is **O(N) network bytes + N+1 round trips per batch** instead of `O(batch_size)` with 1 round trip. At 10k pending rows a single tick transfers entire table. Indexes are dead code. Cross-publisher claim conflicts will be O(claim attempts) instead of the SQL Server `READPAST` skip-locked-rows path. Renders the SQL adapter unusable for the stated throughput target.

**Recommendation**: Introduce two new client methods on `SqlOutboxClient`: `async def claim_batch_pending(*, limit: int, claim_token: str, now: datetime) -> Sequence[SqlStoredEvent]` executing the CTE from proposal §15.3 (atomic UPDATE … OUTPUT inserted.*), and `async def list_replay_candidates(*, failover_started_at, limit)` using `IX_outbox_replay`. Refactor `_SqlOutboxStoreBase.claim_batch` to delegate to `claim_batch_pending` instead of the list+sort+per-row path. Keep `InMemorySqlOutboxClient` only as a test double and have it implement the new methods over its dict. Add a smoke perf test `tests/perf/test_sql_claim_batch_roundtrips.py` asserting one client call per claim.

**Independence**: Independent of P-P0-1 dispatcher fix; both are required. Composes with [A-P0-1].

---

### [P-P0-3] Dual-region blob `put()` serialises 4 sequential network round trips

**Where**: `durable_outbox/stores/blob_geo.py:528-540`.

**Issue**:
```python
await self._prepare(self.primary, event)
await self._prepare(self.secondary, event)
await self._accept(self.primary, event)
await self._accept(self.secondary, event)
```
Each call is a Blob HTTP RPC. Step 1 must precede step 3 in the same region (PREPARED → accepted), and step 2 must precede step 4. But step 1 and step 2 are independent (different regions), as are 3 and 4. The proposal's correctness model only requires the per-region ordering, not the cross-region serialisation.

**Impact**: Acceptance latency = `4 * blob_p50` ≈ 200-400 ms (cross-region) vs achievable `2 * blob_p50` ≈ 100-200 ms with parallel pairs. Doubles `put()` latency on the producer path and halves producer throughput per coroutine.

**Recommendation**: Replace with:
```python
await asyncio.gather(self._prepare(self.primary, event), self._prepare(self.secondary, event))
await asyncio.gather(self._accept(self.primary, event), self._accept(self.secondary, event))
```
`_mirror_terminal_update` (line 631-649) and `cleanup_sent` (line 605-606) can also gather. Add benchmark `tests/perf/test_blob_geo_put_latency.py` measuring `put()` latency against an `InMemoryBlobClient` patched with `asyncio.sleep(50e-3)`.

**Independence**: Independent.

---

### [P-P0-4] Blob `claim_batch` calls `list_blobs` + per-item `get_blob` over the entire events prefix every tick

**Where**: `durable_outbox/stores/blob_geo.py:180-222` (`claim_batch` → line 182 `await self._refresh_records()`), `blob_geo.py:378-384` (`_refresh_records`), and `azure_blob.py:127-134` (`AzureBlobClient.list_blobs`).

**Issue**: `_refresh_records` calls `self.client.list_blobs(prefix="outbox/v1/events/")` which on `AzureBlobClient` enumerates **and re-downloads** every blob via `get_blob(name)` (line 131-132) for each blob returned by `list_blobs`. So one claim tick = `O(total_events_in_container)` HTTP GETs, not just listings. SENT events stay until `cleanup_sent` runs; pending+sent+in_flight all get re-downloaded.

**Impact**: For 10k retained events: 10k+1 HTTP round trips per dispatcher tick, ~$10s of latency. Even at 1k retained events: ~10-20 s per tick of throughput-killing I/O. Hard ceiling well below 1000 msg/min sustained.

**Recommendation**: Three changes. (1) In `AzureBlobClient.list_blobs` add a `with_content: bool = True` parameter and have callers that only need names/metadata pass `False`; populate `BlobObject.content=b""` and require explicit `get_blob` only for records the claim path will actually mutate. (2) In `BlobOutboxStore.claim_batch` page through `list_blobs(prefix="outbox/v1/events/", with_content=False)`, filter by `metadata["status"]=="PENDING"` and `metadata["accepted"]=="true"`, sort, then `get_blob` only for the top `limit`. (3) Cache `_record_etags` and pull a metadata-only listing for in-flight key detection (`_in_flight_ordering_keys`). Tag-based index per proposal §15.1 may also be used as a hint (but not a sole source). Add `tests/perf/test_blob_claim_batch_scan.py` asserting `claim_batch(limit=100)` issues at most `100 + O(1)` round trips at 10k events.

**Independence**: Independent.

---

### [P-P0-5] Cosmos `claim_batch` does full `list_records()` cross-partition scan per tick

**Where**: `durable_outbox/stores/cosmos.py:176-180`, `cosmos.py:349-358` (`_claim_ordered_records`), `cosmos.py:367-381` (`_in_flight_ordering_keys`).

**Issue**: The `CosmosOutboxClient` protocol has `list_records()` returning **every** stored record. Each claim tick therefore performs a cross-partition full container scan, sorts in Python, and applies a per-row `replace()` for claims. The partition key strategy (`partition_key_for`, line 337-341) is correctly defined (`topic#bucket` unordered, `topic#hash(ordering_key)` ordered) but `claim_batch` never uses it as a query predicate. `_in_flight_ordering_keys` also scans the entire collection.

**Impact**: Cross-partition queries are the worst Cosmos RU cost pattern. At 10k stored items with strong consistency this is tens of thousands of RUs per tick. The proposal §15.2 claim path is single-document read-and-conditional-update — not implemented. Each claim tick therefore burns ~`list_RUs + 2 * limit * point_RUs` instead of `~limit * point_RUs`. Will hit RU 429s long before 1000 msg/min.

**Recommendation**: Extend `CosmosOutboxClient` protocol with `async def list_claimable_within_partition(pk: str, *, limit: int, now: datetime) -> Sequence[CosmosStoredEvent]` and call it once per partition. For unordered mode iterate partitions `topic#0..N-1` (use `config.unordered_buckets`). The real client should issue `SELECT TOP @limit ... WHERE c.pk=@pk AND c.status='PENDING' AND (c.next_attempt_at IS NULL OR c.next_attempt_at<=@now)` with the partition key set in the request options. Replace `_in_flight_ordering_keys` lookup with a per-partition query bounded by partition. Independent benchmark `tests/perf/test_cosmos_claim_partition_scoped.py` should verify zero cross-partition queries.

**Independence**: Independent. Composes with [A-P0-1].

---

### [P-P1-1] Failover replay materialises full candidate set in memory and dispatches serially

**Where**: `durable_outbox/core/failover.py:18-31`, mirrored in `stores/blob_geo.py:269-309`, `cosmos.py:265-306`, `sql.py:296-337`, `memory.py:158-193`.

**Issue**: `failover_replay_candidates` returns a `list[ClaimedEvent]` (entire set up to `limit`), then `FailoverReplayer.replay_once` loops `for claimed in candidates: await self.sink.publish(...); await self.store.mark_sent(...)` — strictly sequential. Each adapter also does a full table scan + Python sort to build the list. At 10k+ TTL-valid events this is both a memory spike (decoded events held in a list) and a serial publish loop.

**Impact**: 10k events × 20 ms publish + mark_sent each = 200 seconds of serial replay. Memory grows linearly with `limit` plus the in-memory `records` dict each store keeps. The proposal frames replay as a recovery-time-critical phase, so this directly impacts RTO.

**Recommendation**: Change the contract on `DurableOutboxStore.failover_replay_candidates` to either accept a callback or return an `AsyncIterator[ClaimedEvent]` pageable by `claim_token` or `(created_at, event_id)` cursor. Update `FailoverReplayer.replay_once` to consume the iterator and dispatch through the same concurrent publish loop suggested for the dispatcher (`asyncio.gather` with a semaphore). Adapters then page from storage instead of materialising. Add `tests/perf/test_failover_replay_streaming.py` against a 10k event fixture.

**Independence**: Builds on the same `_publish_one` helper introduced by [P-P0-1] dispatcher. Other concrete adapter pagination work is independent. Composes with [A-P2-4].

---

### [P-P1-2] Kafka sink calls `producer.poll()` on the asyncio loop thread — blocks the loop

**Where**: `durable_outbox/sinks/kafka.py:142-159`.

**Issue**: `confluent_kafka.Producer.produce/poll/flush` are synchronous C calls. `publish()` calls `self.producer.poll(self.poll_interval_seconds)` (50 ms default) inside the asyncio loop. Even though delivery callbacks use `loop.call_soon_threadsafe`, the `poll()` itself is blocking and runs on the asyncio thread, freezing every other coroutine for up to 50 ms each iteration. The dispatcher serial loop ensures every other publish is held up. Multiple concurrent `publish()` calls each call `poll()` racing on the same Producer.

**Impact**: Loop stalls of 50 ms per poll × N concurrent publishes degrade dispatcher throughput and starve metrics/HTTP coroutines. With per-publish loops blocking each other, parallelisation of publish ([P-P0-1] dispatcher fix) won't actually parallelise.

**Recommendation**: Two changes. (1) Run `produce()` and `poll()` via `await asyncio.to_thread(self.producer.produce, ...)` and a single dedicated polling thread/task. Spawn one background task per `KafkaSink` instance in `__init__` that loops `await asyncio.to_thread(self.producer.poll, 0.5)` and exits on `close()`. Then `publish()` only needs `produce()` + `await future`. (2) `flush()` in `close()` should also `await asyncio.to_thread`. Add `tests/perf/test_kafka_sink_loop_yield.py` measuring loop event scheduling latency under sustained publish.

**Independence**: Independent. Required for [P-P0-1] dispatcher parallelism to actually parallelise.

---

### [P-P1-3] Per-event `_event_fingerprint` re-serialises the event on every duplicate check

**Where**: `durable_outbox/stores/blob_geo.py:418-424` (`_ensure_compatible_duplicate`) and `_event_fingerprint` line 817-823.

**Issue**: Every `put()`, `_put_prepared`, and `_accept_prepared` against an existing record calls `_event_fingerprint` on both the stored and incoming event. Each fingerprint = full `_encode_event` (JSON+base64 of payload+headers) + sha256 of the result. Payload bytes get base64-encoded twice (once for each event) per call just to compare.

**Impact**: For a 100 KB event, ~200 KB of base64 + JSON allocation + sha256 per duplicate-check call. In dual-region put `_ensure_compatible_duplicate` runs up to 4 times (per region per phase). At 1000 msg/min: ~7 MB/sec of throwaway allocation just for fingerprinting on the put path during retries. Add the metadata stored in `_record_metadata` (line 669-674) which also calls `_event_fingerprint`, so saving records re-fingerprints every time.

**Recommendation**: Cache the fingerprint on `OutboxEvent` (compute lazily via `functools.cached_property` requires `frozen=False`; use a module-level `WeakKeyDictionary[OutboxEvent, str]` or store on `StoredEvent`). Better: compare fields directly via `event_a == event_b` plus a fast `payload`/`headers` equality check (mirror `_compatible_event` in `memory.py:255-266`). Only fingerprint for cross-process comparison via metadata at write time, not on every save. Drop fingerprint from `_record_metadata` or compute once on construction.

**Independence**: Independent.

---

### [P-P1-4] `_in_flight_ordering_keys` rescans full record set every claim

**Where**: `durable_outbox/stores/blob_geo.py:446-460`, `cosmos.py:367-381`, `sql.py:384-398`, `memory.py:238-252`.

**Issue**: Every claim tick iterates the entire `records` map (or full `list_records()` result) just to build the in-flight ordering-key set. This is O(total events) per tick — dominated by SENT events that never become claim candidates again.

**Impact**: At 10k retained events with 100 actual ordering keys in flight, you do 10k iterations to find 100. Combined with the `_ordered_records` sort (`blob_geo.py:426-435`), each claim tick is O(N log N) Python-side. With 10k events: ~tens of ms of pure CPU per tick before any I/O.

**Recommendation**: Maintain a per-store `_in_flight_by_ordering_key: dict[str, datetime]` (claim time) updated in `_save_record`/`mark_sent`/`mark_pending_after_retryable_failure`/`mark_failed`. Lookup becomes O(1) per claim candidate. Prune entries older than `claim_timeout`. Same approach in Cosmos, SQL, memory adapters.

**Independence**: Independent. Compounds with [P-P0-5] cosmos/SQL fixes — without those, this still scans cross-partition.

---

### [P-P1-5] `CollectingMetricsAdapter` is not thread-safe and rebuilds keys on every call

**Where**: `durable_outbox/operations.py:105-115`, `core/dispatcher.py:42, 51, 68, 78, 85`.

**Issue**: `increment(name, **labels)` constructs a fresh `tuple(sorted(labels.items()))` per call. In the dispatcher hot path that is 1-3 metric increments per event. `InMemoryMetrics.increment` (`telemetry/metrics.py:24-25`) does the same. `to_prometheus_text()` (operations.py:129-138) re-sorts everything on each scrape; per-sample membership check uses `set` (`emitted_types`) but still touches every sample.

**Impact**: Per-event allocations: 2-3 × (dict→tuple sort + tuple hash + Counter/dict update). At 1000 msg/min single topic = ~50 short-lived tuples/sec, negligible alone; but multiplied across replays and multi-topic deployments and with no lock around `_counters`, concurrent increments race. `NoopMetrics` is fine; `CollectingMetricsAdapter` is racey under the parallelism unlocked by [P-P0-1] dispatcher fix.

**Recommendation**: (1) Make `CollectingMetricsAdapter._counters` use `collections.Counter` and wrap mutations in an `asyncio.Lock` or use `threading.Lock` for thread-safety across `to_thread` callers. (2) Cache the sorted-tuple key per call site: change dispatcher to use pre-built `LabelSet` objects, e.g. an `OutboxDispatcher`-owned `dict[str, LabelKey]` for `("outbox_publish_attempts_total", topic)`. (3) `to_prometheus_text` should track `emitted_types` while iterating once instead of inside the loop on `sample.name` (already does this — keep).

**Independence**: Independent. Becomes important once dispatcher parallelism ([P-P0-1]) lands.

---

### [P-P1-6] `cleanup_sent` and `failover_replay_candidates` rescan all records; cleanup has no pagination

**Where**: `durable_outbox/stores/blob_geo.py:319-338`, `cosmos.py:316-327`, `sql.py:347-358`, `memory.py:203-214`, plus `core/cleanup.py` only defines a policy.

**Issue**: Each adapter's `cleanup_sent` walks every record. The blob version does a synchronous-style `await delete_blob` per matched record. No batching, no pagination, no cursor. Cosmos and SQL versions list-then-delete one-by-one with N round trips. The `CleanupPolicy` dataclass is just config — there is no actual cleanup loop driver; callers must wire this themselves.

**Impact**: A 10k-record cleanup tick is 10k+1 round trips on the cleanup path. Cleanup competing with claim for the same blob container's bandwidth will stall both.

**Recommendation**: (1) Add `batch_size: int` and `max_per_tick: int` parameters to `cleanup_sent`. (2) Implement adapter-specific batch delete (Cosmos: bulk delete by partition; SQL: `DELETE TOP (N) FROM ...`; Blob: parallel deletes via `asyncio.gather` with semaphore). (3) Provide a `CleanupScheduler` in `core/cleanup.py` that runs `cleanup_sent` on a cadence with sane defaults. (4) Add benchmark.

**Independence**: Independent. Composes with [A-P3-1] (wire CleanupPolicy into a runner).

---

### [P-P2-1] `FileSink` serialises all publishes through a single `asyncio.Lock` + `fsync` per event

**Where**: `durable_outbox/sinks/file.py:24-49`.

**Issue**: One `asyncio.Lock`, one `os.fsync` per event, file reopened (`self.path.open("ab")`) per publish. Mirrors `JsonlAuditSink` (`operations.py:77-91`).

**Impact**: `fsync` is ~1-10 ms on local SSD, ~50 ms on cloud disk. Cap throughput at 100-1000 events/sec single threaded with no concurrency. File-reopen overhead (~100 μs each) is dwarfed by fsync but still wasted.

**Recommendation**: Keep the file handle open across calls (open on first use, close on `aclose()`). Add a `fsync_interval_events: int = 1` and `fsync_interval_ms: int | None = None` for batched durability. For test usage default `fsync=False`. Same for `JsonlAuditSink`.

**Independence**: Independent. Only relevant if FileSink/JsonlAuditSink are used outside tests.

---

### [P-P2-2] `OutboxEvent.__post_init__` and `PublishResult.__post_init__` allocate `MappingProxyType(dict(...))` on every construction

**Where**: `durable_outbox/core/model.py:37-69`, `model.py:99-100`.

**Issue**: `_freeze_headers` builds a new `dict` then wraps in `MappingProxyType` on every `OutboxEvent` construction — including events decoded from store rounds, replay candidates, and clones in tests. Same for `PublishResult.__post_init__`. `dataclass(frozen=True)` with `slots` already prevents mutation; the proxy is belt-and-braces.

**Impact**: 2-3 transient allocations per event construction. At 1000 msg/min/topic with multi-topic deployments, claim+replay re-decoding events repeatedly: ~minor GC pressure but compounds. Re-decoding events from blob (`_decode_event`) constructs all-new dicts for headers and for `MappingProxyType` wrap.

**Recommendation**: Skip the proxy when the headers mapping is already an immutable mapping (`isinstance(self.headers, MappingProxyType)`). For `PublishResult.metadata` do the same. Avoid re-validation/re-wrapping on `_clone_record`-style paths by introducing an internal `OutboxEvent._unvalidated(cls, ...)` constructor for trusted decoding paths.

**Independence**: Independent. Small win.

---

### [P-P2-3] `BlobOutboxStore.claim_batch` speculatively clones every candidate before CAS

**Where**: `durable_outbox/stores/blob_geo.py:198-210`.

**Issue**: For every candidate record, `_clone_record(record)` is called speculatively before any state mutation; on `BlobPreconditionFailedError` the clone is reinstated. At high concurrency between publishers this clones every candidate even when no conflict occurs.

**Impact**: One full `StoredEvent` deep-clone per claim candidate, including the `OutboxEvent` reference (cheap, shared) and all 12 fields. Per claim tick at limit=100: 100 throwaway dataclass copies.

**Recommendation**: Stash only the 5 fields that get mutated (`status`, `claim_token`, `claimed_at`, `attempt_count`, plus `_record_etags[event_id]`) into a tuple, restore on conflict. Or: do not mutate in place — build a candidate dict, attempt `_save_record`, only mutate `self.records[event_id]` on success.

**Independence**: Independent.

---

### [P-P2-4] No jitter in `RetryPolicy` — synchronised thundering herd after Kafka outage

**Where**: `durable_outbox/core/retry.py:5-15`.

**Issue**: `next_attempt_at = now + base * multiplier^(attempt-1)` (capped). All publishers that hit the same Kafka outage compute the same delay because all start from `now` ≈ outage time. After a 5-minute Kafka brownout, all retries fire at the same instant on recovery.

**Impact**: Thundering herd amplifies recovery-time load on Kafka and on the outbox store (claim race storms). Especially bad with horizontal scaling of publishers.

**Recommendation**: Add `jitter: float = 0.1` and `random: Random | None = None`. Apply `delay *= 1 + random.uniform(-jitter, jitter)` (or full-jitter: `uniform(0, capped)`). Inject `Random` for test determinism. Add unit test that two retry policies with the same seed produce equal results, two with different seeds produce different results.

**Independence**: Independent.

---

### [P-P2-5] `_refresh_records` never evicts entries — unbounded memory growth across long-lived stores

**Where**: `durable_outbox/stores/blob_geo.py:378-384`.

**Issue**: `_refresh_records` overwrites entries from the listing but never removes records that were deleted in storage (e.g., by `cleanup_sent` running on another publisher). `self.records` and `self._record_etags` grow until the process restarts.

**Impact**: Slow leak proportional to events deleted out-of-band on other publishers. Multiplies the cost of `_ordered_records` sort and `_in_flight_ordering_keys` scan over time.

**Recommendation**: In `_refresh_records`, track `seen_ids` from the listing, then `self.records = {k: v for k, v in self.records.items() if k in seen_ids}` (same for `_record_etags`). Or: take a set difference and `pop` missing ids.

**Independence**: Independent. Becomes a P1 if `_refresh_records` scan cost ([P-P0-4]) is fixed but residual dict growth survives.

---

### [P-P3-1] `_encode_record`/`_decode_record` re-`json.dumps` + base64 the entire payload on every save

**Where**: `durable_outbox/stores/blob_geo.py:711-751`.

**Issue**: Every blob `_save_record` re-serialises the full event including base64-encoded payload, even though only state fields (`status`, `claim_token`, etc.) changed. For a 256 KB payload this is ~350 KB of upload per state transition.

**Impact**: With ~3-5 state transitions per event lifecycle (claim → sent + occasional retry), payload bytes are uploaded 3-5× per event. At 1000 msg/min × 256 KB × 4 saves = ~1.7 GB/min upload bandwidth just on state churn.

**Recommendation**: Split storage into two blobs per event: `outbox/v1/payloads/{hash}.bin` (immutable, written once) and `outbox/v1/state/{hash}.json` (mutated frequently, small). `_save_record` only writes the state blob. Trade-off: extra read on first claim, but the load pattern (write-many state, read-once payload) makes this a large net win.

**Independence**: Independent. Requires a one-time blob layout migration.

---

### [P-P3-2] `_ordered_records` re-sorts the entire record map on every claim, even for unordered events

**Where**: `durable_outbox/stores/blob_geo.py:426-435`, `cosmos.py:349-358`, `sql.py:202-211`, `memory.py:86-94`.

**Issue**: Each claim sorts all records by `(topic, ordering_key, ordering_sequence, created_at)` even when 100% of records are unordered. Sort cost is `O(N log N)` per tick.

**Impact**: ~25 μs/record sort cost in Python. At 10k records: ~250 ms pure CPU per tick. Compounds with [P-P1-4] in-flight key scan.

**Recommendation**: Maintain two indices: a `SortedDict` (e.g. via `sortedcontainers`) keyed by the ordering tuple, updated incrementally on put/save/delete. Or partition records into per-topic deques sorted by `created_at` for unordered claim. Likely only worth it after [P-P0-2] SQL and [P-P0-5] Cosmos fixes — for SQL/Cosmos the right answer is to push the sort into the storage layer.

**Independence**: Depends on [P-P0-2], [P-P0-5]. For Blob, depends on [P-P0-4] to reduce the working set.

---

### [P-NIT-1] `OutboxEvent.__post_init__` calls `_freeze_headers` even when constructed from already-frozen `MappingProxyType`

**Where**: `durable_outbox/core/model.py:52, 61-69`.

**Recommendation**: Skip validation when `headers` is a `MappingProxyType` over an already-validated dict (use a marker class or a `_trusted` private constructor). Marginal — only matters if header counts grow large. Aligns with [P-P2-2].

**Independence**: Independent.

---

## 6. Code quality / style / linting / typing findings

> The Quality subagent completed successfully. The full report is preserved below verbatim with finding IDs added so cross-references resolve.

### Quality posture (subagent verbatim)

For an Alpha (0.1.0) library aimed at being reusable across providers, the codebase is unusually disciplined: strict typing (`py.typed` is shipped, ty is wired up, ruff selects ANN/B/RUF/UP), zero runtime deps, optional extras for the heavy adapter SDKs, a `provider_contract` harness, and a clear public API surface in `core/__init__.py`. The biggest gaps are at the seams between the package and *users*: the top-level `durable_outbox/__init__.py` exposes nothing, the optional-dep gating for `azure.storage.blob`/`confluent_kafka` is partially missing (one constructor will raise a generic `ModuleNotFoundError` rather than a friendly `ConfigurationError`), public docstrings are almost entirely absent on protocols and dataclasses, and several lint rule classes (`SIM`, `PERF`, `PT`, `S`, `TCH`, `D`) that would catch real ergonomic issues cheaply are not enabled. Test coverage is strong for memory/blob/cosmos/sql in-memory fakes but the `provider_contract` is a single 23-line happy-path function — the documented behavioral matrix in `docs/durable-outbox-rpo0-proposal.md` §25 is not actually exercised as a parametrised contract across adapters.

### [Q-P0-1] `azure-storage-blob` import in `AzureBlobClient.from_connection_string` is not gated

**Where**: `durable_outbox/stores/azure_blob.py:24-29` and the bare `import_module("azure.core")` at `:138`.

**Problem**: `AzureBlobClient.from_connection_string` calls `import_module("azure.storage.blob.aio")` unguarded. If a user installs `durable-outbox` without the `[azure]` extra and reaches this constructor (the only documented way to get a working Azure client, per `tests/integration/test_aspire_azurite_kafka.py:57`), they get a raw `ModuleNotFoundError: No module named 'azure'` rather than a `ConfigurationError` telling them to install the `azure` extra. The same is true for `_if_not_modified()` at `:138`. Compare with `kafka.py:215-221`, which does this correctly.

**Impact**: Discoverability / ergonomics. Users get a stack trace instead of an actionable message. For a library that ships extras, the friendly `ConfigurationError` is part of the public contract.

**Recommendation**: Wrap both `import_module("azure.storage.blob.aio")` and `import_module("azure.core")` in `try/except ImportError as exc: raise ConfigurationError("Azure Blob store requires the azure extra: install durable-outbox[azure]") from exc`. Factor a small `_import_or_explain(module: str, extra: str)` helper in a new `durable_outbox/_optional.py` and reuse it from `azure_blob.py` and `kafka.py:_confluent_producer_factory`.

**Independence**: Independent.

---

### [Q-P0-2] Top-level `durable_outbox/__init__.py` is empty — public API has no shortest path

**Where**: `durable_outbox/__init__.py` (currently only 23 LOC re-exporting a subset of core; per Style report's analysis the file's user-facing exposure is minimal).

**Problem**: The README's quickstart imports `from durable_outbox.core import OutboxDispatcher, OutboxEvent`, which works, but the top-level package exposes nothing under `from durable_outbox import ...` beyond the limited current set. Reusable libraries conventionally re-export their "obvious" surface (`OutboxEvent`, `OutboxStatus`, `OutboxDispatcher`, `DurableOutboxStore`, `MessageSink`, the error hierarchy) from `durable_outbox` so users can `from durable_outbox import OutboxEvent`. Tools like IDE autocomplete and `from durable_outbox import *` give less than they could.

**Impact**: Ergonomics / discoverability. Every consumer must learn the internal `core/stores/sinks/testing/operations` layout immediately. For a 0.1.0 that aims to be reusable, this is the first thing a user notices.

**Recommendation**: Add an `__all__` to `durable_outbox/__init__.py` that re-exports the same names listed in `durable_outbox/core/__init__.py:23-42` plus `__version__ = "0.1.0"` read from `importlib.metadata.version("durable-outbox")`. Update the README quickstart to `from durable_outbox import OutboxDispatcher, OutboxEvent`.

**Independence**: Independent.

---

### [Q-P1-1] `ClaimConflictError` and `RetryableStoreError` are not exported via the public API

**Where**: `durable_outbox/core/__init__.py:23-42`, `durable_outbox/sinks/kafka.py:67,69,219`, `durable_outbox/stores/cosmos.py:132-137`.

**Problem**: `ConfigurationError` IS exported from `core/__init__.py:26`, but the *adapter modules* that actually raise it import it from `durable_outbox.core.errors` directly. That's fine, but `RetryableStoreError` and `ClaimConflictError` — both raised by `stores/blob_geo.py:71` (`BlobPreconditionFailedError(RetryableStoreError)`), `stores/sql.py:155`, `stores/cosmos.py:106` — are *not* in any `__all__` and are not re-exported. A user catching adapter failures has to import from `durable_outbox.core.errors` (a path the public API has not blessed).

**Impact**: Correctness via lint / ergonomics. Users can't write `except (RetryableStoreError, ClaimConflictError):` from the public API. Refactoring the private module path silently breaks downstream catches.

**Recommendation**: Add `ClaimConflictError` and `RetryableStoreError` to `core/__init__.py`'s `__all__` (and to the new top-level `__init__.py` from [Q-P0-2]). Mention the full error taxonomy in a new section of `docs/operations.md`.

**Independence**: Depends on [Q-P0-2] for top-level re-export.

---

### [Q-P1-2] Protocols have no docstrings — public contracts are unspecified beyond their signatures

**Where**: package-wide; specifically `core/store.py:13`, `core/sink.py:6`, `core/time.py:5`, `core/ordering.py:17,30`, `stores/blob_geo.py:53`, `stores/cosmos.py:61`, `stores/sql.py:89`, `sinks/kafka.py:34`, `telemetry/metrics.py:5`, `telemetry/tracing.py:11`, `operations.py:58,62,68`.

**Problem**: Every `Protocol` and most public dataclasses (`OutboxEvent`, `AcceptedReceipt`, `ClaimedEvent`, `PublishResult`, `OutboxCapabilities`, `RetryPolicy`, `DispatchSummary`, `CleanupPolicy`, `OutboxSettings`) have zero docstrings. `DurableOutboxStore.put` doesn't say "idempotent by event_id"; `claim_batch` doesn't say "returns at most `limit`"; `mark_pending_after_retryable_failure` doesn't say "release the claim". The proposal at `docs/durable-outbox-rpo0-proposal.md` §11 and §6 contains these contracts but they're not in code.

**Impact**: Maintainability / discoverability. A reusable library's protocols ARE the documentation. Implementers writing adapters in downstream services will guess.

**Recommendation**: Add docstrings to every protocol method and to every frozen dataclass in `core/`. Tie each to the named contract in the proposal (e.g. "Acceptance contract — see §6.1"). Then enable ruff `D` (or at least `D101,D102,D106`) with a `per-file-ignores` excluding tests; expect 30-50 new violations to fix, all in `core/`, `stores/`, `sinks/`, `operations.py`.

**Independence**: Independent. Recommended before the `D` ruleset is enabled ([Q-P1-3]).

---

### [Q-P1-3] Ruff `select` is missing high-payoff rule families: `SIM`, `PERF`, `PT`, `S`, `TCH`

**Where**: `pyproject.toml:54-65`.

**Problem**: Current select is `E,W,F,I,B,C4,UP,ANN,RUF`. Missing:
- `SIM` — would catch `if ... is None: return X else: return Y` patterns in `stores/blob_geo.py:80-82`, `stores/cosmos.py:88`, `stores/sql.py:124`, `stores/blob_geo.py:856-858`.
- `PERF` — list-of-tuple sort patterns in `stores/blob_geo.py:120-122,427-435`, `cosmos.py:350-358`, unnecessary `sorted(records.values(), ...)` per-call.
- `PT` (pytest) — broad `pytest.raises(ValueError)` at `tests/test_adapters.py:423`, bare `assert overwrite in {False, True}` at `tests/test_azure_blob_and_file_sink.py:64`.
- `S` (security) — would flag unbounded `int(...)` and `assert` for control flow.
- `TCH` — heavy imports that belong under `TYPE_CHECKING`.

**Impact**: Correctness via lint. These rules cost nothing to enable and would have caught real ergonomic issues cheaply.

**Recommendation**: Add `SIM`, `PERF`, `PT`, `S`, `TCH` to `[tool.ruff.lint] select`. Add `"S101"` (assert) to `per-file-ignores` for `tests/**`. Expect roughly: ~8 `SIM` in `stores/`, ~3 `PERF` in `stores/`, ~5 `PT` across `tests/`, ~10 `TCH` across `stores/` and `sinks/`. Fix or `# noqa` each.

**Independence**: Independent.

---

### [Q-P1-4] Strict `pyproject.toml` table key is wrong: `[tool.pytest]` should be `[tool.pytest.ini_options]`

**Where**: `pyproject.toml:37-49`.

**Problem**: Pytest reads its TOML config from `[tool.pytest.ini_options]` (per pytest docs). `[tool.pytest]` is silently ignored. That means `--strict-config`, `--strict-markers`, `filterwarnings = ["error"]`, `testpaths`, `markers = [...]`, and `strict_xfail` are all currently no-ops. The `@pytest.mark.load` and `@pytest.mark.integration` markers used in `tests/test_failure_load.py:36` and `tests/integration/test_aspire_azurite_kafka.py:14` should be raising "unknown mark" warnings, but they don't, *because the warning filter and the marker registration are both inert*. This is a real defect masked by both halves failing together.

**Impact**: Correctness via lint / test-gating. Integration tests advertised as `-m integration` will run by default if a developer omits the marker filter, because `filterwarnings = ["error"]` isn't applied and `testpaths` isn't pinned. `strict_xfail` is also lost.

**Recommendation**: Rename `[tool.pytest]` to `[tool.pytest.ini_options]` in `pyproject.toml`. Verify by running `uv run pytest --markers` and confirming `load` and `integration` are listed; then run a default `uv run pytest` and confirm tests under `tests/integration/` are deselected (they should be, since they rely on env vars and skip if unset — but the marker should still gate them explicitly).

**Independence**: Independent.

---

### [Q-P1-5] Heavy adapter SDK types aren't gated with `TYPE_CHECKING`

**Where**: `durable_outbox/stores/azure_blob.py:1-9`, `durable_outbox/sinks/kafka.py:1-14`.

**Problem**: The adapter modules use `Any` plus `import_module` to defer the actual SDK imports — good. But neither module uses `from __future__ import annotations` or a `if TYPE_CHECKING:` block to type the `container_client: Any` and `producer: KafkaProducerLike` parameters against the real SDK types. The protocols `BlobClientProtocol` and `KafkaProducerLike` are reasonable substitutes, but the `Any` annotation on `AzureBlobClient.__init__(container_client: Any)` (line 14) discards all type safety for the most common construction path. Same with `module: Any` at `:24`.

**Impact**: Typing rigor. The library's typed public surface degrades to `Any` at the most important integration point — the adapter constructor. The `ANN401` ignore in `pyproject.toml:70` justifies `**kwargs: Any` but not the constructor parameter.

**Recommendation**: Add `from __future__ import annotations` at the top of both `azure_blob.py` and `kafka.py`. Add `if TYPE_CHECKING: from azure.storage.blob.aio import ContainerClient` and type `container_client: ContainerClient`. Do the same with `confluent_kafka.Producer` in `kafka.py`. Keep `module: Any` only at the `import_module` call site (single line). Once `TCH` ([Q-P1-3]) is enabled, ruff will flag this automatically.

**Independence**: Depends on the `TCH` rule being enabled ([Q-P1-3]).

---

### [Q-P2-1] `_decode_event` silently substitutes `datetime.now(UTC)` for missing required timestamps

**Where**: `durable_outbox/stores/blob_geo.py:773-790` (specifically `created_at=_decode_datetime(...) or datetime.now(UTC)` on lines 783-784).

**Problem**: If a Blob record has corrupt or null `created_at`/`expires_at`, the decoder silently fills in *now* — which then passes `OutboxEvent.__post_init__`'s `expires_at > created_at` check (both are now), and the event silently re-enters the pipeline with bogus timestamps. The whole point of the validation in `core/model.py:46-47` and `core/validation.py:8-10` is to refuse ambiguous values. The same `or datetime.now(UTC)` substitution appears at `:810` for `PublishResult.published_at`.

**Impact**: Correctness. Bypasses the model's invariants; debugging a corrupted blob becomes harder because the timestamps will silently track wall-clock time on every dispatcher run.

**Recommendation**: Replace `or datetime.now(UTC)` with `raise RetryableStoreError(f"blob record missing created_at for event_id={data.get('event_id')!r}")` (and similarly for `expires_at` and `published_at`). Add a unit test in `tests/test_adapters.py` that round-trips a record with null `created_at` and asserts the error.

**Independence**: Independent. Composes with [S-P3-1].

---

### [Q-P2-2] Provider contract harness is a single happy-path function

**Where**: `durable_outbox/testing/provider_contract.py:36-64`, called only from `tests/test_core.py:85,91` and `tests/provider_contract/test_fake_store_contract.py:8`.

**Problem**: `run_basic_provider_contract` exercises put-idempotency, claim, retryable-failure, mark-sent — about 20 lines. `run_provider_contract` is identical (just wraps it). The proposal §25 lists ~20 named provider behaviors (claim-conflict, stale `IN_FLIGHT`, failover replay, ordered single-key blocking, cleanup freeze, payload size, etc.) — none of these are in the harness. The Blob, Cosmos, and SQL adapters get their per-behavior tests via individual parametrised cases in `tests/test_adapters.py` but the harness itself doesn't enforce the matrix.

**Impact**: Coverage / discoverability. Downstream adapter authors won't have a one-line "does my new store pass the contract?" answer. Today, `run_basic_provider_contract` returning successfully only proves the trivial path.

**Recommendation**: Promote each section of `tests/test_adapters.py`'s parametrised matrix (`test_provider_put_is_idempotent_for_compatible_duplicate`, `test_provider_put_rejects_incompatible_duplicate`, `test_provider_claim_retry_sent_failed_replay_and_cleanup_freeze`, `test_*_single_winner_*`) into named async helpers in `durable_outbox/testing/provider_contract.py` and call them from `run_provider_contract(contract: ProviderContract)`. Then add `BlobOutboxStore`, `DualRegionBlobOutboxStore`, `CosmosStrongOutboxStore`, `AzureSqlSyncOutboxStore`, `SqlAlwaysOnOutboxStore` to a parametrised test in `tests/test_adapters.py` that calls `run_provider_contract` exactly once per adapter.

**Independence**: Independent.

---

### [Q-P2-3] `AzureBlobClient.put_blob` does a get-after-put round trip on every write

**Where**: `durable_outbox/stores/azure_blob.py:103-106`.

**Problem**: After `upload_blob`, the adapter calls `await self.get_blob(name)` (line 103) just to read the new etag and metadata. That's 1 extra Azure request per write and 1 extra per record per state transition. For a 100 events/sec dispatcher that's an extra 100 RPS to Blob. `upload_blob` already returns the etag/last-modified via its response object.

**Impact**: Correctness/cost. Doubles the Azure request bill on the hot path. Also doubles latency on every state transition.

**Recommendation**: Capture the `BlobClient.upload_blob(...)` response and read `response['etag']` directly. Keep `metadata` from the `metadata=dict(metadata)` already passed in. Add a comment that this matches `BlobClient.upload_blob`'s documented return shape. Add a focused test that verifies `put_blob` issues exactly one Azure call.

**Independence**: Independent.

---

### [Q-P2-4] `DualRegionBlobOutboxStore` reaches into `_private` methods of `BlobOutboxStore`

**Where**: `durable_outbox/stores/blob_geo.py:614-649`.

**Problem**: Eight cross-class accesses to underscore-prefixed methods (`self.primary._load_record`, `self.primary._write_new_record`, `self.primary._save_record`, `self.secondary._load_record`). The class is in the same module so it works at runtime, but the semantics — "dual region is composed of two single-region stores plus mirroring" — are leaking. If `BlobOutboxStore`'s internal record management is ever refactored (e.g. to lazy-load), `DualRegionBlobOutboxStore` silently breaks.

**Impact**: Maintainability.

**Recommendation**: Promote `_load_record`, `_write_new_record`, `_save_record`, `_put_prepared`, `_accept_prepared` to a small `_BlobStoreInternals` mixin or to module-level helpers that take a `BlobOutboxStore` argument explicitly. Rename without the underscore where they truly are shared. Add a focused docstring describing the prepare/accept boundary.

**Independence**: Independent. Composes with [A-P0-2].

---

### [Q-P2-5] Naming inconsistency between SQL column suffixes and JSON key conventions

**Where**: `durable_outbox/stores/sql.py:40-50` vs `durable_outbox/stores/blob_geo.py:653-666` vs `docs/durable-outbox-rpo0-proposal.md:543-548,652-659`.

**Problem**: SQL columns use `*_utc` (`created_at_utc`, `expires_at_utc`, `claimed_at_utc`). Blob metadata uses `*_epoch_ms` (`created_at_epoch_ms`, `expires_at_epoch_ms`). The proposal §9 uses `*_at` for Python fields and §15.2 uses `createdAtEpochMs` (camelCase) for the Cosmos document shape. The actual Cosmos adapter doesn't serialize to JSON at all (it stores Python `CosmosStoredEvent` in an in-memory client), so the documented camelCase shape isn't enforced. The result: same field has 3 different names across 3 adapters.

**Impact**: Discoverability / future correctness. A real Cosmos client implementation will have to pick a serialization scheme; today there's no contract.

**Recommendation**: Add a `docs/data-model.md` (one page) listing the canonical field name and the per-adapter rendering. Pick one of `*_at_epoch_ms` (matches Blob) or `*_at_utc` (matches SQL) — recommend `*_at_epoch_ms` for any future JSON serialization since it's locale-free. Update the proposal's Cosmos JSON shape to match.

**Independence**: Independent. Documentation-only change.

---

### [Q-P2-6] `KafkaSink.publish` uses `asyncio.sleep(0)` busy-loop to yield while polling

**Where**: `durable_outbox/sinks/kafka.py:149-159`.

(Duplicate of [P-P1-2] — see that finding for full text. Listed here to confirm Style review independently flagged the same defect.)

**Independence**: Same as [P-P1-2].

---

### [Q-P3-1] No standard `logging` usage anywhere in the package

**Where**: package-wide. No `import logging` exists in any source file (`durable_outbox/**/*.py`).

**Problem**: The dispatcher silently swallows `mark_sent` failures (`dispatcher.py:77-84`), the Blob store silently retries on `BlobPreconditionFailedError` (`blob_geo.py:206-210, 300-301, 334-335`), and `_release_ordering_lease` silently no-ops if the lease is missing (`blob_geo.py:476-480`). For a library at the at-least-once delivery boundary, these are operationally important events. Today, all visibility is via the `MetricsAdapter` counter only — no message, no context, no event_id.

**Impact**: Maintainability / operability. Operators get a counter increment but no log line correlating it to an event_id. Debugging a stuck event becomes guesswork.

**Recommendation**: Add `_logger = logging.getLogger("durable_outbox")` in each module that handles error branches (`core/dispatcher.py`, `stores/blob_geo.py`, `stores/cosmos.py`, `stores/sql.py`, `sinks/kafka.py`). Log at `WARNING` for retryable conditions with `event_id=`, `topic=`, `error_type=` extras. Document in `docs/operations.md` that the library logs to the `durable_outbox` logger and the host should configure handlers.

**Independence**: Independent.

---

### [Q-P3-2] `int(_hash(event.event_id), 16) % N` is overkill

**Where**: `durable_outbox/stores/cosmos.py:340`.

**Problem**: `_hash` returns a 64-char hex string; `int(..., 16)` parses a 256-bit integer just to take `% unordered_buckets`. The hot path is per-event.

**Impact**: Performance (very minor at MVP scale).

**Recommendation**: Change to `int.from_bytes(sha256(event.event_id.encode()).digest()[:8], "big") % self.config.unordered_buckets`. Add a test that asserts the partition_key is stable across runs (lock in determinism).

**Independence**: Independent.

---

### [Q-P3-3] `pyproject.toml` packaging metadata is sparse

**Where**: `pyproject.toml:5-22`.

**Problem**: Missing `[project.urls]` (no Homepage, Source, Issues, Documentation), no `keywords`, no `Operating System :: OS Independent` classifier, no `Topic :: Software Development :: Libraries`, no `Framework :: AsyncIO`. The `authors` block has `name=` without an `email`. The `[tool.hatch.build.targets.wheel]` doesn't pin `force-include` for `py.typed`. The sdist includes `/tests` which is unusual.

**Impact**: Discoverability on PyPI / reproducibility.

**Recommendation**: Add `[project.urls]`, `keywords`, additional classifiers, optional `email` in authors. Verify `py.typed` ships in the wheel by inspecting the built `.whl`; if absent, add explicit `[tool.hatch.build] include = ["durable_outbox/py.typed"]`.

**Independence**: Independent.

---

### [Q-P3-4] `OutboxEvent.__post_init__` mutates frozen instance via `object.__setattr__`

**Where**: `durable_outbox/core/model.py:52` and `:100`.

**Problem**: Both `OutboxEvent` and `PublishResult` are `frozen=True, slots=True` dataclasses that use `object.__setattr__` to substitute a `MappingProxyType` for the mutable input. This works, but it makes `dataclass.replace()` behavior subtle.

**Impact**: Maintainability / subtle bug surface.

**Recommendation**: Use a factory function `OutboxEvent.create(...)` (a `@classmethod`) that performs the freeze before construction, then make the dataclass field accept *only* `MappingProxyType`. Alternatively, accept the current pattern but add a comment explaining the freeze. Lower-effort fix: add a comment.

**Independence**: Independent.

---

### [Q-P3-5] `MemoryOutboxStore`, `BlobOutboxStore`, `CosmosStrongOutboxStore`, `SqlOutboxStoreBase` duplicate `_eligible_for_claim` / `_in_flight_ordering_keys` / `_claim_ordered_records`

**Where**: `stores/memory.py:229-252`, `stores/blob_geo.py:437-460`, `stores/cosmos.py:349-381`, `stores/sql.py:202-211,377-398`.

**Problem**: Four near-identical implementations of "is this record claimable now" and "which ordering keys are in flight". The risk is divergence: a bug fix in one path won't reach the others.

**Impact**: Maintainability.

**Recommendation**: Define a `ClaimableRecord` protocol in `core/store.py` exposing the four fields. Move `_eligible_for_claim` and `_in_flight_ordering_keys` into a module-level function in `core/ordering.py` or a new `core/claim.py`. Have each store call the shared helper. Lock in with a single test in `tests/test_core.py`.

**Independence**: Independent.

---

### [Q-NIT-1] `FailingSink` is defined twice

**Where**: `durable_outbox/testing/failure_injection.py:15-31` and `tests/test_failover_ordering_cleanup.py:22-25`.

**Recommendation**: Replace the local class with the public `FailingSink`.

**Independence**: Independent.

---

### [Q-NIT-2] `FixedClock` is defined four times across the test suite

**Where**: `tests/test_core.py:28-33`, `tests/test_adapters.py:37-42`, `tests/test_kafka_operations.py:140-145`, `tests/test_operations.py:41-46`.

**Recommendation**: Add a `tests/conftest.py` with `FixedClock` and a `@pytest.fixture` for the standard event. Remove the duplicates.

**Independence**: Independent.

---

## 7. Coverage gaps (subagent + reviewer)

- **No real benchmarks exist.** `tests/test_failure_load.py:37` "load" test uses in-memory fakes — measures nothing about real adapters. A real perf baseline at 1000 msg/min sustained against `BlobOutboxStore` with `AzureBlobClient` (Azurite) is needed to confirm the magnitude of each performance finding.
- **`durable_outbox/testing/fake_store.py`** was not deeply analysed for scan characteristics.
- **Concurrency on `BlobOutboxStore.records`**: the in-memory mirror is mutated by all coroutines without a lock. When parallelism lands (P-P0-1), `self.records[event_id]` mutations across coroutines will race.
- **`durable_outbox/core/failover.py` `FailoverReplayer.replay_once`** is tested only against `FakeOutboxStore`. Blob/Cosmos/SQL failover replay is NOT exercised through `FailoverReplayer` — only direct `failover_replay_candidates` calls.
- **`durable_outbox/core/ordering.py`** `validate_ordered_event` and `one_per_ordering_key` and `InMemoryOrderingLockBackend.active_lease` are untested.
- **`durable_outbox/core/cleanup.py`** `CleanupPolicy` is dead/aspirational.
- **`durable_outbox/config/settings.py`** `OutboxSettings` looks like dead code.
- **`durable_outbox/telemetry/tracing.py`** is unused by every other module despite the proposal §22 promising OTel header propagation.
- **`durable_outbox/stores/azure_blob.py:127-134`** `list_blobs` per-item `get_blob` is untested under realistic load.
- **`durable_outbox/stores/sql.py:23-64`** `SQL_SCHEMA` is asserted to contain index names but no test parses or executes the DDL.
- **`durable_outbox/operations.py:141-231`** `AdminService.events()` gauge-emission side effect not tested.
- **`durable_outbox/sinks/kafka.py:215-223`** `_confluent_producer_factory` ImportError branch is not explicitly tested.
- **No test asserts** that optional adapter modules (`azure-storage-blob`, `azure-cosmos`, `pyodbc`, `confluent-kafka`, `aiohttp`) are not imported at `import durable_outbox`-time without their extras installed.
- **Integration tests** under `tests/integration/` only cover file sink and Kafka sink — no integration coverage for `DualRegionBlobOutboxStore`, `CosmosStrongOutboxStore`, or any SQL adapter against real services.

---

## 8. Suggested task-packet manifest for parallel coding agents

Each row below maps a finding cluster to a self-contained work packet sized for a single agent invocation. Suggested branch names use the finding-ID prefix.

| Packet | Findings | Files touched | Effort | Branch |
|---|---|---|---|---|
| **PKT-01** Dual-region failover role swap | A-P0-2, A-P2-1, A-P2-2 | `stores/blob_geo.py`, `tests/test_failover_ordering_cleanup.py`, new `docs/dual-region-failover-runbook.md` | M | `feat/a-p0-2-dual-region-role-swap` |
| **PKT-02** FailoverReplayer reliability + persistent freeze | A-P0-3, A-P1-1 | `core/failover.py`, all `stores/*.py`, `tests/test_failover_ordering_cleanup.py` | M | `fix/a-p0-3-replay-resume-freeze` |
| **PKT-03** Ordered-mode cross-process lease backend | A-P0-4, A-P1-2, A-P2-3 | new `stores/blob_lease.py`, `stores/blob_geo.py`, `core/ordering.py`, `tests/test_adapters.py` | M | `feat/a-p0-4-blob-lease-backend` |
| **PKT-04** Safe defaults — no implicit in-memory clients | A-P0-5 | `stores/blob_geo.py`, `stores/sql.py`, `stores/cosmos.py`, every test that constructs a store | L (high test churn) | `fix/a-p0-5-explicit-clients` |
| **PKT-05** Real SQL backend + real Cosmos backend | A-P0-1, P-P0-2, P-P0-5 | new `stores/sql_pyodbc.py`, new `stores/cosmos_azure.py`, refactor `stores/sql.py`, `stores/cosmos.py`, new integration tests | XL (split into two PRs) | `feat/p-p0-2-sql-pyodbc-backend` / `feat/p-p0-5-cosmos-azure-backend` |
| **PKT-06** Dispatcher concurrent publish + Kafka loop yield + jitter | P-P0-1, P-P1-2, P-P2-4, P-P1-5 | `core/dispatcher.py`, `sinks/kafka.py`, `core/retry.py`, `operations.py`, new `tests/perf/` | L | `feat/p-p0-1-concurrent-dispatch` |
| **PKT-07** Blob claim batch scan elimination | P-P0-4, P-P1-3, P-P1-4, P-P1-6, P-P2-3, P-P2-5 | `stores/blob_geo.py`, `stores/azure_blob.py`, mirror small changes in `stores/cosmos.py`, `stores/sql.py`, `stores/memory.py`; new `tests/perf/` | L | `perf/p-p0-4-blob-scan-elimination` |
| **PKT-08** Dual-region put parallelisation | P-P0-3 | `stores/blob_geo.py`, `tests/perf/` | S | `perf/p-p0-3-dual-region-parallel-put` |
| **PKT-09** Failover streaming + idempotent partial-replay | P-P1-1, A-P2-4 | `core/failover.py`, all `stores/*.py`, `tests/perf/` | M | `perf/p-p1-1-failover-streaming` |
| **PKT-10** Protocol completeness + capability gate | A-P1-3, A-P1-4 | `core/store.py`, `core/dispatcher.py`, `core/failover.py`, `tests/test_core.py` | S | `feat/a-p1-3-complete-store-protocol` |
| **PKT-11** Security hardening — limits + TLS + header filter | S-P0-1, S-P1-1, S-P1-2, S-P2-1, S-P2-2 | `core/validation.py`, `core/model.py`, `sinks/kafka.py`, `stores/azure_blob.py`, `tests/test_core.py` | M | `security/s-p0-1-input-limits-tls` |
| **PKT-12** Audit + admin async safety | S-P1-3 | `operations.py`, `sinks/file.py` (composes with P-P2-1) | S | `fix/s-p1-3-async-audit-io` |
| **PKT-13** Public API surface | Q-P0-2, Q-P1-1, Q-P1-2 | `durable_outbox/__init__.py`, `core/__init__.py`, docstrings across `core/`, `stores/`, `sinks/`, `operations.py` | M | `docs/q-p0-2-public-api-docstrings` |
| **PKT-14** Lint + packaging hardening | Q-P0-1, Q-P1-3, Q-P1-4, Q-P1-5, Q-P3-3 | `pyproject.toml`, `stores/azure_blob.py`, `sinks/kafka.py`, all per-file noqa fallout | M | `chore/q-p1-3-lint-and-packaging` |
| **PKT-15** Provider contract harness expansion | Q-P2-2 | `durable_outbox/testing/provider_contract.py`, parametrised tests in `tests/test_adapters.py` | M | `test/q-p2-2-contract-harness` |
| **PKT-16** Logging | Q-P3-1 | every adapter and dispatcher; `docs/operations.md` | S | `feat/q-p3-1-named-logger` |
| **PKT-17** Decode strictness + safety | Q-P2-1, S-P3-1, P-P2-2 | `stores/blob_geo.py`, `core/model.py` | S | `fix/q-p2-1-strict-decode` |
| **PKT-18** Cleanup batching + scheduler | P-P1-6, A-P3-1 (cleanup portion) | `core/cleanup.py` (new runner), every adapter's `cleanup_sent` | M | `feat/p-p1-6-cleanup-batch` |
| **PKT-19** Misc small wins | A-P3-1 (tracing/settings portions), A-NIT-1, P-P2-1, P-P2-2, P-P3-1, P-P3-2, Q-P2-3, Q-P2-4, Q-P2-5, Q-P2-6, Q-P3-2, Q-P3-4, Q-P3-5, Q-NIT-1, Q-NIT-2 | Various | M (cherry-pick) | `chore/misc-cleanups` |

**Sequencing notes**:
- PKT-04 (safe defaults) should land *before* PKT-15 (contract harness) to avoid massive test churn during the harness rework.
- PKT-06 (concurrent dispatch) unlocks the *value* of PKT-05 real backends and PKT-08 dual-region parallel put — those should land first or in parallel.
- PKT-13 (public API) is the highest-visibility user-facing improvement; recommend prioritising before any 0.2.0 release.
- Security PKT-11 and PKT-12 are independent of the perf/correctness work and can run on a parallel branch throughout.

---

## 9. Addendum — Architecture & Security retry passes (2026-05-25)

The Architecture and Security subagents stalled on first attempt; their reports above were written inline by the orchestrator. They were re-dispatched with a tighter scope ("find what was missed") and both completed. The new findings below are **additive** — they do not overlap with sections 3 (architecture) or 4 (security) above.

**Retry verdict (both agents)**: The original §3 and §4 P0s were independently confirmed. The Security retry would upgrade S-P1-2 (unfiltered Kafka headers) to P0 on the grounds that header passthrough is the most realistic credential-leak path. Otherwise, all existing findings stand.

### 9.1 New architecture findings

#### [A-NEW-P0-1] Dual-region failover replay is blind to PREPARED-only events; the secondary's accepted copy is silently lost on disaster

**Where**: `stores/blob_geo.py:528-540` (`put` ordering), `:579-588` (`failover_replay_candidates` reads only `self.primary`), `:283-284` (PREPARED-only records filtered in `BlobOutboxStore.failover_replay_candidates`), `:613-623` (`repair_prepared` — only callable per event_id).

**Problem**: `put()` performs `prepare(primary) → prepare(secondary) → accept(primary) → accept(secondary)`. If the process or primary region fails between steps 2 and 3, both regions hold a PREPARED-only record (`accepted=False`). The caller has already received an error and may have moved on; the event is in storage but invisible. After a real disaster the operator runs failover replay against this store — but `DualRegionBlobOutboxStore.failover_replay_candidates` delegates to `self.primary.failover_replay_candidates`, which excludes `accepted=False` records (`:283-284`). The secondary copy, even when fully prepared, is also never queried. There is no `list_prepared_event_ids()` so `repair_prepared` cannot be driven from operations.

**Impact**: P0 silent acceptance loss on the *exact* failure mode dual-region writes exist to defend against. The proposal §6.4 says failover replay must cover "all accepted events where `expires_at >= failover_started_at`"; the implementation drops every event the dual-write phase didn't reach `accept` on. The proposal's "Main safety rule: Dispatchers only process accepted=true" (§15.1) becomes a footgun without a PREPARED-repair sweep at startup.

**Recommendation**:
1. On `DualRegionBlobOutboxStore` startup (or as part of `freeze_cleanup` failover entry), perform a one-shot `await self._reconcile_prepared()` that lists both regions' event blobs, finds every event with `accepted=False` in either region, and calls `repair_prepared(event_id)` (or marks the event `FAILED` after a retention window, recording in audit).
2. Add a public `async def list_prepared_event_ids(self) -> Sequence[str]` so the operator can drive repair from the admin surface.
3. In `failover_replay_candidates`, after consulting the primary, scan the **secondary** for `accepted=True` records the primary lacks (e.g. primary failure between step 3 and step 4) and replay from secondary state. Composes with the role-swap work ([A-P0-2]).
4. Test: stage events through `_prepare(primary)` only, then `_prepare(secondary)` only, then `_prepare(primary)` + `_prepare(secondary)`, and assert that a failover replay either reports them as recoverable candidates after reconcile or fails fast (never silently 0).

**Independence**: Composes with [A-P0-2] (role swap) and [A-P0-3] (replay reliability). Should ship in the same PR as [A-P0-2].

---

#### [A-NEW-P0-2] `AdminService.manual_replay` calls `admin_actions.replay_event`, but no store implements `replay_event` — admin replay is dead code on every adapter

**Where**: `operations.py:62-66` (protocol declares `replay_event`), `:182-197` (`AdminService.manual_replay` calls it); zero implementations across `stores/*.py`. Only the test doubles in `tests/test_operations.py:36` and `tests/test_kafka_operations.py:127` provide it.

**Problem**: `OutboxAdminActions.replay_event` is a documented capability — the audit action `manual_replay` is a `Literal` member (`:37`), and `AdminService.manual_replay` is wired through `outbox_admin_actions_total{action="manual_replay"}`. But none of `MemoryOutboxStore`, `BlobOutboxStore`, `DualRegionBlobOutboxStore`, `CosmosStrongOutboxStore`, `AzureSqlSyncOutboxStore`, `SqlAlwaysOnOutboxStore` implements `replay_event`. A user wiring `AdminService(admin_actions=my_blob_store, …)` only gets `repair_failed_to_pending`; calling `manual_replay(event_id=...)` would `AttributeError` at runtime. [A-P1-3] notes the protocol gap on `cleanup_sent`/`repair_failed_to_pending` but doesn't catch this — `replay_event` is the inverse problem (the protocol has it, the implementations don't).

**Impact**: P0 functional gap on a privileged operational path. The aim list item "admin replay" is not delivered. Worse, the test harness mocks make the surface look complete.

**Recommendation**: Pick one of two paths and commit.
- **Path A (recommended)**: define `replay_event` on `DurableOutboxStore` with semantics "fetch the event for `event_id`, set status back to `PENDING`, clear `claim_token`, `claimed_at`, `last_error*`; reset `next_attempt_at=None`; do not change `attempt_count`". Implement on every adapter (`stores/*.py`). The Blob version is a small extension of `repair_failed_to_pending` allowing any status (including `SENT`).
- **Path B**: drop `replay_event` from `OutboxAdminActions`, rename `AdminService.manual_replay` to `AdminService.repair_failed`, and remove the `"manual_replay"` audit literal. Document that replay-of-SENT is handled exclusively via `FailoverReplayer`.

**Independence**: Independent. Affects API surface — should land before any 0.2.0 release.

---

#### [A-NEW-P1-1] `repair_failed_to_pending` does not reset `attempt_count`, `last_error*`, or `next_attempt_at`

**Where**: `stores/memory.py:216-221`, `stores/blob_geo.py:340-346`, `stores/cosmos.py:329-335`, `stores/sql.py:360-366`.

**Problem**: All four implementations set only `status = PENDING` and `failed_at = None`. They do not clear `attempt_count` (so the next retry uses the existing exponential backoff exponent — an event that hit `FAILED` after 7 attempts will wait 5 minutes before its first re-publish), `last_error_type`/`last_error` (surfaces in `AdminEventMetadata` and gauges, misleading dashboards), or `next_attempt_at`.

**Impact**: P1 operability. Repair-and-retry is meant to be the recovery path for poison-pill investigations; in practice it produces opaque behaviour ("I repaired it but it didn't try for 5 minutes" + "it still shows the old error in the admin UI").

**Recommendation**: In every adapter's `repair_failed_to_pending`, set:
```python
record.status = OutboxStatus.PENDING
record.failed_at = None
record.attempt_count = 0
record.last_error_type = None
record.last_error = None
record.next_attempt_at = None
record.claim_token = None
record.claimed_at = None
```
Add a parametrised provider-contract test `test_repair_failed_to_pending_clears_retry_state` covering all adapters.

**Independence**: Independent. Composes with [A-NEW-P0-2] which may consolidate this code into `replay_event`.

---

#### [A-NEW-P1-2] `repair_failed_to_pending` on SQL/Cosmos has unhandled CAS conflict

**Where**: `stores/cosmos.py:335`, `stores/sql.py:366`. Compare with the claim path (`cosmos.py:205-211`, `sql.py:237-242`) which correctly catches `ClaimConflictError`.

**Problem**: `repair_failed_to_pending` does `await self.client.replace(record, expected_version=record.version)`. If anything (cleanup tick, concurrent operator) bumps the row's version between `get` and `replace`, the call raises `ClaimConflictError` — which in `AdminService.repair_failed` propagates as an unhandled exception, returning a 500 to the operator with no audit trail and no `outbox_admin_actions_total` increment.

**Impact**: P1 operator endpoint flakiness; partial action goes unaudited.

**Recommendation**: Add a `_cas_update` helper that retries up to 3 times on `ClaimConflictError`:
```python
async def _cas_update(self, event_id, mutate, *, attempts=3) -> bool:
    for _ in range(attempts):
        record = await self.client.get(event_id)
        if record is None: return False
        mutate(record)
        try:
            await self.client.replace(record, expected_version=record.version)
            return True
        except ClaimConflictError:
            continue
    raise RetryableStoreError("repair lost too many CAS races")
```
Use from `repair_failed_to_pending`, `mark_sent`, `mark_failed`, `mark_pending_after_retryable_failure` to standardise CAS semantics.

**Independence**: Independent.

---

#### [A-NEW-P1-3] `DualRegionBlobOutboxStore.cleanup_sent` swallows secondary errors and reports primary-only count

**Where**: `stores/blob_geo.py:602-607`.

**Problem**:
```python
async def cleanup_sent(self, *, now, safety_margin) -> int:
    if self.cleanup_frozen: return 0
    deleted = await self.primary.cleanup_sent(...)
    await self.secondary.cleanup_sent(...)   # ← discarded count, no error handling
    return deleted
```
Three issues: (1) Dual-store freeze flag and per-region freeze flags can diverge if `primary.freeze_cleanup` is called directly. (2) Secondary deletion failures are silently swallowed — if primary deletes blob A but secondary call raises on blob A, regions diverge in a way that [A-NEW-P0-1] then mishandles after disaster. (3) Reported `deleted` count is primary-only.

**Impact**: P1. Mirror drift over time; failover replay sees fewer SENT records on post-failover active region.

**Recommendation**:
1. Gather both regions' deletes in parallel: `primary_count, secondary_count = await asyncio.gather(...)`. Return a `CleanupSummary(primary=..., secondary=...)`.
2. Queue failed secondary deletes for retry (`_pending_secondary_deletes: set[str]`). Emit `outbox_mirror_cleanup_failures_total{region="secondary"}`.
3. Single source of truth for freeze (persisted per [A-P1-1]).

**Independence**: Composes with [A-P1-1] and [A-P2-2].

---

#### [A-NEW-P1-4] `MemoryOutboxStore.repair_failed_to_pending` raises `KeyError` for unknown event_id

**Where**: `stores/memory.py:216-221`.

**Problem**: Uses `self.records[event_id]` (KeyError on missing) while Blob/Cosmos/SQL all return silently if the record is `None`. `AdminService.repair_failed` (`operations.py:165-180`) doesn't catch the difference.

**Impact**: P1 contract divergence. Memory is the canonical reference implementation; subtle inconsistencies propagate.

**Recommendation**:
```python
async def repair_failed_to_pending(self, *, event_id: str) -> None:
    record = self.records.get(event_id)
    if record is None or record.status is not OutboxStatus.FAILED:
        return
    record.status = OutboxStatus.PENDING
    record.failed_at = None
```
Add parametrised provider-contract test `test_repair_unknown_event_is_no_op`. Composes with [A-NEW-P1-1].

**Independence**: Independent.

---

#### [A-NEW-P1-5] `AcceptedReceipt.rpo_zero` is per-store-static, not per-event

**Where**: `core/model.py:78-83`, `stores/blob_geo.py:535-540`, `stores/sql.py:189-194`, `stores/cosmos.py:169-174`.

**Problem**: The receipt's `rpo_zero` is hard-coded from the capability declaration. There's no place in the data model to express "RPO=0 was *intended* but not certain for this particular event," and `rpo_zero` is duplicative of `store` + capability lookup. A reader writing `if receipt.rpo_zero:` then doing an action depending on durability has no way to validate the receipt wasn't fabricated.

**Impact**: P1 design integrity. The `AcceptedReceipt` is the boundary at which the caller gets to depend on the RPO=0 contract; today it's a copy of static config.

**Recommendation**: Either (a) remove `rpo_zero` from `AcceptedReceipt` (callers reference `store.capabilities.rpo_zero_for_accepted_events` once at startup), or (b) make `rpo_zero` a *per-put assertion*: add a structured `durability_witness: tuple[str, ...]` field listing the durability boundaries achieved (e.g. `("primary-westus", "secondary-eastus")` for dual-Blob; `("primary", "secondary-1")` for AlwaysOn). The witness is auditable; the static `bool` is not.

**Independence**: Independent. Schema-additive (add `durability_witness: tuple[str, ...] = ()`) so non-breaking.

---

#### [A-NEW-P1-6] In-flight ordering scope omits topic AND uses no separator — collision risk with topic names

**Where**: `stores/memory.py:238-252`, `stores/cosmos.py:367-381`, `stores/sql.py:384-398`. Blob (`stores/blob_geo.py:446-460`) correctly scopes by `_ordering_scope(record.event)`.

**Problem**: Strict super-set of [A-P1-2]. Three adapters (memory, cosmos, sql) store the raw `effective_ordering_key` as the set member without the topic prefix — *and* the Blob version's separator (`\0`) is not used elsewhere. The `value` stored in the set is a user-supplied string with no version prefix; a future rename to include topic would have to migrate semantics.

**Impact**: P1 — expands [A-P1-2] from "throughput regression" to "potential lock collision".

**Recommendation**: Adopt the existing review's `core/ordering.py` helper, but require the helper return a value with an embedded null byte and a version prefix: `f"v1\0{event.topic}\0{key}"`, so future renames are safe to evolve. Document the helper signature in the proposal's §18.

**Independence**: Strict super-set of [A-P1-2].

---

#### [A-NEW-P1-7] `FailoverReplayer` re-publishes SENT events without per-replay-cluster idempotency awareness

**Where**: `core/failover.py:18-31`, `stores/*.py` `failover_replay_candidates` blocks, Kafka sink config at `sinks/kafka.py:55-72`.

**Problem**: The Kafka sink declares `enable.idempotence=true`, which gives *producer-instance-scoped* idempotence (PID + sequence on a single broker cluster). After failover, the new producer has a fresh PID — Kafka idempotence will *not* dedupe; consumers must dedupe by `event_id` header. The library injects the header (`kafka.py:165-168`) but there is no test gate, no doc warning, and no consumer-side helper. The library should at minimum surface a Kafka **transactional** mode option (`transactional.id`) for replay-on-same-cluster scenarios; proposal §17 doesn't mention it.

**Impact**: P1 — the "consumers dedupe" contract is documented but not enforced or testable.

**Recommendation**:
1. Add `transactional_id: str | None = None` to `KafkaProducerConfig`. When set, wrap each `replay_once`'s publishes in `init_transactions`/`begin_transaction`/`commit_transaction`. Document the trade-off.
2. Log at `WARNING` the first time `FailoverReplayer.replay_once` publishes a `SENT` event: "Replaying event_id=X (status=SENT). Consumers must dedupe by event_id header."
3. Provide a small consumer-side helper module `durable_outbox.consumer.dedupe` that hashes `(topic, event_id)` and skips repeats.

**Independence**: Independent. Depends on [A-P0-3] for the loop-error fix being in place first.

---

#### [A-NEW-P2-1] `BlobOutboxStore._accept_prepared` overwrites `accepted_at` on every call, breaking acceptance-time monotonicity after repair

**Where**: `stores/blob_geo.py:358-367`.

**Problem**: `_accept_prepared` unconditionally sets `accepted_at = self.clock.utcnow()`. If invoked from `repair_prepared` long after the original prepare, the recorded acceptance time becomes the repair time. Combined with [Q-P2-1] (silent now() substitution), the original creation/acceptance window is hard to reconstruct after a repair.

**Impact**: P2 observability/forensics; affects `outbox_oldest_pending_age_seconds` and age-based alerting after a partial-write repair.

**Recommendation**: Mirror the `or now` idiom from `BlobOutboxStore.put` in `_accept_prepared`. If the goal is to record *when accept was achieved*, add a distinct field `accepted_at_durable: datetime | None` and keep `accepted_at` as "first-prepare time".

**Independence**: Independent. Pairs with [Q-P2-1].

---

#### [A-NEW-P2-2] `_ensure_compatible_duplicate` error message lacks field hint — operators waste time diagnosing duplicate conflicts

**Where**: `stores/blob_geo.py:418-424`, `_event_fingerprint` at `:817-823`.

**Problem**: The error message says "incompatible content" without saying which field diverged. Operators get no hint whether the producer is buggy (re-using event_ids) or the schema is evolving. Compare `MemoryOutboxStore._compatible_event` (`memory.py:255-266`) which lists fields explicitly and could trivially yield a field-by-field diff.

**Impact**: P2 operability. Duplicate conflicts are a common production diagnostic event; the current message wastes 5-20 minutes of investigation each time.

**Recommendation**:
```python
def _ensure_compatible_duplicate(self, record, event):
    diff = _first_field_difference(record.event, event)
    if diff is not None:
        field_name, stored, incoming = diff
        raise DuplicateEventConflictError(
            f"event_id {event.event_id!r} already exists with incompatible {field_name!r}: "
            f"stored={_redact(stored)} incoming={_redact(incoming)}"
        )
```
Where `_redact` truncates bytes/strings to 32 chars to avoid logging payloads. Apply across all four adapters.

**Independence**: Independent. Composes with [P-P1-3] — that fix can drop the fingerprint approach entirely in favour of field-by-field comparison.

---

#### [A-NEW-P2-3] RPO=0 contract is under-exercised by tests — would not catch silent skip of secondary write

**Where**: `tests/test_adapters.py:121-129`, `tests/test_failover_ordering_cleanup.py` (no dual-region failover replay test).

**Problem**: The single test that asserts the RPO=0 contract on dual-Blob reads the in-memory mirrors of both regions. There's no test that: (1) crashes after `_accept(primary)` and before `_accept(secondary)` and asserts recovery; (2) asserts `put()` raises if `_accept(secondary)` raises; (3) asserts `failover_replay_candidates` returns the event when only the secondary holds it; (4) asserts `receipt.rpo_zero is True` *only* when both regions confirmed.

**Impact**: P2. A regression in the dual-write fan-out would slip through.

**Recommendation**: Add the four tests above to `tests/test_adapters.py`. Provide a `FaultInjectionBlobClient(BlobClientProtocol)` in `durable_outbox.testing` that fails the Nth call, and expose `make_dual_region_store_with_faulty_secondary(fail_on="accept")`. Composes with [Q-P2-2] (contract harness expansion).

**Independence**: Independent. Composes with [Q-P2-2].

---

#### [A-NEW-NIT-1] `DualRegionBlobOutboxStore.put` returns the primary's `accepted_at` only

**Where**: `stores/blob_geo.py:528-540`.

**Problem**: For a fresh `put`, primary and secondary `accepted_at` differ by however long the secondary's accept took. The receipt reports the primary's, but the *correct* "RPO=0 was achieved at time T" is `max(primary.accepted_at, secondary.accepted_at)`.

**Recommendation**: After both `_accept` calls, set `receipt.accepted_at = max(primary.accepted_at, secondary.accepted_at)`.

**Independence**: Independent.

---

### 9.2 New security findings

#### [S-NEW-P1-1] Prometheus exposition injection via unsanitised `topic` metric label

**Where**: `core/dispatcher.py:42, 51-55, 68-72, 78-82, 85-87` (every `self.metrics.increment(..., topic=event.topic, ...)`); `operations.py:262-263` (`_escape_prometheus_label_value` escapes `\`, `\n`, `"` but not `\r`, `\f`, `\v`, NUL).

**Vulnerability**: `OutboxEvent.topic` is validated only as non-empty. Any caller can submit `topic="orders\rfake_metric{} 999999\n"`. When the `CollectingMetricsAdapter` renders to Prometheus, `_escape_prometheus_label_value` does not escape `\r`, so the rendered line becomes:
```
outbox_publish_attempts_total{topic="orders<CR>fake_metric{} 999999<LF>"} 1
```
A Prometheus scraper accepts `\r` followed by a fresh metric line, allowing the attacker to inject arbitrary fake samples (`up 1`, `outbox_events_failed_total 0`) into operator dashboards/alerts. The same attacker gets unbounded **metric cardinality** explosion — every distinct topic creates a new key, never evicted.

**Impact**: Integrity of operator-facing metrics (silent suppression of real alerts). Availability under cardinality blow-up. Exploitable by any caller with `OutboxStore.put()` access.

**Recommendation**:
1. Tighten escape function to cover all C0 controls:
```python
def _escape_prometheus_label_value(value: str) -> str:
    out = value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace('"', '\\"')
    return "".join(ch if ch.isprintable() or ch == " " else f"\\x{ord(ch):02x}" for ch in out)
```
2. Add a strict topic regex to `OutboxEvent.__post_init__` (Kafka-compatible: `^[A-Za-z0-9._-]{1,249}$`), raising `ValidationError` otherwise.
3. Add an optional `allowed_label_values` deny/allow-list to `MetricsAdapter` for label hardening.

**Independence**: Independent. The escape fix is local; the topic regex composes with [S-P2-1] header bounds.

---

#### [S-NEW-P1-2] Unbounded `error_message` from `str(exc)` — pyodbc write failure stalls IN_FLIGHT events forever

**Where**: `core/dispatcher.py:65` (`error_message=str(exc)`); `stores/sql.py:52` (schema `NVARCHAR(1024)`); `stores/cosmos.py:249-263`; `stores/blob_geo.py:245-263`.

**Vulnerability**: A 1500-character Kafka error message overflows `NVARCHAR(1024)`. pyodbc raises `DataError` → bubbles out of `mark_pending_after_retryable_failure` → dispatcher catches nothing → event stays `IN_FLIGHT` until `claim_timeout`. The same broker keeps returning the same long message, so every claim re-fails identically. Self-amplifying poison-pill DoS. For Cosmos, per-document size headroom shrinks; eventually all replaces rejected. For Blob, full record re-encoded per save means cost paid every CAS cycle.

**Impact**: Availability — pipeline-wide stall via single bad event class. Integrity — original error truncated silently with no metric.

**Recommendation**:
1. Truncate in `dispatcher.py` before the store call:
```python
_MAX_ERROR_MESSAGE_BYTES = 512
def _truncate_error(exc: BaseException) -> str:
    message = str(exc)
    encoded = message.encode("utf-8", errors="replace")
    if len(encoded) <= _MAX_ERROR_MESSAGE_BYTES:
        return message
    return encoded[: _MAX_ERROR_MESSAGE_BYTES - 1].decode("utf-8", errors="ignore") + "…"
```
2. Add `outbox_store_update_failures_total{error_type=...}` increment around the `mark_*` calls so a stuck-IN_FLIGHT pattern is observable.
3. SQL: relax to `NVARCHAR(2048)` to give headroom; document the truncation.

**Independence**: Independent. Composes with [S-P1-1] (PLAINTEXT allows attacker-controlled error messages).

---

#### [S-NEW-P1-3] `claim_token` exposure surface needs hardening — currently safe but one PR away from leakage

**Where**: `stores/blob_geo.py:412, 415`; `stores/cosmos.py:346`; `stores/sql.py:374`; `stores/memory.py:226`; `core/dispatcher.py:65`.

**Vulnerability**: `ClaimConflictError("claim token does not match current owner")` is benign today, but `dispatcher.py:65` does `error_message=str(exc)` for the outer `Exception` block. If a future store implementation includes the `claim_token` in the conflict message (a common diagnostic temptation), that token would be persisted into `last_error` on the very record whose claim it identifies — surfaced in `AdminEventMetadata`. A leaked claim_token enables a different dispatcher to bypass `ClaimConflictError` and mutate the IN_FLIGHT record.

**Impact**: Integrity. Preventive — not exploitable today, trivially introducible later.

**Recommendation**:
1. Add CI gate `tests/test_security.py::test_claim_token_never_in_error_message` asserting no `ValueError`/`ClaimConflictError`/`RetryableStoreError` message contains the token.
2. Use `hmac.compare_digest(record.claim_token or "", claimed.claim_token)` in every `_claimed_record` instead of `!=` for defence-in-depth against timing side-channels.
3. In `dispatcher.py`, strip UUID-looking substrings from `error_message` before persisting: `re.sub(r"[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}", "<uuid>", message)`.

**Independence**: Independent.

---

#### [S-NEW-P2-1] Azure blob metadata header injection via `topic`/`environment` — producer-induced permanent stall

**Where**: `stores/blob_geo.py:652-666` (`blob_metadata`), called from every `_save_record` / `_write_new_record`.

**Vulnerability**: `blob_metadata` writes raw `event_id`, `topic`, `environment` strings into Azure metadata, which the SDK serialises as HTTP headers `x-ms-meta-<key>: <value>` subject to RFC 7230 grammar. A producer can submit `event_id="x\rPOISON"` or `topic="orders\r\n..."`; the Azure SDK then raises `ValueError` from inside `_validate_metadata_request_headers`, which the dispatcher catches and re-stores the error message — the event cycles indefinitely. Defence-in-depth for the same root cause as [S-NEW-P1-1].

**Impact**: Availability — producer-induced permanent stall.

**Recommendation**: Add `enforce_metadata_safe(value: str)` in `core/validation.py` rejecting any control character (`<0x20`, `0x7F`, non-ASCII) for `event_id`, `topic`, and any operator-supplied string written to backend metadata; call from `OutboxEvent.__post_init__` and from `BlobOutboxStore.__init__` for `environment`.

**Independence**: Composes with [S-NEW-P1-1] (same root cause; fix once).

---

#### [S-NEW-P2-2] `_decode_record` accepts secondary-region blob content without re-validating fingerprint — asymmetric-credential injection

**Where**: `stores/blob_geo.py:629-638, 733-751`.

**Vulnerability**: `DualRegionBlobOutboxStore.repair_prepared` falls back to `secondary_record` if primary is missing. `_load_record` blindly trusts decoded JSON. An attacker with write access to **only** the secondary container (realistic split-credential scenario: primary access-protected, secondary RA-GRS-exposed) can pre-poison a secondary record. During failover/repair, the poisoned event is re-prepared into the primary as if it originated there. `event_fingerprint` is written into metadata but never verified on read.

**Impact**: Integrity. A secondary-only attacker can inject events during DR exercises. The asymmetric-credential model is exactly the case the dual-region design caters for.

**Recommendation**:
1. In `_load_record`, after `_decode_record`, recompute `_event_fingerprint(record.event)` and compare against `blob.metadata.get("event_fingerprint")`. Raise `BlobIntegrityError(NonRetryablePublishError)` on mismatch.
2. Replace the unkeyed `_event_fingerprint` with an HMAC over a `BlobOutboxStore(integrity_key: bytes | None = None)` — combines with [S-P2-3] for one fix covering both mechanisms.

**Independence**: Composes with [S-P2-3].

---

#### [S-NEW-P2-3] Dependency floors permit transitive-CVE territory if lock file is bypassed

**Where**: `pyproject.toml:26` — `azure-storage-blob>=12.23.0`, `azure-cosmos>=4.7.0`, `aiohttp>=3.13.0`, `pyodbc>=5.2.0`, `confluent-kafka>=2.6.0`.

**Vulnerability**: `uv.lock` currently resolves to safe versions (`12.29.0` for azure-storage-blob, etc.), but the floors permit `azure-storage-blob==12.23.0` → `azure-core>=1.30.0` → potential `cryptography<43.0.1` resolution paths flagged in late-2025 disclosure cycles (CVE-2024-12797 / Raccoon-style padding-oracle, fixed in `cryptography==44.0.1`). Best-effort finding; risk realised only for users installing without honouring the lock.

**Impact**: Confidentiality, capped severity because lock pins safe versions.

**Recommendation**:
1. Raise floors to align with the resolved lock: `azure-storage-blob>=12.29.0`, `azure-cosmos>=4.15.0`, `aiohttp>=3.13.5`, `pyodbc>=5.3.0`, `confluent-kafka>=2.14.0`.
2. Add `dependabot.yml` to drive floors forward.
3. Add `pip-audit` to CI when CI exists.

**Independence**: Independent.

---

#### [S-NEW-P3-1] `_decode_event` silently substitutes `datetime.now(UTC)` — creates zombie events that bypass TTL and cleanup

**Where**: `stores/blob_geo.py:783-784`.

**Vulnerability**: This is **the security framing of [Q-P2-1]** — but the security impact wasn't characterised. A corrupted blob with `"created_at": null` and `"expires_at": null` is silently rehabilitated by substituting current time, which makes:
- `failover_replay_candidates`' `record.event.expires_at < failover_started_at` always false → event **always replayed**.
- Cleanup TTL guard `now > record.event.expires_at + safety_margin` becomes `now > now + 5min` → false → event **never cleaned up**.

Net effect of a storage-tier writer setting `expires_at = null`: persistent zombie events that bypass both TTL and cleanup.

**Impact**: Integrity (zombie events) + availability (cleanup never reclaims). Defence-in-depth; requires storage write access.

**Recommendation**: Same fix as [Q-P2-1] — raise `RetryableStoreError` on missing timestamps. Adding this as a separate finding to emphasise the security framing.

**Independence**: Identical pattern to [Q-P2-1].

---

#### [S-NEW-NIT-1] `AdminService._record_action` increments success metric before audit write completes

**Where**: `operations.py:215-231`.

**Vulnerability**: `self.metrics.increment("outbox_admin_actions_total", action=action, result=result)` runs before `await self.audit_sink.record(...)`. If the audit write fails (disk full in `JsonlAuditSink`), the metric already reads "success". Observability records claim audit happened when it didn't. Compounded by [S-P1-3] (audit blocks the loop) — a long audit write that times out leaves a counted-but-unaudited admin action.

**Impact**: Audit integrity. Operationally meaningful for compliance scenarios where "every replay must be audited" is an invariant.

**Recommendation**: Move the `self.metrics.increment` call to *after* `await self.audit_sink.record(...)`, and emit a separate `outbox_admin_audit_failures_total` counter inside an `except` block around the audit write.

**Independence**: Independent.

---

### 9.3 Updated task-packet manifest (incorporating addendum)

Add to §8:

| Packet | Findings | Files touched | Effort | Branch |
|---|---|---|---|---|
| **PKT-20** Dual-region PREPARED reconciliation + admin replay | A-NEW-P0-1, A-NEW-P0-2 | `stores/blob_geo.py`, all `stores/*.py`, `operations.py`, `tests/test_adapters.py` | M | `feat/a-new-p0-1-prepared-reconcile` |
| **PKT-21** Repair semantics + CAS retry helper | A-NEW-P1-1, A-NEW-P1-2, A-NEW-P1-4 | all `stores/*.py` | S | `fix/a-new-p1-1-repair-semantics` |
| **PKT-22** Dual-region cleanup robustness | A-NEW-P1-3 | `stores/blob_geo.py`, `tests/` | S | `fix/a-new-p1-3-dual-cleanup` |
| **PKT-23** Receipt durability witness | A-NEW-P1-5 | `core/model.py`, all `stores/*.py` `put()`, docs | S | `feat/a-new-p1-5-durability-witness` |
| **PKT-24** Ordering scope helper (super-set of A-P1-2) | A-NEW-P1-6 | `core/ordering.py`, all `stores/*.py` `_in_flight_ordering_keys` | S | `feat/a-new-p1-6-ordering-scope` (supersedes PKT-02 ordering portion) |
| **PKT-25** Kafka transactional replay mode | A-NEW-P1-7 | `sinks/kafka.py`, `core/failover.py`, new `consumer/dedupe.py`, docs | M | `feat/a-new-p1-7-replay-transactions` |
| **PKT-26** Diagnostic improvements (accepted_at, dup-conflict messages) | A-NEW-P2-1, A-NEW-P2-2, A-NEW-NIT-1 | `stores/blob_geo.py`, all stores' `_ensure_compatible_duplicate` | S | `chore/a-new-p2-diagnostics` |
| **PKT-27** RPO=0 contract test expansion | A-NEW-P2-3 | `durable_outbox/testing/`, `tests/test_adapters.py` | M | `test/a-new-p2-3-rpo-contract-coverage` |
| **PKT-28** Prometheus + metadata sanitisation (compose with topic regex) | S-NEW-P1-1, S-NEW-P2-1 | `operations.py`, `core/model.py`, `core/validation.py` | S | `security/s-new-p1-1-prometheus-sanitise` |
| **PKT-29** Error-message size cap + observability | S-NEW-P1-2 | `core/dispatcher.py`, `stores/sql.py` schema | S | `fix/s-new-p1-2-error-truncation` |
| **PKT-30** Claim token CI gate + constant-time comparison | S-NEW-P1-3 | new `tests/test_security.py`, all `_claimed_record` impls | S | `security/s-new-p1-3-claim-token-hardening` |
| **PKT-31** Integrity-keyed fingerprint on Blob | S-NEW-P2-2 | `stores/blob_geo.py` (composes with S-P2-3) | S | `security/s-new-p2-2-blob-integrity-key` |
| **PKT-32** Dependency floor bump + dependabot | S-NEW-P2-3 | `pyproject.toml`, new `.github/dependabot.yml` | XS | `chore/s-new-p2-3-dep-floors` |
| **PKT-33** Audit-then-metric ordering | S-NEW-NIT-1 | `operations.py` | XS | `fix/s-new-nit-1-audit-order` |

**Sequencing additions**:
- PKT-20 (PREPARED reconcile) should land in the same PR window as PKT-01 (dual-region role swap) and PKT-02 (failover reliability).
- PKT-24 (ordering scope helper) **supersedes** the ordering portion of PKT-02 in §8.
- PKT-28 and PKT-29 are quick wins — recommend landing first to shrink the security surface before any 0.2.0 release.

### 9.4 Recount

After addendum, the full finding tally is:

| Dimension | P0 | P1 | P2 | P3 | NIT | Total |
|---|---:|---:|---:|---:|---:|---:|
| Architecture (incl. retry) | 7 | 11 | 6 | 1 | 2 | 27 |
| Security (incl. retry) | 1 | 6 | 6 | 4 | 2 | 19 |
| Performance | 5 | 6 | 5 | 2 | 1 | 19 |
| Code quality | 2 | 5 | 6 | 5 | 2 | 20 |
| **Total** | **15** | **28** | **23** | **12** | **7** | **85** |

The library has substantial real-world readiness work ahead. The P0s cluster in two themes: **(a) the dual-region failover story is partially implemented — role-swap, PREPARED reconciliation, admin replay, freeze persistence are all missing**, and **(b) the SQL and Cosmos adapters are protocol-only — no production-ready backend client ships**. None of the P0s is a deep design flaw; all are bounded engineering work that fits into the parallel-agent packet structure in §8 + §9.3.

---

## 10. What was NOT reviewed (consolidated)

- **`durable_outbox/testing/fake_store.py`** and **`fake_sink.py`** — light read only.
- **`integration/aspire/**`** C#/Aspire scaffolding — out of scope for the Python package review.
- **`tests/integration/test_aspire_azurite_kafka.py`** — only browsed for envvar conventions.
- **`uv.lock`** — not audited for transitive CVEs; recommend running `uv pip audit` or `pip-audit` as part of CI.
- **Live behaviour against real Azure/Cosmos/SQL/Kafka** — credential-gated.
- **Concurrency-stress under real parallelism** — no parallel runs exercised; all findings about race conditions inferred from code shape.

---

*End of report.*
