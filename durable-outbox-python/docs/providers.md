# Durable Outbox Providers

RPO=0 is an adapter acceptance contract. A store may claim RPO=0 only when `put(event)` returns after the event is durably present at that adapter's documented failover durability boundary.

## Blob

Blob RPO=0 requires application-level dual writes:

1. Write a PREPARED record in Region A.
2. Write a PREPARED record in Region B.
3. Mark Region A accepted.
4. Mark Region B accepted.
5. Return success only after both regions are accepted.

Azure Storage GRS, RA-GRS, GZRS, or RA-GZRS alone is asynchronous replication and is not an RPO=0 acceptance boundary.

`AzureBlobClient` adapts Azure Blob Storage, including Azurite, to the
`BlobOutboxStore` protocol. The optional Aspire integration suite starts
Azurite for local Blob coverage and Kafka for real broker coverage.

Ordered Blob publishing requires a cross-process ordering lock. By default,
`BlobOutboxStore` uses `BlobOrderingLockBackend`, which stores lock blobs with
conditional writes through the same blob client. Deployments can provide another
`OrderingLockBackend` implementation, such as Redis, when lock coordination
should live outside the outbox container. `BlobOutboxStore.for_testing()` remains
the intended path for process-local in-memory tests.

Cleanup freeze state is written to the backing provider rather than only to the
store instance. A restarted Blob, SQL, or Cosmos store that reopens the same
backend will continue skipping TTL cleanup until `resume_cleanup()` clears the
freeze marker.

Every `AcceptedReceipt` includes a `durability_witness` tuple naming the
backend durability boundaries reached for that event. The legacy `rpo_zero`
boolean remains for compatibility, but operators should prefer the witness for
per-event audit evidence.

Blob records include an `event_fingerprint` metadata value so readers can detect
metadata/content mismatch before replaying a poisoned record. Without extra
configuration this is an unkeyed SHA-256 over the event envelope, which is
appropriate for corruption detection but can leak equality information to
principals with Blob metadata read access. Sensitive deployments should pass a
deployment-secret `fingerprint_key` to `BlobOutboxStore` or
`DualRegionBlobOutboxStore`; keyed mode stores an HMAC-SHA256 instead and all
readers of the same container must use the same key.

Blob stores set `max_payload_bytes` to 10 MiB. This keeps JSON/base64 encoding,
fingerprint calculation, and Blob readback below the adapter's defensive memory
budget. Larger messages should use a claim-check payload stored outside the
outbox record.

## Cosmos

Cosmos RPO=0 requires strong consistency, more than one region, and single-write configuration. Multi-write and session-consistency modes can still be useful, but they must not declare `rpo_zero_for_accepted_events=True` in certified mode.

`AzureCosmosOutboxClient` provides the first Azure Cosmos SDK-backed slice
behind the `azure` optional extra. It covers lazy SDK import, snake_case JSON
encode/decode with epoch-millisecond timestamps, point insert/read/replace/delete
operations, `_etag` optimistic concurrency mapping, cleanup-freeze control
items, `read_account()` validation for certified RPO=0 account shape, and
bounded candidate selection over configured or previously observed partition
keys. Candidate selection is intentionally partition-scoped: the client passes a
single `partition_key` to each Cosmos query and never enables cross-partition
querying. Query methods still return candidates only; claim ownership remains in
the store's `_etag` compare-and-swap `replace()` path. Replay candidate
streaming consumes Cosmos SDK query pages with a bounded k-way merge across
known partitions, keeping at most one active item per partition before the store
claims each replay event.

The client persists observed data partitions into a control-partition registry
and loads that registry before candidate queries. Operators can also seed
partitions explicitly with `add_known_partition_key()` for pre-created buckets or
ordered-key partitions that have not yet been observed by the current process.
It also creates a control-partition event index before writing each event item,
then marks that index committed after the event write succeeds. This gives
restart-safe `event_id -> partition_key` lookup, lets compatible duplicate
inserts resolve to the original record, rejects incompatible duplicates before a
second partition-local event can be written, and deletes the target event before
the index during cleanup/admin delete paths.
If an insert is interrupted after reserving the index but before writing the
event item, operators can call `AzureCosmosOutboxClient.repair_event_index()` to
remove the dangling index before retrying the write.

This is still not a full provider-contract client. Cross-partition index and
event writes cannot be made transactional through the single-container API.
Live Cosmos integration tests for SDK query behavior, registry completeness,
restart duplicate handling, conditional index commits, event-index repair, and
ETag conflicts remain required before treating this adapter as certified
provider complete.

Live Cosmos certification tests are present but opt-in. Set
`DURABLE_OUTBOX_COSMOS_LIVE=1`,
`DURABLE_OUTBOX_COSMOS_CONNECTION_STRING`,
`DURABLE_OUTBOX_COSMOS_DATABASE`, and `DURABLE_OUTBOX_COSMOS_CONTAINER` to run
the SDK-backed integration suite. `DURABLE_OUTBOX_COSMOS_REGIONS`,
`DURABLE_OUTBOX_COSMOS_CONSISTENCY`, and
`DURABLE_OUTBOX_COSMOS_MULTI_WRITE=1` describe the expected account shape, and
`DURABLE_OUTBOX_COSMOS_CERTIFY_ACCOUNT=1` enables the strong-consistency,
multi-region, single-write account validation assertion. Without
`DURABLE_OUTBOX_COSMOS_LIVE=1`, these tests skip in normal local and CI runs.

## SQL

Azure SQL RPO=0 requires committing the outbox row and then completing `sp_wait_for_database_copy_sync` against the active secondary before returning success. SQL Server Always On RPO=0 requires synchronous commit with the required synchronized secondaries configured.

`PyodbcSqlOutboxClient` provides the first production SQL client slice behind
the `sql` optional extra. It covers lazy pyodbc import, row encode/decode,
parameterized insert/update/delete/get primitives, cleanup freeze state, and
the SQL durability checks used by `AzureSqlSyncOutboxStore` and
`SqlAlwaysOnOutboxStore`. It also provides bounded provider-side queries for
normal claim, failover replay, and cleanup.

Normal dispatcher claims use a SQL Server single-statement
`UPDATE ... OUTPUT INSERTED.*` path when the client exposes the atomic claim
capability. SQL Server generates `NEWID()` claim tokens inside the update,
increments attempts, and returns already-claimed rows to the store, avoiding the
older select-candidates-then-CAS loop for pyodbc clients. Replay claiming still
uses a SQL Server `UPDATE ... OUTPUT` path when the pyodbc client is wired,
returning the original source status alongside the claimed row so SENT replay
warnings remain accurate. A longer-lived SQL replay cursor with explicit
batch-token rollback remains a future optimization for very large replay runs.

Live SQL certification tests are present but opt-in. Set
`DURABLE_OUTBOX_SQL_LIVE=1` and `DURABLE_OUTBOX_SQL_CONNECTION_STRING` to run
the pyodbc-backed integration suite against isolated test tables. Optional
`DURABLE_OUTBOX_SQL_TABLE_NAME` and
`DURABLE_OUTBOX_SQL_CLEANUP_STATE_TABLE_NAME`
override those generated table names. Set `DURABLE_OUTBOX_SQL_PARTNER_SERVER`
and `DURABLE_OUTBOX_SQL_PARTNER_DATABASE` to enable the Azure SQL
`sp_wait_for_database_copy_sync` acceptance test. Without
`DURABLE_OUTBOX_SQL_LIVE=1`, these tests skip in normal local and CI runs.

## Failover Replay Streaming

`FailoverReplayer` supports two store shapes. Existing stores can keep the
portable `failover_replay_candidates(..., limit, exclude_event_ids=...)` method,
which the replayer consumes in bounded pages. Providers that can hold a backend
cursor or stream claimed rows can additionally expose
`iter_failover_replay_candidates(failover_started_at=..., limit=...)` as an async
iterator. The replayer consumes that stream in bounded in-memory pages and uses
the same concurrent publish path, avoiding repeated list/exclusion calls.
The built-in Blob, dual-region Blob, SQL, and Cosmos stores expose this
streaming shape. SQL uses its atomic replay-claim capability when the client
provides one. Cosmos uses its client replay iterator when available, and
`AzureCosmosOutboxClient` drives that iterator from SDK query pages rather than
collecting all partition results first. Deeper backend-native replay cursors,
such as a SQL batch-token rollback cursor, remain provider-specific
optimizations.

## Kafka

The Kafka sink enforces certified producer defaults such as `acks=all` and
idempotence. For local integration tests that should not publish to Kafka,
`FileSink` writes the same event envelope to a JSONL file and returns
Kafka-like partition/offset metadata. This gives deterministic dispatch
coverage while the Aspire suite can still exercise a real Kafka broker. The
sink keeps its file handle open until `aclose()` or async context-manager exit,
defaults to `fsync=False` for local test throughput, and supports
`fsync_interval_events` / `fsync_interval_ms` when callers need batched local
durability.

Kafka producer idempotence is producer-session scoped. During failover replay,
previously sent events can be published again by a new producer, so consumers
must dedupe by the `event_id` Kafka header. `durable_outbox.consumer.EventDeduper`
provides a small `(topic, event_id)` helper for consumers that need a reference
implementation or test double.
