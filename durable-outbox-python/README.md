# durable-outbox

Reusable Python 3.14 durable outbox primitives for at-least-once publishing with explicit provider capability declarations.

The core package is storage- and sink-agnostic. Applications persist accepted events through a `DurableOutboxStore`, publish them through a `MessageSink`, and let `OutboxDispatcher` handle claim, publish, retry, failed, and sent transitions.

## Install

```bash
uv add durable-outbox
uv add "durable-outbox[kafka]"
uv add "durable-outbox[azure]"
uv add durable-outbox-file-sink
uv add durable-outbox-sql-store
```

## Quickstart

```python
from datetime import UTC, datetime, timedelta

from durable_outbox import OutboxDispatcher, OutboxEvent
from durable_outbox.testing import FakeOutboxStore, FakeSink

now = datetime.now(UTC)
event = OutboxEvent(
    event_id="event-1",
    topic="durable.outbox.outputs",
    payload=b'{"ok": true}',
    key=b"model-run-1",
    headers={"content-type": b"application/json"},
    created_at=now,
    expires_at=now + timedelta(minutes=15),
)

store = FakeOutboxStore()
sink = FakeSink()
await store.put(event)
await OutboxDispatcher(store, sink).run_once(limit=100)
```

## Provider Modes

RPO=0 is an adapter acceptance contract, not a storage product label.

- Blob RPO=0 requires application-level dual writes to two regional outboxes. Azure GRS/RA-GRS alone is not certified RPO=0 for accepted events.
- Cosmos RPO=0 requires strong consistency, more than one region, and single-write configuration.
- SQL RPO=0 requires Azure SQL commit plus `sp_wait_for_database_copy_sync`, or SQL Server Always On synchronous commit with required synchronized secondaries.

## Provider Plugins

Provider implementations that are not part of the core package are loaded from
Python entry points. Install the plugin package, then load the configured store
or sink by name:

```python
from durable_outbox import load_sink, load_store

sink = load_sink("file", {"path": "published.jsonl"})
store = load_store("azure-sql-sync", {"connection_string": "Driver={ODBC Driver 18};..."})
```

Installed sink names are available with `available_sinks()`. Installed store
names are available with `available_stores()`. See
`docs/plugin-authoring.md` for local path plugins, registry-published plugins,
entry point metadata, factory signatures, and verification guidance.

## Operations

`durable_outbox.operations` provides structural admin protocols, durable
JSONL audit records, and a collector-style metrics adapter:

```python
from durable_outbox.operations import CollectingMetricsAdapter, JsonlAuditSink

metrics = CollectingMetricsAdapter()
audit_sink = JsonlAuditSink("/var/log/durable-outbox/outbox-admin.jsonl")
```

Use `metrics.to_prometheus_text()` for a Prometheus scrape response or
`metrics.collect()` to bridge samples into OpenTelemetry instruments in the host
service. See `docs/operations.md` for admin hooks, test markers, integration
credential expectations, and release workflow notes.

## Development

```bash
uv sync --all-packages --group dev
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv build --all-packages
```

## Provider Contract

Adapter authors can run the reusable provider matrix against a fresh in-memory
or test-container-backed store factory:

```python
from durable_outbox.testing import ProviderContract, run_provider_contract

await run_provider_contract(ProviderContract(store_factory=make_store))
```

The full contract checks compatible and incompatible duplicate puts, claim and
retry transitions, failover replay eligibility, ordered-key blocking, cleanup
freeze/resume, and admin repair/replay statuses.

## Integration Tests

The optional Aspire suite starts Azurite, Kafka, and the Python integration
tests as one local topology. It requires the Aspire CLI plus a local container
runtime such as Podman.

```bash
cd integration/aspire
aspire run --apphost DurableOutbox.Integration.AppHost/DurableOutbox.Integration.AppHost.csproj
```

The integration tests can also run against manually managed services:

```bash
export DURABLE_OUTBOX_AZURITE_CONNECTION_STRING="UseDevelopmentStorage=true"
export DURABLE_OUTBOX_KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
uv run --extra azure --extra kafka pytest -m integration tests/integration
```
