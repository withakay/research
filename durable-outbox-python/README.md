# durable-outbox

Reusable Python 3.14 durable outbox primitives for at-least-once publishing with explicit provider capability declarations.

The core package is storage- and sink-agnostic. Applications persist accepted events through a `DurableOutboxStore`, publish them through a `MessageSink`, and let `OutboxDispatcher` handle claim, publish, retry, failed, and sent transitions.

## Install

```bash
uv add durable-outbox
uv add "durable-outbox[kafka]"
uv add "durable-outbox[azure]"
uv add "durable-outbox[sql]"
```

## Quickstart

```python
from datetime import UTC, datetime, timedelta

from durable_outbox.core import OutboxDispatcher, OutboxEvent
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
uv sync --group dev
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv build
```

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
