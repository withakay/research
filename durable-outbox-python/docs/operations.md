# Durable Outbox Operations

Operational hooks expose status and repair behavior without reading or logging opaque payload bytes. Hosting services remain responsible for authentication and authorization.

## Metrics

`AdminService` accepts any object implementing `MetricsAdapter`. For local
collection and scrape endpoints, use `CollectingMetricsAdapter`:

```python
from durable_outbox.operations import AdminService, CollectingMetricsAdapter

metrics = CollectingMetricsAdapter()
service = AdminService(
    status_reader=store_admin,
    admin_actions=store_admin,
    metrics=metrics,
)

prometheus_body = metrics.to_prometheus_text()
otel_samples = metrics.collect()
```

`to_prometheus_text()` emits Prometheus text exposition. `collect()` returns
structured metric samples with name, type, value, and labels so a host service
can bridge them to OpenTelemetry instruments without this package taking a hard
runtime dependency on an observability SDK.

- `outbox_events_pending_total{store,topic,environment}`
- `outbox_events_in_flight_total{store,topic,environment}`
- `outbox_events_sent_total{store,topic,environment}`
- `outbox_events_failed_total{store,topic,environment,error_type}`
- `outbox_oldest_pending_age_seconds{store,topic,environment}`
- `outbox_claim_conflicts_total{store}`
- `outbox_stale_in_flight_reclaims_total{store}`
- `outbox_put_latency_ms{store}`
- `outbox_put_failures_total{store,error_type}`
- `outbox_publish_attempts_total{topic}`
- `outbox_publish_success_total{topic}`
- `outbox_publish_failures_total{topic,error_type}`
- `outbox_mark_sent_failures_total{topic,error_type}`
- `outbox_publish_latency_ms{topic}`
- `failover_replay_events_total{store,topic}`
- `failover_replay_duration_seconds{store}`
- `failover_replay_lag_seconds{store}`
- `cleanup_deleted_events_total{store}`
- `cleanup_frozen{store}`
- `outbox_admin_actions_total{action,result}`

## Admin Hooks

Admin operations use structural protocols rather than test-store internals:

- `OutboxStatusReader` lists event metadata for status queries.
- `OutboxAdminActions` performs manual replay and FAILED-to-PENDING repair.
- `AuditSink` records operator identity, reason, timestamp, event id, and action.

Hosting services own authentication and authorization. The package records audit metadata supplied by the host and keeps payload bytes out of status and audit records.

Use `JsonlAuditSink` when the host needs a durable local audit trail:

```python
from durable_outbox.operations import AdminService, JsonlAuditSink

service = AdminService(
    status_reader=store_admin,
    admin_actions=store_admin,
    audit_sink=JsonlAuditSink("/var/log/durable-outbox/outbox-admin.jsonl"),
)
```

Each line is a JSON object containing `action`, `event_id`, `operator`,
`reason`, and `occurred_at`. The sink creates parent directories, appends one
record per successful admin action, flushes the file handle, and calls `fsync`
by default. Set `fsync=False` only when the host has a stronger durability
boundary around log shipping.

## Integration Credentials and Markers

Integration tests are opt-in and marked with `@pytest.mark.integration`.
Load-shaped tests are marked with `@pytest.mark.load`. Run the default suite
without external services:

```bash
uv run pytest
```

Run external-service checks explicitly after exporting provider credentials:

```bash
uv run pytest -m integration
uv run pytest -m load
```

Kafka integration hosts should provide `KAFKA_BOOTSTRAP_SERVERS` and any
security settings required by the selected producer configuration. Azure Blob,
Cosmos, and SQL integration hosts should provide credentials through the
standard Azure identity chain or service-specific connection strings used by
their adapter factory. Do not commit credentials, SAS tokens, broker passwords,
or generated client certificates; keep them in the CI secret store or the local
developer environment.

## Failover Runbook

1. Record `failover_started_at` before replay begins.
2. Freeze cleanup.
3. Replay accepted `PENDING`, `IN_FLIGHT`, and `SENT` events where `expires_at >= failover_started_at`.
4. Confirm replay progress and sink acknowledgements.
5. Resume cleanup after replay completion is recorded.

## Alerts

- Oldest pending age exceeds the service objective.
- Failed event count is non-zero for deterministic errors.
- Cleanup is not frozen during active failover.
- Failover replay lag is increasing.
- A certified RPO=0 provider reports degraded capability.

## Release Workflow Notes

1. Run the scoped verification suite:

   ```bash
   uv sync --group dev
   uv run pytest
   uv run ruff check .
   uv run ruff format --check .
   uv run ty check
   uv build
   ```

2. Confirm `README.md`, `docs/`, `LICENSE`, and `pyproject.toml` are included
   in the source distribution.
3. Tag releases from a clean commit after the package version is updated.
4. Publish with repository credentials from the release environment, never from
   a developer environment with long-lived credentials.
