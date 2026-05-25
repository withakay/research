import json
import threading
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from durable_outbox.core.model import OutboxStatus
from durable_outbox.operations import (
    AdminEventMetadata,
    AdminService,
    AuditRecord,
    CollectingMetricsAdapter,
    JsonlAuditSink,
    MetricSample,
    StatusSummary,
)


@dataclass(slots=True)
class OperationsAdapter:
    events: list[AdminEventMetadata]
    action_result: bool = True

    async def list_event_metadata(self) -> list[AdminEventMetadata]:
        return self.events

    async def repair_failed_to_pending(self, *, event_id: str) -> bool:
        if self.action_result:
            self.events = [
                event.as_pending() if event.event_id == event_id else event
                for event in self.events
            ]
        return self.action_result

    async def replay_event(self, *, event_id: str) -> bool:
        _ = event_id
        return self.action_result


class FailingAuditSink:
    async def record(self, record: AuditRecord) -> None:
        _ = record
        raise RuntimeError("audit unavailable")


class FixedClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def utcnow(self) -> datetime:
        return self.now


def make_metadata(
    event_id: str,
    status: OutboxStatus,
    *,
    topic: str = "durable.outbox.outputs",
    last_error_type: str | None = None,
) -> AdminEventMetadata:
    now = datetime(2026, 5, 21, tzinfo=UTC)
    return AdminEventMetadata(
        event_id=event_id,
        topic=topic,
        status=status,
        created_at=now,
        expires_at=now + timedelta(minutes=15),
        attempt_count=2,
        last_error_type=last_error_type,
    )


@pytest.mark.asyncio
async def test_jsonl_audit_sink_appends_fsynced_records(tmp_path: Path) -> None:
    occurred_at = datetime(2026, 5, 21, 10, 30, tzinfo=UTC)
    path = tmp_path / "audit" / "outbox-admin.jsonl"
    sink = JsonlAuditSink(path)

    await sink.record(
        AuditRecord(
            action="repair_failed",
            event_id="event-1",
            operator="ops@example.test",
            reason="route fixed",
            occurred_at=occurred_at,
        )
    )
    await sink.record(
        AuditRecord(
            action="manual_replay",
            event_id="event-2",
            operator="ops@example.test",
            reason="downstream replay",
            occurred_at=occurred_at,
        )
    )

    records = [json.loads(line) for line in path.read_text().splitlines()]
    assert records == [
        {
            "action": "repair_failed",
            "event_id": "event-1",
            "occurred_at": occurred_at.isoformat(),
            "operator": "ops@example.test",
            "reason": "route fixed",
        },
        {
            "action": "manual_replay",
            "event_id": "event-2",
            "occurred_at": occurred_at.isoformat(),
            "operator": "ops@example.test",
            "reason": "downstream replay",
        },
    ]


@pytest.mark.asyncio
async def test_jsonl_audit_sink_runs_fsync_off_event_loop_thread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    occurred_at = datetime(2026, 5, 21, 10, 30, tzinfo=UTC)
    path = tmp_path / "audit" / "outbox-admin.jsonl"
    sink = JsonlAuditSink(path)
    loop_thread_id = threading.get_ident()
    observed = SimpleNamespace(thread_id=None)

    def fsync(fd: int) -> None:
        _ = fd
        observed.thread_id = threading.get_ident()

    monkeypatch.setattr("durable_outbox.operations.os.fsync", fsync)

    await sink.record(
        AuditRecord(
            action="repair_failed",
            event_id="event-1",
            operator="ops@example.test",
            reason="route fixed",
            occurred_at=occurred_at,
        )
    )

    assert observed.thread_id is not None
    assert observed.thread_id != loop_thread_id


@pytest.mark.asyncio
async def test_admin_service_does_not_audit_unsuccessful_action(
    tmp_path: Path,
) -> None:
    occurred_at = datetime(2026, 5, 21, tzinfo=UTC)
    adapter = OperationsAdapter(
        events=[make_metadata("event-1", OutboxStatus.FAILED)],
        action_result=False,
    )
    metrics = CollectingMetricsAdapter()
    audit_path = tmp_path / "audit.jsonl"
    service = AdminService(
        status_reader=adapter,
        admin_actions=adapter,
        audit_sink=JsonlAuditSink(audit_path),
        metrics=metrics,
        clock=FixedClock(occurred_at),
    )

    repaired = await service.repair_failed(
        event_id="missing",
        operator="ops@example.test",
        reason="not found",
    )

    assert repaired is False
    assert not audit_path.exists()
    assert (
        MetricSample(
            name="outbox_admin_actions_total",
            kind="counter",
            value=1.0,
            labels=(("action", "repair_failed"), ("result", "not_found")),
        )
        in metrics.collect()
    )


@pytest.mark.asyncio
async def test_admin_service_exports_status_gauges_for_collectors() -> None:
    adapter = OperationsAdapter(
        events=[
            make_metadata("event-1", OutboxStatus.PENDING),
            make_metadata("event-2", OutboxStatus.IN_FLIGHT),
            make_metadata("event-3", OutboxStatus.SENT),
            make_metadata(
                "event-4",
                OutboxStatus.FAILED,
                last_error_type="NonRetryablePublishError",
            ),
        ]
    )
    metrics = CollectingMetricsAdapter()
    service = AdminService(
        status_reader=adapter,
        admin_actions=adapter,
        metrics=metrics,
    )

    assert await service.status() == StatusSummary(
        pending=1,
        in_flight=1,
        sent=1,
        failed=1,
    )

    assert tuple(asdict(sample) for sample in metrics.collect()) == (
        {
            "name": "outbox_events_failed_total",
            "kind": "gauge",
            "value": 1,
            "labels": (),
        },
        {
            "name": "outbox_events_in_flight_total",
            "kind": "gauge",
            "value": 1,
            "labels": (),
        },
        {
            "name": "outbox_events_pending_total",
            "kind": "gauge",
            "value": 1,
            "labels": (),
        },
        {
            "name": "outbox_events_sent_total",
            "kind": "gauge",
            "value": 1,
            "labels": (),
        },
    )


def test_collecting_metrics_adapter_exports_prometheus_text() -> None:
    metrics = CollectingMetricsAdapter()
    metrics.increment(
        "outbox_admin_actions_total",
        action='manual_replay"quoted\rlater',
        result="success\nnext\x00hidden",
    )
    metrics.gauge("outbox_events_pending_total", 2)

    assert metrics.to_prometheus_text() == (
        "# TYPE outbox_admin_actions_total counter\n"
        'outbox_admin_actions_total{action="manual_replay\\"quoted\\rlater",'
        'result="success\\nnext\\x00hidden"} 1\n'
        "# TYPE outbox_events_pending_total gauge\n"
        "outbox_events_pending_total 2\n"
    )


@pytest.mark.asyncio
async def test_admin_service_records_success_metric_only_after_audit_success() -> None:
    occurred_at = datetime(2026, 5, 21, tzinfo=UTC)
    adapter = OperationsAdapter(
        events=[make_metadata("event-1", OutboxStatus.FAILED)],
        action_result=True,
    )
    metrics = CollectingMetricsAdapter()
    service = AdminService(
        status_reader=adapter,
        admin_actions=adapter,
        audit_sink=FailingAuditSink(),
        metrics=metrics,
        clock=FixedClock(occurred_at),
    )

    with pytest.raises(RuntimeError, match="audit unavailable"):
        await service.repair_failed(
            event_id="event-1",
            operator="ops@example.test",
            reason="route fixed",
        )

    assert (
        MetricSample(
            name="outbox_admin_actions_total",
            kind="counter",
            value=1.0,
            labels=(("action", "repair_failed"), ("result", "audit_failed")),
        )
        in metrics.collect()
    )
    assert not any(
        sample.labels == (("action", "repair_failed"), ("result", "success"))
        for sample in metrics.collect()
    )
