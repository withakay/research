import asyncio
import json
import os
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Literal, Protocol

from durable_outbox.core.model import OutboxStatus
from durable_outbox.core.time import Clock, SystemClock
from durable_outbox.telemetry.metrics import MetricsAdapter, NoopMetrics


@dataclass(frozen=True, slots=True)
class StatusSummary:
    pending: int
    in_flight: int
    sent: int
    failed: int


@dataclass(frozen=True, slots=True)
class AdminEventMetadata:
    event_id: str
    topic: str
    status: OutboxStatus
    created_at: datetime
    expires_at: datetime
    attempt_count: int
    last_error_type: str | None = None

    def as_pending(self) -> AdminEventMetadata:
        return replace(self, status=OutboxStatus.PENDING, last_error_type=None)


AuditAction = Literal["repair_failed", "manual_replay"]


@dataclass(frozen=True, slots=True)
class AuditRecord:
    action: AuditAction
    event_id: str
    operator: str
    reason: str
    occurred_at: datetime

    def to_json_dict(self) -> dict[str, str]:
        return {
            "action": self.action,
            "event_id": self.event_id,
            "operator": self.operator,
            "reason": self.reason,
            "occurred_at": self.occurred_at.isoformat(),
        }


class OutboxStatusReader(Protocol):
    async def list_event_metadata(self) -> Sequence[AdminEventMetadata]: ...


class OutboxAdminActions(Protocol):
    async def repair_failed_to_pending(self, *, event_id: str) -> bool: ...

    async def replay_event(self, *, event_id: str) -> bool: ...


class AuditSink(Protocol):
    async def record(self, record: AuditRecord) -> None: ...


class NoopAuditSink:
    async def record(self, record: AuditRecord) -> None:
        _ = record


class JsonlAuditSink:
    def __init__(self, path: str | Path, *, fsync: bool = True) -> None:
        self.path = Path(path)
        self.fsync = fsync
        self._lock = asyncio.Lock()

    async def record(self, record: AuditRecord) -> None:
        line = json.dumps(record.to_json_dict(), sort_keys=True, separators=(",", ":"))
        async with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as audit_file:
                audit_file.write(f"{line}\n")
                audit_file.flush()
                if self.fsync:
                    os.fsync(audit_file.fileno())


MetricKind = Literal["counter", "gauge"]


@dataclass(frozen=True, slots=True)
class MetricSample:
    name: str
    kind: MetricKind
    value: float
    labels: tuple[tuple[str, str], ...] = ()


class CollectingMetricsAdapter:
    def __init__(self) -> None:
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}

    def increment(self, name: str, **labels: str) -> None:
        key = _metric_key(name, labels)
        self._counters[key] = self._counters.get(key, 0.0) + 1.0

    def gauge(self, name: str, value: float, **labels: str) -> None:
        self._gauges[_metric_key(name, labels)] = float(value)

    def collect(self) -> tuple[MetricSample, ...]:
        samples: list[MetricSample] = []
        samples.extend(
            MetricSample(name=name, kind="counter", value=value, labels=labels)
            for (name, labels), value in sorted(self._counters.items())
        )
        samples.extend(
            MetricSample(name=name, kind="gauge", value=value, labels=labels)
            for (name, labels), value in sorted(self._gauges.items())
        )
        return tuple(samples)

    def to_prometheus_text(self) -> str:
        lines: list[str] = []
        emitted_types: set[str] = set()
        for sample in self.collect():
            if sample.name not in emitted_types:
                lines.append(f"# TYPE {sample.name} {sample.kind}")
                emitted_types.add(sample.name)
            labels = _format_prometheus_labels(sample.labels)
            lines.append(f"{sample.name}{labels} {_format_metric_value(sample.value)}")
        return "\n".join(lines) + ("\n" if lines else "")


class AdminService:
    def __init__(
        self,
        *,
        status_reader: OutboxStatusReader,
        admin_actions: OutboxAdminActions,
        audit_sink: AuditSink | None = None,
        metrics: MetricsAdapter | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.status_reader = status_reader
        self.admin_actions = admin_actions
        self.audit_sink = audit_sink or NoopAuditSink()
        self.metrics = metrics or NoopMetrics()
        self.clock = clock or SystemClock()

    async def events(self) -> tuple[AdminEventMetadata, ...]:
        events = tuple(await self.status_reader.list_event_metadata())
        self._emit_status_gauges(events)
        return events

    async def status(self) -> StatusSummary:
        return status_summary(await self.events())

    async def repair_failed(
        self,
        *,
        event_id: str,
        operator: str,
        reason: str,
    ) -> bool:
        repaired = await self.admin_actions.repair_failed_to_pending(event_id=event_id)
        await self._record_action(
            action="repair_failed",
            event_id=event_id,
            operator=operator,
            reason=reason,
            succeeded=repaired,
        )
        return repaired

    async def manual_replay(
        self,
        *,
        event_id: str,
        operator: str,
        reason: str,
    ) -> bool:
        replayed = await self.admin_actions.replay_event(event_id=event_id)
        await self._record_action(
            action="manual_replay",
            event_id=event_id,
            operator=operator,
            reason=reason,
            succeeded=replayed,
        )
        return replayed

    def _emit_status_gauges(self, events: Sequence[AdminEventMetadata]) -> None:
        counts = status_summary(events)
        self.metrics.gauge("outbox_events_pending_total", counts.pending)
        self.metrics.gauge("outbox_events_in_flight_total", counts.in_flight)
        self.metrics.gauge("outbox_events_sent_total", counts.sent)
        self.metrics.gauge("outbox_events_failed_total", counts.failed)

    async def _record_action(
        self,
        *,
        action: AuditAction,
        event_id: str,
        operator: str,
        reason: str,
        succeeded: bool,
    ) -> None:
        result = "success" if succeeded else "not_found"
        if not succeeded:
            self.metrics.increment(
                "outbox_admin_actions_total",
                action=action,
                result=result,
            )
            return
        try:
            await self.audit_sink.record(
                AuditRecord(
                    action=action,
                    event_id=event_id,
                    operator=operator,
                    reason=reason,
                    occurred_at=self.clock.utcnow(),
                )
            )
        except Exception:
            self.metrics.increment(
                "outbox_admin_actions_total",
                action=action,
                result="audit_failed",
            )
            raise
        self.metrics.increment(
            "outbox_admin_actions_total",
            action=action,
            result=result,
        )


def status_summary(events: Sequence[AdminEventMetadata]) -> StatusSummary:
    counts = dict.fromkeys(OutboxStatus, 0)
    for event in events:
        counts[event.status] += 1
    return StatusSummary(
        pending=counts[OutboxStatus.PENDING],
        in_flight=counts[OutboxStatus.IN_FLIGHT],
        sent=counts[OutboxStatus.SENT],
        failed=counts[OutboxStatus.FAILED],
    )


def _metric_key(
    name: str,
    labels: dict[str, str],
) -> tuple[str, tuple[tuple[str, str], ...]]:
    return name, tuple(sorted(labels.items()))


def _format_prometheus_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    rendered = ",".join(
        f'{name}="{_escape_prometheus_label_value(value)}"' for name, value in labels
    )
    return f"{{{rendered}}}"


def _escape_prometheus_label_value(value: str) -> str:
    escaped: list[str] = []
    for char in value:
        if char == "\\":
            escaped.append("\\\\")
        elif char == "\n":
            escaped.append("\\n")
        elif char == "\r":
            escaped.append("\\r")
        elif char == '"':
            escaped.append('\\"')
        elif _is_c0_control(char):
            escaped.append(f"\\x{ord(char):02x}")
        else:
            escaped.append(char)
    return "".join(escaped)


def _is_c0_control(char: str) -> bool:
    codepoint = ord(char)
    return codepoint < 0x20 or codepoint == 0x7F


def _format_metric_value(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return str(value)
