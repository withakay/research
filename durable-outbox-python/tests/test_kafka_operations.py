from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from threading import get_ident

import pytest

from durable_outbox.core import (
    AdminActionStatus,
    ConfigurationError,
    NonRetryablePublishError,
    RetryablePublishError,
    ValidationError,
)
from durable_outbox.core.model import OutboxEvent, OutboxStatus, PublishResult
from durable_outbox.operations import (
    AdminEventMetadata,
    AdminService,
    AuditRecord,
    StatusSummary,
)
from durable_outbox.sinks.kafka import KafkaProducerConfig, KafkaSink
from durable_outbox.telemetry import InMemoryMetrics


@dataclass(frozen=True, slots=True)
class ProduceCall:
    topic: str
    key: bytes | None
    value: bytes
    headers: list[tuple[str, bytes]]


class Message:
    def partition(self) -> int:
        return 3

    def offset(self) -> int:
        return 42


class Producer:
    def __init__(
        self,
        *,
        delivery_error: object | None = None,
        deliver_after_polls: int = 0,
        never_deliver: bool = False,
    ) -> None:
        self.calls: list[ProduceCall] = []
        self.polls: list[float] = []
        self.poll_thread_ids: list[int] = []
        self.flushes: list[float] = []
        self.delivery_error = delivery_error
        self.deliver_after_polls = deliver_after_polls
        self.never_deliver = never_deliver
        self._on_delivery: Callable[[object, object], None] | None = None

    def produce(
        self,
        topic: str,
        *,
        key: bytes | None,
        value: bytes,
        headers: list[tuple[str, bytes]],
        on_delivery: Callable[[object, object], None],
    ) -> None:
        self.calls.append(
            ProduceCall(topic=topic, key=key, value=value, headers=headers)
        )
        self._on_delivery = on_delivery
        if self.deliver_after_polls == 0 and not self.never_deliver:
            self._on_delivery = None
            on_delivery(self.delivery_error, Message())

    def poll(self, timeout: float) -> None:
        self.polls.append(timeout)
        self.poll_thread_ids.append(get_ident())
        if (
            self._on_delivery is not None
            and len(self.polls) >= self.deliver_after_polls
            and not self.never_deliver
        ):
            on_delivery = self._on_delivery
            self._on_delivery = None
            on_delivery(self.delivery_error, Message())

    def flush(self, timeout: float) -> int:
        self.flushes.append(timeout)
        return 0


class KafkaError:
    def __init__(
        self,
        message: str,
        *,
        retriable: bool = False,
        name: str = "UNKNOWN",
    ) -> None:
        self.message = message
        self._retriable = retriable
        self._name = name

    def retriable(self) -> bool:
        return self._retriable

    def name(self) -> str:
        return self._name

    def __str__(self) -> str:
        return self.message


class ProtocolAdminAdapter:
    def __init__(self, events: list[AdminEventMetadata]) -> None:
        self.events = events
        self.repaired_event_ids: list[str] = []
        self.replayed_event_ids: list[str] = []

    async def list_event_metadata(self) -> list[AdminEventMetadata]:
        return self.events

    async def repair_failed_to_pending(self, *, event_id: str) -> AdminActionStatus:
        self.repaired_event_ids.append(event_id)
        self.events = [
            replace(event, status=OutboxStatus.PENDING, last_error_type=None)
            if event.event_id == event_id
            else event
            for event in self.events
        ]
        return AdminActionStatus.SUCCESS

    async def replay_event(self, *, event_id: str) -> AdminActionStatus:
        self.replayed_event_ids.append(event_id)
        return AdminActionStatus.SUCCESS


class CollectingAuditSink:
    def __init__(self) -> None:
        self.records: list[AuditRecord] = []

    async def record(self, record: AuditRecord) -> None:
        self.records.append(record)


class FixedClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def utcnow(self) -> datetime:
        return self.now


def make_event(
    event_id: str = "event-1",
    *,
    headers: dict[str, bytes] | None = None,
) -> OutboxEvent:
    now = datetime.now(UTC)
    return OutboxEvent(
        event_id=event_id,
        topic="durable.outbox.outputs",
        payload=b'{"ok":true}',
        key=b"model-run-1",
        headers=headers or {"content-type": b"application/json"},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
    )


def test_kafka_config_rejects_unsafe_certified_settings() -> None:
    with pytest.raises(ConfigurationError):
        KafkaProducerConfig({"acks": "1"}).validated()


def test_kafka_config_rejects_plaintext_in_certified_mode() -> None:
    with pytest.raises(ConfigurationError, match=r"security\.protocol"):
        KafkaProducerConfig({"security.protocol": "PLAINTEXT"}).validated()


def test_kafka_config_allows_plaintext_when_not_certified() -> None:
    config = KafkaProducerConfig(
        {"security.protocol": "PLAINTEXT"},
        certified_mode=False,
    ).validated()

    assert config["security.protocol"] == "PLAINTEXT"


def test_outbox_event_rejects_sensitive_headers() -> None:
    with pytest.raises(ValidationError, match="blocked"):
        make_event(headers={"Authorization": b"Bearer secret"})


@pytest.mark.asyncio
async def test_kafka_sink_returns_result_after_ack_and_adds_event_id_header() -> None:
    producer = Producer()
    sink = KafkaSink(producer=producer)
    event = make_event()

    result = await sink.publish(event)

    assert result == PublishResult(
        partition=3,
        offset=42,
        published_at=result.published_at,
    )
    assert ("event_id", event.event_id.encode()) in producer.calls[0].headers


@pytest.mark.asyncio
async def test_kafka_sink_polls_until_delivery_ack() -> None:
    producer = Producer(deliver_after_polls=3)
    sink = KafkaSink(producer=producer, poll_interval_seconds=0)

    result = await sink.publish(make_event())

    assert result.partition == 3
    assert len(producer.polls) == 3


@pytest.mark.asyncio
async def test_kafka_sink_poll_does_not_run_on_event_loop_thread() -> None:
    producer = Producer(deliver_after_polls=1)
    sink = KafkaSink(producer=producer, poll_interval_seconds=0)
    event_loop_thread_id = get_ident()

    await sink.publish(make_event())

    assert producer.poll_thread_ids
    assert all(
        thread_id != event_loop_thread_id for thread_id in producer.poll_thread_ids
    )


@pytest.mark.asyncio
async def test_kafka_sink_delivery_timeout_is_retryable() -> None:
    producer = Producer(never_deliver=True)
    sink = KafkaSink(
        producer=producer,
        delivery_timeout_seconds=0,
        poll_interval_seconds=0,
    )

    with pytest.raises(RetryablePublishError, match="timed out"):
        await sink.publish(make_event())

    assert producer.polls


def test_kafka_sink_close_flushes_producer_with_bounded_timeout() -> None:
    producer = Producer()
    sink = KafkaSink(producer=producer, close_timeout_seconds=7.5)

    sink.close()

    assert producer.flushes == [7.5]


def test_kafka_sink_from_config_uses_real_producer_factory_hook() -> None:
    constructed: list[dict[str, object]] = []

    def producer_factory(config: dict[str, object]) -> Producer:
        constructed.append(config)
        return Producer()

    sink = KafkaSink.from_config(
        KafkaProducerConfig(
            {
                "bootstrap.servers": "localhost:9092",
                "security.protocol": "SASL_SSL",
            }
        ),
        producer_factory=producer_factory,
    )

    assert isinstance(sink.producer, Producer)
    assert constructed == [
        {
            "acks": "all",
            "enable.idempotence": True,
            "retries": 2_147_483_647,
            "max.in.flight.requests.per.connection": 5,
            "compression.type": "zstd",
            "linger.ms": 5,
            "bootstrap.servers": "localhost:9092",
            "security.protocol": "SASL_SSL",
        }
    ]


@pytest.mark.asyncio
async def test_kafka_sink_classifies_retryable_kafka_errors() -> None:
    producer = Producer(
        delivery_error=KafkaError("broker unavailable", retriable=True),
    )
    sink = KafkaSink(producer=producer)

    with pytest.raises(RetryablePublishError, match="broker unavailable"):
        await sink.publish(make_event())


@pytest.mark.asyncio
async def test_kafka_sink_classifies_authorization_errors_as_non_retryable() -> None:
    producer = Producer(
        delivery_error=KafkaError(
            "topic authorization failed",
            name="TOPIC_AUTHORIZATION_FAILED",
        ),
    )
    sink = KafkaSink(producer=producer)

    with pytest.raises(NonRetryablePublishError, match="topic authorization failed"):
        await sink.publish(make_event())


@pytest.mark.asyncio
async def test_kafka_sink_preserves_trace_headers_and_adds_event_identity() -> None:
    producer = Producer()
    sink = KafkaSink(producer=producer)
    event = make_event(headers={"traceparent": b"00-abc", "x-app": b"kept"})

    await sink.publish(event)

    assert producer.calls[0].headers == [
        ("traceparent", b"00-abc"),
        ("x-app", b"kept"),
        ("event_id", event.event_id.encode()),
    ]


@pytest.mark.asyncio
async def test_admin_service_reports_and_repairs_failed_events() -> None:
    occurred_at = datetime(2026, 5, 21, tzinfo=UTC)
    adapter = ProtocolAdminAdapter(
        [
            AdminEventMetadata(
                event_id="event-1",
                topic="durable.outbox.outputs",
                status=OutboxStatus.FAILED,
                created_at=occurred_at,
                expires_at=occurred_at + timedelta(minutes=15),
                attempt_count=1,
                last_error_type="NonRetryablePublishError",
            )
        ]
    )
    service = AdminService(status_reader=adapter, admin_actions=adapter)

    assert await service.status() == StatusSummary(
        pending=0,
        in_flight=0,
        sent=0,
        failed=1,
    )
    await service.repair_failed(
        event_id="event-1",
        operator="ops@example.test",
        reason="bad topic fixed",
    )
    assert (await service.status()).pending == 1


@pytest.mark.asyncio
async def test_admin_service_uses_protocols_and_audits_without_payload_bytes() -> None:
    payload = b"secret-payload"
    occurred_at = datetime(2026, 5, 21, tzinfo=UTC)
    adapter = ProtocolAdminAdapter(
        [
            AdminEventMetadata(
                event_id="event-1",
                topic="payments",
                status=OutboxStatus.FAILED,
                created_at=occurred_at,
                expires_at=occurred_at + timedelta(minutes=15),
                attempt_count=2,
                last_error_type="NonRetryablePublishError",
            )
        ]
    )
    audit_sink = CollectingAuditSink()
    metrics = InMemoryMetrics()
    service = AdminService(
        status_reader=adapter,
        admin_actions=adapter,
        audit_sink=audit_sink,
        metrics=metrics,
        clock=FixedClock(occurred_at),
    )

    events = await service.events()
    await service.repair_failed(
        event_id="event-1",
        operator="ops@example.test",
        reason="fixed bad route",
    )
    await service.manual_replay(
        event_id="event-1",
        operator="ops@example.test",
        reason="verify downstream",
    )

    audit_text = repr([asdict(record) for record in audit_sink.records])
    event_text = repr([asdict(event) for event in events])
    assert payload.decode() not in audit_text
    assert payload.decode() not in event_text
    assert audit_sink.records == [
        AuditRecord(
            action="repair_failed",
            event_id="event-1",
            operator="ops@example.test",
            reason="fixed bad route",
            occurred_at=occurred_at,
        ),
        AuditRecord(
            action="manual_replay",
            event_id="event-1",
            operator="ops@example.test",
            reason="verify downstream",
            occurred_at=occurred_at,
        ),
    ]
    assert adapter.repaired_event_ids == ["event-1"]
    assert adapter.replayed_event_ids == ["event-1"]
    assert (
        metrics.counts[
            (
                "outbox_admin_actions_total",
                (("action", "repair_failed"), ("result", "success")),
            )
        ]
        == 1
    )
    assert (
        metrics.counts[
            (
                "outbox_admin_actions_total",
                (("action", "manual_replay"), ("result", "success")),
            )
        ]
        == 1
    )


def test_admin_event_metadata_has_no_payload_field() -> None:
    fields = set(AdminEventMetadata.__dataclass_fields__)

    assert "payload" not in fields
    assert "payload_bytes" not in fields
