import asyncio
from collections.abc import MutableMapping
from datetime import UTC, datetime, timedelta
from random import Random
from types import MappingProxyType
from typing import cast

import pytest

from durable_outbox.core import (
    ClaimConflictError,
    ConfigurationError,
    NonRetryablePublishError,
    OutboxDispatcher,
    OutboxEvent,
    OutboxStatus,
    PublishingMode,
    PublishResult,
    RetryableStoreError,
    RetryPolicy,
    ValidationError,
)
from durable_outbox.core.errors import DuplicateEventConflictError
from durable_outbox.stores.blob_geo import BlobOutboxStore
from durable_outbox.telemetry import InMemoryMetrics
from durable_outbox.testing import FailingSink, FakeOutboxStore, FakeSink, FixedClock
from durable_outbox.testing.failure_injection import FailingStore
from durable_outbox.testing.provider_contract import (
    ProviderContract,
    make_event,
    run_basic_provider_contract,
    run_provider_contract,
)


class ConcurrentSink(FakeSink):
    def __init__(self, *, started: asyncio.Event, release: asyncio.Event) -> None:
        super().__init__()
        self.started = started
        self.release = release
        self.in_flight = 0
        self.max_in_flight = 0

    async def publish(self, event: OutboxEvent) -> PublishResult:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        if self.in_flight >= 2:
            self.started.set()
        await self.release.wait()
        try:
            return await super().publish(event)
        finally:
            self.in_flight -= 1


def test_event_preserves_opaque_payload_and_freezes_headers() -> None:
    now = datetime.now(UTC)
    event = OutboxEvent(
        event_id="event-1",
        topic="topic",
        payload=b"\x00\x01not-json",
        key=None,
        headers={"traceparent": b"abc"},
        created_at=now,
        expires_at=now + timedelta(minutes=1),
    )

    assert event.payload == b"\x00\x01not-json"
    assert event.headers["traceparent"] == b"abc"
    with pytest.raises(TypeError):
        cast(MutableMapping[str, bytes], event.headers)["x"] = b"nope"


def test_event_reuses_already_frozen_headers_after_validation() -> None:
    now = datetime.now(UTC)
    headers = MappingProxyType({"traceparent": b"abc"})

    event = OutboxEvent(
        event_id="event-1",
        topic="topic",
        payload=b"{}",
        key=None,
        headers=headers,
        created_at=now,
        expires_at=now + timedelta(minutes=1),
    )

    assert event.headers is headers


def test_publish_result_reuses_already_frozen_metadata() -> None:
    metadata = MappingProxyType({"broker": "kafka"})
    result = PublishResult(
        partition=1,
        offset=2,
        published_at=datetime.now(UTC),
        metadata=metadata,
    )

    assert result.metadata is metadata


def test_ordered_event_requires_key() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValidationError):
        OutboxEvent(
            event_id="event-1",
            topic="topic",
            payload=b"{}",
            key=None,
            headers={},
            created_at=now,
            expires_at=now + timedelta(minutes=1),
            publishing_mode=PublishingMode.ORDERED,
        )


@pytest.mark.parametrize(
    "topic",
    [
        "",
        "orders\rspoofed_metric 1",
        "orders/created",
        "x" * 250,
    ],
)
def test_event_rejects_invalid_topic_names(topic: str) -> None:
    now = datetime.now(UTC)

    with pytest.raises(ValidationError, match="topic"):
        OutboxEvent(
            event_id="event-1",
            topic=topic,
            payload=b"{}",
            key=None,
            headers={},
            created_at=now,
            expires_at=now + timedelta(minutes=1),
        )


def test_event_rejects_naive_datetimes() -> None:
    now = datetime.now()

    with pytest.raises(ValidationError, match="timezone-aware"):
        OutboxEvent(
            event_id="event-1",
            topic="topic",
            payload=b"{}",
            key=None,
            headers={},
            created_at=now,
            expires_at=now + timedelta(minutes=1),
        )


def test_retry_policy_can_disable_jitter_for_exact_backoff() -> None:
    now = datetime.now(UTC)
    policy = RetryPolicy(
        initial_delay=timedelta(seconds=1),
        max_delay=timedelta(minutes=1),
        jitter=0.0,
    )

    assert policy.next_attempt_at(now, attempt_count=1) == now + timedelta(seconds=1)
    assert policy.next_attempt_at(now, attempt_count=2) == now + timedelta(seconds=2)


def test_retry_policy_jitter_is_seedable_and_bounded() -> None:
    now = datetime.now(UTC)
    first = RetryPolicy(
        initial_delay=timedelta(seconds=10),
        max_delay=timedelta(minutes=1),
        jitter=0.5,
        random=Random(42),
    )
    second = RetryPolicy(
        initial_delay=timedelta(seconds=10),
        max_delay=timedelta(minutes=1),
        jitter=0.5,
        random=Random(42),
    )
    different = RetryPolicy(
        initial_delay=timedelta(seconds=10),
        max_delay=timedelta(minutes=1),
        jitter=0.5,
        random=Random(7),
    )

    first_attempt = first.next_attempt_at(now, attempt_count=1)
    assert first_attempt == second.next_attempt_at(now, attempt_count=1)
    assert first_attempt != different.next_attempt_at(now, attempt_count=1)
    assert now + timedelta(seconds=5) <= first_attempt <= now + timedelta(seconds=15)


@pytest.mark.parametrize("jitter", [-0.1, 1.1])
def test_retry_policy_rejects_invalid_jitter(jitter: float) -> None:
    with pytest.raises(ValueError, match="jitter"):
        RetryPolicy(jitter=jitter)


def test_event_rejects_too_many_headers() -> None:
    now = datetime.now(UTC)

    with pytest.raises(ValidationError, match="headers"):
        OutboxEvent(
            event_id="event-1",
            topic="topic",
            payload=b"{}",
            key=None,
            headers={f"x-{index}": b"value" for index in range(65)},
            created_at=now,
            expires_at=now + timedelta(minutes=1),
        )


def test_event_rejects_oversized_header_value() -> None:
    now = datetime.now(UTC)

    with pytest.raises(ValidationError, match="header"):
        OutboxEvent(
            event_id="event-1",
            topic="topic",
            payload=b"{}",
            key=None,
            headers={"x-large": b"x" * 8193},
            created_at=now,
            expires_at=now + timedelta(minutes=1),
        )


def test_event_rejects_oversized_header_name() -> None:
    now = datetime.now(UTC)

    with pytest.raises(ValidationError, match="header name"):
        OutboxEvent(
            event_id="event-1",
            topic="topic",
            payload=b"{}",
            key=None,
            headers={"x" * 257: b"value"},
            created_at=now,
            expires_at=now + timedelta(minutes=1),
        )


def test_event_rejects_oversized_header_total() -> None:
    now = datetime.now(UTC)

    with pytest.raises(ValidationError, match="total bytes"):
        OutboxEvent(
            event_id="event-1",
            topic="topic",
            payload=b"{}",
            key=None,
            headers={f"x-{index}": b"x" * 2048 for index in range(33)},
            created_at=now,
            expires_at=now + timedelta(minutes=1),
        )


def test_public_error_exports_are_available_from_core_package() -> None:
    assert ClaimConflictError.__name__ == "ClaimConflictError"
    assert RetryableStoreError.__name__ == "RetryableStoreError"


@pytest.mark.asyncio
async def test_basic_provider_contract_passes_for_fake_store() -> None:
    await run_basic_provider_contract(FakeOutboxStore)


@pytest.mark.asyncio
async def test_provider_contract_accepts_protocol_contract() -> None:
    await run_provider_contract(ProviderContract(store_factory=FakeOutboxStore))


@pytest.mark.asyncio
async def test_duplicate_put_rejects_incompatible_event_envelope() -> None:
    store = FakeOutboxStore()
    event = make_event("same-id")
    incompatible = make_event("same-id")
    incompatible = type(incompatible)(
        event_id=incompatible.event_id,
        topic="different-topic",
        payload=incompatible.payload,
        key=incompatible.key,
        headers=incompatible.headers,
        created_at=incompatible.created_at,
        expires_at=incompatible.expires_at,
    )

    await store.put(event)
    with pytest.raises(DuplicateEventConflictError):
        await store.put(incompatible)


@pytest.mark.asyncio
async def test_claim_batch_rejects_non_positive_limit() -> None:
    store = FakeOutboxStore()

    with pytest.raises(ValidationError, match="limit"):
        await store.claim_batch(limit=0)


@pytest.mark.asyncio
async def test_claim_batch_rejects_excessive_limit() -> None:
    store = FakeOutboxStore()

    with pytest.raises(ValidationError, match="limit"):
        await store.claim_batch(limit=1001)


@pytest.mark.asyncio
async def test_dispatcher_marks_sent_after_sink_ack() -> None:
    store = FakeOutboxStore()
    event = make_event()
    await store.put(event)

    summary = await OutboxDispatcher(store, FakeSink()).run_once(limit=10)

    assert summary.sent == 1
    assert store.records[event.event_id].status is OutboxStatus.SENT


@pytest.mark.asyncio
async def test_dispatcher_publishes_claimed_events_concurrently() -> None:
    store = FakeOutboxStore()
    for index in range(3):
        await store.put(make_event(f"event-{index}"))
    started = asyncio.Event()
    release = asyncio.Event()
    sink = ConcurrentSink(started=started, release=release)
    dispatch_task = asyncio.create_task(
        OutboxDispatcher(store, sink, concurrency=3).run_once(limit=3)
    )

    await asyncio.wait_for(started.wait(), timeout=1)
    release.set()
    summary = await dispatch_task

    assert summary.claimed == 3
    assert summary.sent == 3
    assert sink.max_in_flight >= 2


def test_dispatcher_can_require_rpo_zero_store() -> None:
    with pytest.raises(ConfigurationError, match="RPO=0"):
        OutboxDispatcher(
            BlobOutboxStore.for_testing(), FakeSink(), require_rpo_zero=True
        )


@pytest.mark.asyncio
async def test_dispatcher_returns_retryable_failure_to_pending() -> None:
    store = FakeOutboxStore()
    event = make_event()
    await store.put(event)

    summary = await OutboxDispatcher(
        store,
        FailingSink(errors=[TimeoutError("broker timeout")]),
        retry_policy=RetryPolicy(initial_delay=timedelta(seconds=3)),
    ).run_once(limit=10)

    record = store.records[event.event_id]
    assert summary.retried == 1
    assert record.status is OutboxStatus.PENDING
    assert record.next_attempt_at is not None
    assert record.last_error_type == "TimeoutError"


@pytest.mark.asyncio
async def test_dispatcher_truncates_retryable_error_message_before_store_update() -> (
    None
):
    store = FakeOutboxStore()
    event = make_event()
    await store.put(event)
    metrics = InMemoryMetrics()
    long_error = "broker unavailable: " + ("x" * 1500)

    summary = await OutboxDispatcher(
        store,
        FailingSink(errors=[TimeoutError(long_error)]),
        metrics=metrics,
    ).run_once(limit=10)

    record = store.records[event.event_id]
    assert summary.retried == 1
    assert record.last_error is not None
    assert len(record.last_error.encode()) <= 512
    assert record.last_error.endswith("...[truncated]")
    assert (
        metrics.counts[
            (
                "outbox_error_messages_truncated_total",
                (
                    ("error_type", "TimeoutError"),
                    ("topic", event.topic),
                ),
            )
        ]
        == 1
    )


@pytest.mark.asyncio
async def test_dispatcher_observes_retry_state_update_failure() -> None:
    inner = FakeOutboxStore()
    event = make_event()
    await inner.put(event)
    store = FailingStore(
        inner,
        mark_retry_errors=[RuntimeError("retry update failed")],
    )
    metrics = InMemoryMetrics()

    summary = await OutboxDispatcher(
        store,
        FailingSink(errors=[TimeoutError("broker timeout")]),
        metrics=metrics,
    ).run_once(limit=10)

    record = inner.records[event.event_id]
    assert summary.retried == 0
    assert summary.store_update_failed == 1
    assert record.status is OutboxStatus.IN_FLIGHT
    assert (
        metrics.counts[
            (
                "outbox_store_update_failures_total",
                (
                    ("error_type", "RuntimeError"),
                    ("operation", "mark_pending_after_retryable_failure"),
                    ("topic", event.topic),
                ),
            )
        ]
        == 1
    )


@pytest.mark.asyncio
async def test_dispatcher_observes_failed_state_update_failure() -> None:
    inner = FakeOutboxStore()
    event = make_event()
    await inner.put(event)
    store = FailingStore(
        inner,
        mark_failed_errors=[RuntimeError("failed update failed")],
    )
    metrics = InMemoryMetrics()

    summary = await OutboxDispatcher(
        store,
        FailingSink(errors=[NonRetryablePublishError("unknown topic")]),
        metrics=metrics,
    ).run_once(limit=10)

    record = inner.records[event.event_id]
    assert summary.failed == 0
    assert summary.store_update_failed == 1
    assert record.status is OutboxStatus.IN_FLIGHT
    assert (
        metrics.counts[
            (
                "outbox_store_update_failures_total",
                (
                    ("error_type", "RuntimeError"),
                    ("operation", "mark_failed"),
                    ("topic", event.topic),
                ),
            )
        ]
        == 1
    )


@pytest.mark.asyncio
async def test_dispatcher_uses_claim_attempt_count_for_retry_backoff() -> None:
    store = FakeOutboxStore()
    event = make_event()
    await store.put(event)
    now = datetime.now(UTC)
    dispatcher = OutboxDispatcher(
        store,
        FailingSink(errors=[TimeoutError("first"), TimeoutError("second")]),
        clock=FixedClock(now),
        retry_policy=RetryPolicy(
            initial_delay=timedelta(seconds=1),
            max_delay=timedelta(minutes=1),
            jitter=0.0,
        ),
    )

    await dispatcher.run_once(limit=10)
    first_retry = store.records[event.event_id].next_attempt_at
    store.records[event.event_id].next_attempt_at = now - timedelta(seconds=1)
    await dispatcher.run_once(limit=10)
    second_retry = store.records[event.event_id].next_attempt_at

    assert first_retry == now + timedelta(seconds=1)
    assert second_retry == now + timedelta(seconds=2)


@pytest.mark.asyncio
async def test_dispatcher_marks_non_retryable_failure_failed() -> None:
    store = FakeOutboxStore()
    event = make_event()
    await store.put(event)

    summary = await OutboxDispatcher(
        store,
        FailingSink(errors=[NonRetryablePublishError("unknown topic")]),
    ).run_once(limit=10)

    assert summary.failed == 1
    assert store.records[event.event_id].status is OutboxStatus.FAILED


@pytest.mark.asyncio
async def test_dispatcher_metrics_are_sink_agnostic() -> None:
    store = FakeOutboxStore()
    event = make_event()
    await store.put(event)
    metrics = InMemoryMetrics()

    await OutboxDispatcher(store, FakeSink(), metrics=metrics).run_once(limit=10)

    assert (
        metrics.counts[("outbox_publish_attempts_total", (("topic", event.topic),))]
        == 1
    )
    assert (
        metrics.counts[("outbox_publish_success_total", (("topic", event.topic),))] == 1
    )
    assert not any(name.startswith("kafka_") for name, _ in metrics.counts)


@pytest.mark.asyncio
async def test_dispatcher_does_not_mark_pending_after_post_ack_store_failure() -> None:
    inner = FakeOutboxStore()
    event = make_event()
    await inner.put(event)
    store = FailingStore(
        inner,
        mark_sent_errors=[RuntimeError("store unavailable after ack")],
    )
    metrics = InMemoryMetrics()

    summary = await OutboxDispatcher(store, FakeSink(), metrics=metrics).run_once(
        limit=10
    )

    record = inner.records[event.event_id]
    assert summary.sent == 0
    assert summary.store_update_failed == 1
    assert record.status is OutboxStatus.IN_FLIGHT
    assert record.last_error_type is None
    assert (
        metrics.counts[
            (
                "outbox_mark_sent_failures_total",
                (
                    ("error_type", "RuntimeError"),
                    ("topic", event.topic),
                ),
            )
        ]
        == 1
    )
