from datetime import UTC, datetime

import pytest

from durable_outbox.core import OutboxDispatcher
from durable_outbox.core.errors import RetryableStoreError
from durable_outbox.core.model import OutboxStatus
from durable_outbox.testing import FailingStore, FakeOutboxStore, FakeSink
from durable_outbox.testing.provider_contract import make_event


@pytest.mark.asyncio
async def test_ack_before_mark_sent_failure_leaves_event_replayable() -> None:
    base_store = FakeOutboxStore()
    event = make_event("ack-before-mark-sent")
    await base_store.put(event)
    store = FailingStore(
        base_store,
        mark_sent_errors=[RetryableStoreError("mark_sent timed out")],
    )
    sink = FakeSink()

    summary = await OutboxDispatcher(store, sink).run_once(limit=10)

    assert summary.store_update_failed == 1
    assert len(sink.published) == 1
    assert base_store.records[event.event_id].status is OutboxStatus.IN_FLIGHT
    replay = await base_store.failover_replay_candidates(
        failover_started_at=datetime.now(UTC),
        limit=10,
    )
    assert [claim.event.event_id for claim in replay] == [event.event_id]


@pytest.mark.asyncio
@pytest.mark.load
async def test_fake_store_sustains_mvp_throughput_shape() -> None:
    store = FakeOutboxStore()
    sink = FakeSink()
    for index in range(1_000):
        await store.put(make_event(f"event-{index}"))

    summary = await OutboxDispatcher(store, sink).run_once(limit=1_000)

    assert summary.sent == 1_000
    assert len(sink.published) == 1_000
    assert all(record.status is OutboxStatus.SENT for record in store.records.values())
