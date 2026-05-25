from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from durable_outbox.core.failover import FailoverReplayer
from durable_outbox.core.model import (
    OutboxEvent,
    OutboxStatus,
    PublishingMode,
    PublishResult,
)
from durable_outbox.core.ordering import InMemoryOrderingLockBackend
from durable_outbox.stores.blob_geo import (
    BlobOutboxStore,
    ordering_lock_blob_name,
)
from durable_outbox.stores.cosmos import CosmosConfiguration, CosmosStrongOutboxStore
from durable_outbox.stores.sql import AzureSqlSyncOutboxStore, SqlAlwaysOnOutboxStore
from durable_outbox.testing import FakeOutboxStore, FakeSink
from durable_outbox.testing.provider_contract import make_event


class FailingSink(FakeSink):
    async def publish(self, event: OutboxEvent) -> PublishResult:
        raise RuntimeError(f"publish failed for {event.event_id}")


@pytest.mark.asyncio
async def test_failover_replay_uses_failover_started_at_not_now() -> None:
    store = FakeOutboxStore()
    sink = FakeSink()
    failover_started_at = datetime.now(UTC)
    event = make_event("sent-event")
    await store.put(event)
    claim = (await store.claim_batch(limit=1))[0]
    await store.mark_sent(
        claim,
        await sink.publish(event),
    )

    summary = await FailoverReplayer(store, sink).replay_once(
        failover_started_at=failover_started_at,
        limit=10,
    )

    assert summary.replayed == 1
    assert len(sink.published) == 2
    assert store.cleanup_frozen is True


@pytest.mark.parametrize(
    "status",
    [OutboxStatus.PENDING, OutboxStatus.IN_FLIGHT, OutboxStatus.SENT],
)
@pytest.mark.asyncio
async def test_failover_replay_selects_live_pending_in_flight_and_sent_records(
    status: OutboxStatus,
) -> None:
    store = FakeOutboxStore()
    sink = FakeSink()
    failover_started_at = datetime.now(UTC)
    event = replace(
        make_event(status.value.lower()),
        created_at=failover_started_at - timedelta(minutes=1),
        expires_at=failover_started_at,
    )
    await store.put(event)
    if status is OutboxStatus.IN_FLIGHT:
        await store.claim_batch(limit=1)
    elif status is OutboxStatus.SENT:
        claim = (await store.claim_batch(limit=1))[0]
        await store.mark_sent(claim, await sink.publish(event))

    summary = await FailoverReplayer(store, sink).replay_once(
        failover_started_at=failover_started_at,
        limit=10,
    )

    assert summary.replayed == 1


@pytest.mark.asyncio
async def test_failover_replay_excludes_expired_records() -> None:
    store = FakeOutboxStore()
    failover_started_at = datetime.now(UTC)
    event = replace(
        make_event("expired-before-failover"),
        created_at=failover_started_at - timedelta(minutes=10),
        expires_at=failover_started_at - timedelta(seconds=1),
    )
    await store.put(event)

    summary = await FailoverReplayer(store, FakeSink()).replay_once(
        failover_started_at=failover_started_at,
        limit=10,
    )

    assert summary.replayed == 0


@pytest.mark.asyncio
async def test_cleanup_skips_deletion_while_frozen() -> None:
    store = FakeOutboxStore()
    sink = FakeSink()
    event = make_event("expired-event")
    await store.put(event)
    claim = (await store.claim_batch(limit=1))[0]
    await store.mark_sent(claim, await sink.publish(event))
    await store.freeze_cleanup(reason="failover")

    deleted = await store.cleanup_sent(
        now=datetime.now(UTC) + timedelta(hours=1),
        safety_margin=timedelta(seconds=0),
    )

    assert deleted == 0
    assert event.event_id in store.records


@pytest.mark.asyncio
async def test_cleanup_freeze_survives_replay_failure_until_completion() -> None:
    store = FakeOutboxStore()
    event = make_event("failing-replay")
    await store.put(event)

    with pytest.raises(RuntimeError, match="publish failed"):
        await FailoverReplayer(store, FailingSink()).replay_once(
            failover_started_at=datetime.now(UTC),
            limit=10,
        )

    assert store.cleanup_frozen is True
    await FailoverReplayer(store, FakeSink()).complete_replay()
    assert store.cleanup_frozen is False


@pytest.mark.asyncio
async def test_ordered_claiming_blocks_later_same_key_but_allows_different_keys() -> (
    None
):
    store = FakeOutboxStore()
    first = replace(
        make_event("first", ordering_key="customer-1"),
        publishing_mode=PublishingMode.ORDERED,
        ordering_sequence=1,
    )
    second = replace(
        make_event("second", ordering_key="customer-1"),
        publishing_mode=PublishingMode.ORDERED,
        ordering_sequence=2,
    )
    other = replace(
        make_event("other", ordering_key="customer-2"),
        publishing_mode=PublishingMode.ORDERED,
        ordering_sequence=1,
    )
    await store.put(first)
    await store.put(second)
    await store.put(other)

    claimed = await store.claim_batch(limit=10)

    assert {claim.event.event_id for claim in claimed} == {"first", "other"}
    assert store.records["second"].status is OutboxStatus.PENDING


@pytest.mark.asyncio
async def test_in_memory_ordering_lock_backend_expires_stale_leases() -> None:
    backend = InMemoryOrderingLockBackend()
    now = datetime.now(UTC)
    lease = await backend.acquire(
        lock_name="lock",
        owner_token="first",
        now=now,
        lease_duration=timedelta(seconds=5),
    )

    blocked = await backend.acquire(
        lock_name="lock",
        owner_token="second",
        now=now + timedelta(seconds=1),
        lease_duration=timedelta(seconds=5),
    )
    recovered = await backend.acquire(
        lock_name="lock",
        owner_token="second",
        now=now + timedelta(seconds=6),
        lease_duration=timedelta(seconds=5),
    )

    assert lease is not None
    assert blocked is None
    assert recovered is not None
    assert recovered.owner_token == "second"


@pytest.mark.asyncio
async def test_blob_ordering_releases_lock_after_successful_publish() -> None:
    store = BlobOutboxStore(environment="prod")
    first = replace(
        make_event("first", ordering_key="customer-1"),
        publishing_mode=PublishingMode.ORDERED,
        ordering_sequence=1,
    )
    second = replace(
        make_event("second", ordering_key="customer-1"),
        publishing_mode=PublishingMode.ORDERED,
        ordering_sequence=2,
    )
    await store.put(first)
    await store.put(second)

    claimed = await store.claim_batch(limit=10)
    assert [claim.event.event_id for claim in claimed] == ["first"]

    await store.mark_sent(claimed[0], await FakeSink().publish(first))
    reclaimed = await store.claim_batch(limit=10)

    assert [claim.event.event_id for claim in reclaimed] == ["second"]


@pytest.mark.asyncio
async def test_blob_ordering_lock_scope_includes_topic() -> None:
    store = BlobOutboxStore(environment="prod")
    first = replace(
        make_event("first", ordering_key="shared"),
        publishing_mode=PublishingMode.ORDERED,
        topic="topic-a",
    )
    second = replace(
        make_event("second", ordering_key="shared"),
        publishing_mode=PublishingMode.ORDERED,
        topic="topic-b",
    )
    await store.put(first)
    await store.put(second)

    claimed = await store.claim_batch(limit=10)

    assert {claim.event.event_id for claim in claimed} == {"first", "second"}
    assert ordering_lock_blob_name("prod", "topic-a", "shared") != (
        ordering_lock_blob_name("prod", "topic-b", "shared")
    )


@pytest.mark.parametrize(
    "store",
    [
        FakeOutboxStore(),
        CosmosStrongOutboxStore(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        ),
        AzureSqlSyncOutboxStore(),
        SqlAlwaysOnOutboxStore(),
    ],
)
@pytest.mark.asyncio
async def test_ordering_scope_includes_topic_for_all_ordered_stores(store: Any) -> None:
    first = replace(
        make_event("first", ordering_key="shared"),
        publishing_mode=PublishingMode.ORDERED,
        topic="topic-a",
    )
    second = replace(
        make_event("second", ordering_key="shared"),
        publishing_mode=PublishingMode.ORDERED,
        topic="topic-b",
    )
    await store.put(first)
    await store.put(second)

    claimed = await store.claim_batch(limit=10)

    assert {claim.event.event_id for claim in claimed} == {"first", "second"}


@pytest.mark.asyncio
async def test_blob_ordering_recovers_stale_lock_after_lease_expiry() -> None:
    store = BlobOutboxStore(
        environment="prod",
        claim_timeout=timedelta(seconds=1),
        ordering_lock_lease_duration=timedelta(seconds=0),
    )
    event = replace(
        make_event("stale", ordering_key="customer-1"),
        publishing_mode=PublishingMode.ORDERED,
        ordering_sequence=1,
    )
    await store.put(event)
    first_claim = (await store.claim_batch(limit=1))[0]
    store.records[event.event_id].claimed_at = datetime.now(UTC) - timedelta(seconds=2)
    await store._save_record(store.records[event.event_id])

    recovered = await store.claim_batch(limit=1)

    assert first_claim.event.event_id == "stale"
    assert [claim.event.event_id for claim in recovered] == ["stale"]
