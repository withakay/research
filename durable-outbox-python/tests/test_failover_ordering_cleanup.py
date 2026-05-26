from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest
from durable_outbox_sql_store import (
    AzureSqlSyncOutboxStore,
    InMemorySqlOutboxClient,
    SqlAlwaysOnOutboxStore,
)

from durable_outbox.core import ConfigurationError
from durable_outbox.core.failover import FailoverReplayer
from durable_outbox.core.model import (
    OutboxStatus,
    PublishingMode,
)
from durable_outbox.core.ordering import InMemoryOrderingLockBackend
from durable_outbox.stores.blob_geo import (
    BlobOutboxStore,
    InMemoryBlobClient,
    ordering_lock_blob_name,
)
from durable_outbox.stores.cosmos import (
    CosmosConfiguration,
    CosmosStoredEvent,
    CosmosStrongOutboxStore,
    InMemoryCosmosOutboxClient,
)
from durable_outbox.telemetry import InMemoryMetrics
from durable_outbox.testing import FailingSink, FakeOutboxStore, FakeSink, FixedClock
from durable_outbox.testing.provider_contract import make_event

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Collection

    from durable_outbox.core.model import ClaimedEvent


class ConcurrentReplaySink(FakeSink):
    def __init__(self, *, release: asyncio.Event) -> None:
        super().__init__()
        self.release = release
        self.in_flight = 0
        self.max_in_flight = 0

    async def publish(self, event: Any) -> Any:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        await self.release.wait()
        try:
            return await super().publish(event)
        finally:
            self.in_flight -= 1


class RecordingReplayStore(FakeOutboxStore):
    def __init__(self) -> None:
        super().__init__()
        self.replay_limits: list[int] = []
        self.replay_exclusions: list[tuple[str, ...]] = []

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> Any:
        self.replay_limits.append(limit)
        self.replay_exclusions.append(tuple(sorted(exclude_event_ids)))
        return await super().failover_replay_candidates(
            failover_started_at=failover_started_at,
            limit=limit,
            exclude_event_ids=exclude_event_ids,
        )


class StreamingReplayStore(FakeOutboxStore):
    def __init__(self) -> None:
        super().__init__()
        self.stream_calls = 0
        self.legacy_calls = 0

    async def iter_failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
    ) -> AsyncIterator[ClaimedEvent]:
        self.stream_calls += 1
        seen_event_ids: set[str] = set()
        for _ in range(limit):
            page = await FakeOutboxStore.failover_replay_candidates(
                self,
                failover_started_at=failover_started_at,
                limit=1,
                exclude_event_ids=seen_event_ids,
            )
            if not page:
                break
            seen_event_ids.add(page[0].event.event_id)
            yield page[0]

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> Any:
        _ = failover_started_at, limit, exclude_event_ids
        self.legacy_calls += 1
        raise AssertionError("streaming replay should not use list candidates")


class StreamingSqlStore(AzureSqlSyncOutboxStore):
    def __init__(self) -> None:
        super().__init__(client=InMemorySqlOutboxClient())
        self.legacy_calls = 0

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> Any:
        _ = failover_started_at, limit, exclude_event_ids
        self.legacy_calls += 1
        raise AssertionError("SQL streaming replay should not use list candidates")


class StreamingCosmosStore(CosmosStrongOutboxStore):
    def __init__(self) -> None:
        super().__init__(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
            client=InMemoryCosmosOutboxClient(),
        )
        self.legacy_calls = 0

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> Any:
        _ = failover_started_at, limit, exclude_event_ids
        self.legacy_calls += 1
        raise AssertionError("Cosmos streaming replay should not use list candidates")


class StreamingBlobStore(BlobOutboxStore):
    def __init__(self) -> None:
        super().__init__(client=InMemoryBlobClient(), store_name="StreamingBlobStore")
        self.legacy_calls = 0

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> Any:
        _ = failover_started_at, limit, exclude_event_ids
        self.legacy_calls += 1
        raise AssertionError("Blob streaming replay should not use list candidates")


class ClientStreamingCosmosClient(InMemoryCosmosOutboxClient):
    def __init__(self) -> None:
        super().__init__()
        self.stream_calls = 0

    async def list_failover_replay_candidates(self, **kwargs: object) -> Any:
        _ = kwargs
        raise AssertionError("store should use Cosmos client streaming replay")

    async def iter_failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> AsyncIterator[CosmosStoredEvent]:
        self.stream_calls += 1
        records = await InMemoryCosmosOutboxClient.list_failover_replay_candidates(
            self,
            failover_started_at=failover_started_at,
            limit=limit,
            exclude_event_ids=exclude_event_ids,
        )
        for record in records:
            yield record


def test_failover_replayer_requires_rpo_zero_by_default() -> None:
    with pytest.raises(ConfigurationError, match="RPO=0"):
        FailoverReplayer(BlobOutboxStore.for_testing(), FakeSink())


def test_failover_replayer_can_opt_out_of_rpo_zero_validation() -> None:
    replayer = FailoverReplayer(
        BlobOutboxStore.for_testing(), FakeSink(), require_rpo_zero=False
    )

    assert replayer.store.capabilities.rpo_zero_for_accepted_events is False


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


@pytest.mark.asyncio
async def test_failover_replay_warns_when_republishing_sent_event(
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = FakeOutboxStore()
    sink = FakeSink()
    metrics = InMemoryMetrics()
    event = make_event("sent-replay-warning")
    await store.put(event)
    claim = (await store.claim_batch(limit=1))[0]
    await store.mark_sent(claim, await sink.publish(event))

    with caplog.at_level(logging.WARNING, logger="durable_outbox.core.failover"):
        summary = await FailoverReplayer(store, sink, metrics=metrics).replay_once(
            failover_started_at=datetime.now(UTC),
            limit=10,
        )

    assert summary.replayed == 1
    assert "consumers must dedupe by event_id" in caplog.text
    assert (
        metrics.counts[
            (
                "outbox_failover_sent_replays_total",
                (("topic", event.topic),),
            )
        ]
        == 1
    )


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
async def test_replay_continues_after_publish_failure_and_keeps_cleanup_frozen() -> (
    None
):
    store = FakeOutboxStore()
    sink = FailingSink(errors=[RuntimeError("publish failed for failing-replay")])
    metrics = InMemoryMetrics()
    failing = make_event("failing-replay")
    succeeding = make_event("succeeding-replay")
    await store.put(failing)
    await store.put(succeeding)

    summary = await FailoverReplayer(store, sink, metrics=metrics).replay_once(
        failover_started_at=datetime.now(UTC),
        limit=10,
    )

    assert summary.replayed == 1
    assert summary.errored == 1
    assert store.records["failing-replay"].status is OutboxStatus.IN_FLIGHT
    assert store.records["succeeding-replay"].status is OutboxStatus.SENT
    assert store.cleanup_frozen is True
    assert (
        metrics.counts[
            (
                "outbox_failover_replay_failures_total",
                (
                    ("error_type", "RuntimeError"),
                    ("topic", failing.topic),
                ),
            )
        ]
        == 1
    )
    await FailoverReplayer(store, FakeSink()).complete_replay()
    assert store.cleanup_frozen is False


@pytest.mark.asyncio
async def test_failover_replay_fetches_bounded_pages_without_replaying_seen_events() -> (
    None
):
    store = RecordingReplayStore()
    sink = FakeSink()
    for index in range(5):
        await store.put(make_event(f"paged-replay-{index}"))

    summary = await FailoverReplayer(
        store,
        sink,
        replay_page_size=2,
        max_concurrency=2,
    ).replay_once(
        failover_started_at=datetime.now(UTC),
        limit=5,
    )

    assert summary.replayed == 5
    assert summary.errored == 0
    assert store.replay_limits == [2, 2, 1]
    assert store.replay_exclusions[0] == ()
    assert set(store.replay_exclusions[1]) == {"paged-replay-0", "paged-replay-1"}
    assert set(store.replay_exclusions[2]) == {
        "paged-replay-0",
        "paged-replay-1",
        "paged-replay-2",
        "paged-replay-3",
    }
    assert len({event.event_id for event in sink.published}) == 5


@pytest.mark.asyncio
async def test_failover_replay_publishes_page_concurrently() -> None:
    store = FakeOutboxStore()
    release = asyncio.Event()
    sink = ConcurrentReplaySink(release=release)
    for index in range(3):
        await store.put(make_event(f"concurrent-replay-{index}"))

    replay_task = asyncio.create_task(
        FailoverReplayer(
            store,
            sink,
            replay_page_size=3,
            max_concurrency=2,
        ).replay_once(
            failover_started_at=datetime.now(UTC),
            limit=3,
        )
    )
    while sink.max_in_flight < 2:
        await asyncio.sleep(0)
    release.set()
    summary = await replay_task

    assert summary.replayed == 3
    assert sink.max_in_flight == 2


@pytest.mark.asyncio
async def test_failover_replay_consumes_streaming_store_without_list_pages() -> None:
    store = StreamingReplayStore()
    sink = FakeSink()
    for index in range(5):
        await store.put(make_event(f"streamed-replay-{index}"))

    summary = await FailoverReplayer(
        store,
        sink,
        replay_page_size=2,
        max_concurrency=2,
    ).replay_once(
        failover_started_at=datetime.now(UTC),
        limit=5,
    )

    assert summary.replayed == 5
    assert summary.errored == 0
    assert store.stream_calls == 1
    assert store.legacy_calls == 0
    assert len({event.event_id for event in sink.published}) == 5


@pytest.mark.parametrize("store", [StreamingSqlStore(), StreamingCosmosStore()])
@pytest.mark.asyncio
async def test_sql_and_cosmos_failover_replay_expose_streaming_store(
    store: StreamingSqlStore | StreamingCosmosStore,
) -> None:
    sink = FakeSink()
    for index in range(3):
        await store.put(make_event(f"provider-streamed-replay-{index}"))

    summary = await FailoverReplayer(
        store,
        sink,
        replay_page_size=2,
        max_concurrency=2,
    ).replay_once(
        failover_started_at=datetime.now(UTC),
        limit=3,
    )

    assert summary.replayed == 3
    assert summary.errored == 0
    assert store.legacy_calls == 0
    assert len({event.event_id for event in sink.published}) == 3


@pytest.mark.asyncio
async def test_blob_failover_replay_exposes_streaming_store() -> None:
    store = StreamingBlobStore()
    sink = FakeSink()
    for index in range(3):
        await store.put(make_event(f"blob-streamed-replay-{index}"))

    summary = await FailoverReplayer(
        store,
        sink,
        require_rpo_zero=False,
        replay_page_size=2,
        max_concurrency=2,
    ).replay_once(
        failover_started_at=datetime.now(UTC),
        limit=3,
    )

    assert summary.replayed == 3
    assert summary.errored == 0
    assert store.legacy_calls == 0
    assert len({event.event_id for event in sink.published}) == 3


@pytest.mark.asyncio
async def test_cosmos_streaming_replay_uses_client_iterator() -> None:
    client = ClientStreamingCosmosClient()
    store = CosmosStrongOutboxStore(
        CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
        client=client,
    )
    sink = FakeSink()
    for index in range(3):
        await store.put(make_event(f"cosmos-client-streamed-replay-{index}"))

    summary = await FailoverReplayer(
        store,
        sink,
        replay_page_size=2,
        max_concurrency=2,
    ).replay_once(
        failover_started_at=datetime.now(UTC),
        limit=3,
    )

    assert summary.replayed == 3
    assert summary.errored == 0
    assert client.stream_calls == 1


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
        owner_token="first",  # noqa: S106
        now=now,
        lease_duration=timedelta(seconds=5),
    )

    blocked = await backend.acquire(
        lock_name="lock",
        owner_token="second",  # noqa: S106
        now=now + timedelta(seconds=1),
        lease_duration=timedelta(seconds=5),
    )
    recovered = await backend.acquire(
        lock_name="lock",
        owner_token="second",  # noqa: S106
        now=now + timedelta(seconds=6),
        lease_duration=timedelta(seconds=5),
    )

    assert lease is not None
    assert blocked is None
    assert recovered is not None
    assert recovered.owner_token == "second"  # noqa: S105


@pytest.mark.asyncio
async def test_blob_ordering_releases_lock_after_successful_publish() -> None:
    store = BlobOutboxStore.for_testing(environment="prod")
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
    store = BlobOutboxStore.for_testing(environment="prod")
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
        CosmosStrongOutboxStore.for_testing(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        ),
        AzureSqlSyncOutboxStore.for_testing(),
        SqlAlwaysOnOutboxStore.for_testing(),
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
    clock = FixedClock(datetime.now(UTC))
    store = BlobOutboxStore.for_testing(
        environment="prod",
        claim_timeout=timedelta(seconds=1),
        ordering_lock_lease_duration=timedelta(seconds=1),
        clock=clock,
    )
    event = replace(
        make_event("stale", ordering_key="customer-1"),
        publishing_mode=PublishingMode.ORDERED,
        ordering_sequence=1,
    )
    await store.put(event)
    first_claim = (await store.claim_batch(limit=1))[0]
    clock.now += timedelta(seconds=2)

    recovered = await store.claim_batch(limit=1)

    assert first_claim.event.event_id == "stale"
    assert [claim.event.event_id for claim in recovered] == ["stale"]


@pytest.mark.parametrize(
    "ordering_lock_lease_duration",
    [timedelta(milliseconds=500), timedelta(seconds=2)],
)
def test_blob_ordering_lock_lease_duration_must_match_claim_timeout(
    ordering_lock_lease_duration: timedelta,
) -> None:
    with pytest.raises(ConfigurationError, match="ordering_lock_lease_duration"):
        BlobOutboxStore.for_testing(
            claim_timeout=timedelta(seconds=1),
            ordering_lock_lease_duration=ordering_lock_lease_duration,
        )


@pytest.mark.asyncio
async def test_blob_ordering_lock_blocks_stale_second_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = InMemoryBlobClient()
    first = BlobOutboxStore(client=client, environment="prod")
    second = BlobOutboxStore(client=client, environment="prod")
    first_event = replace(
        make_event("first-shared-lock", ordering_key="customer-1"),
        publishing_mode=PublishingMode.ORDERED,
        ordering_sequence=1,
    )
    second_event = replace(
        make_event("second-shared-lock", ordering_key="customer-1"),
        publishing_mode=PublishingMode.ORDERED,
        ordering_sequence=2,
    )
    await first.put(first_event)
    await first.put(second_event)
    await first._refresh_records()
    await second._refresh_records()

    first_claim = await first.claim_batch(limit=1)
    monkeypatch.setattr(second, "_refresh_records", _noop_refresh)
    stale_second_claim = await second.claim_batch(limit=1)

    assert [claim.event.event_id for claim in first_claim] == ["first-shared-lock"]
    assert stale_second_claim == []


async def _noop_refresh() -> None:
    return None
