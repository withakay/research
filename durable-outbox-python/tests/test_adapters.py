from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from durable_outbox.core import ConfigurationError, ValidationError
from durable_outbox.core.errors import DuplicateEventConflictError, RetryableStoreError
from durable_outbox.core.model import OutboxStatus, PublishResult
from durable_outbox.stores.blob_geo import (
    BlobOutboxStore,
    DualRegionBlobOutboxStore,
    InMemoryBlobClient,
    blob_metadata,
    event_blob_name,
    ordering_lock_blob_name,
)
from durable_outbox.stores.cosmos import (
    CosmosConfiguration,
    CosmosStoredEvent,
    CosmosStrongOutboxStore,
    InMemoryCosmosOutboxClient,
)
from durable_outbox.stores.sql import (
    SQL_ORDERED_INDEX_NAME,
    SQL_PENDING_INDEX_NAME,
    SQL_REPLAY_INDEX_NAME,
    SQL_SCHEMA,
    AzureSqlSyncConfiguration,
    AzureSqlSyncOutboxStore,
    InMemorySqlOutboxClient,
    SqlAlwaysOnOutboxStore,
    SqlStoredEvent,
)
from durable_outbox.testing import FakeOutboxStore
from durable_outbox.testing.provider_contract import make_event


class FixedClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def utcnow(self) -> datetime:
        return self.now


def test_store_package_exports_are_importable() -> None:
    from durable_outbox import stores

    for name in stores.__all__:
        assert getattr(stores, name).__name__ == name


def test_blob_names_are_deterministic_and_do_not_embed_raw_event_id() -> None:
    first = event_blob_name("event/with/slash")
    second = event_blob_name("event/with/slash")

    assert first == second
    assert first.startswith("outbox/v1/events/")
    assert "event/with/slash" not in first


def test_blob_metadata_preserves_envelope_fields() -> None:
    event = make_event("event-1", ordering_key="customer-1")

    metadata = blob_metadata(event, environment="test")

    assert metadata["accepted"] == "true"
    assert metadata["status"] == "PENDING"
    assert metadata["event_id"] == "event-1"
    assert "ordering_key_hash" in metadata


def test_ordering_lock_name_is_deterministic() -> None:
    first = ordering_lock_blob_name("prod", "topic", "key")
    second = ordering_lock_blob_name("prod", "topic", "key")

    assert first == second
    assert first.endswith(".lock")


@pytest.mark.asyncio
async def test_blob_put_is_idempotent_for_compatible_duplicate() -> None:
    store = BlobOutboxStore()
    event = make_event("same-event")

    first = await store.put(event)
    second = await store.put(event)

    assert second.event_id == first.event_id
    assert second.accepted_at == first.accepted_at


@pytest.mark.asyncio
async def test_blob_put_rejects_incompatible_duplicate() -> None:
    store = BlobOutboxStore()
    event = make_event("same-event")
    incompatible = replace(event, topic="other-topic")
    await store.put(event)

    with pytest.raises(DuplicateEventConflictError, match="incompatible"):
        await store.put(incompatible)


@pytest.mark.asyncio
async def test_blob_claim_is_single_winner_with_shared_client() -> None:
    client = InMemoryBlobClient()
    first = BlobOutboxStore(client=client)
    second = BlobOutboxStore(client=client)
    event = make_event("single-winner")
    await first.put(event)
    await first._refresh_records()
    await second._refresh_records()

    first_claim = await first.claim_batch(limit=1)
    second_claim = await second.claim_batch(limit=1)

    assert [claim.event.event_id for claim in first_claim] == ["single-winner"]
    assert second_claim == []


@pytest.mark.asyncio
async def test_dual_region_blob_accepts_only_after_both_regions() -> None:
    store = DualRegionBlobOutboxStore()
    event = make_event()

    receipt = await store.put(event)

    assert receipt.rpo_zero is True
    assert store.primary.records[event.event_id].accepted is True
    assert store.secondary.records[event.event_id].accepted is True


@pytest.mark.asyncio
async def test_dual_region_blob_repair_copies_missing_region() -> None:
    store = DualRegionBlobOutboxStore()
    event = make_event()
    await store._prepare(store.primary, event)
    await store._accept(store.primary, event)

    await store.repair_prepared(event.event_id)

    assert store.secondary.records[event.event_id].accepted is True


@pytest.mark.parametrize(
    ("primary_accepted", "secondary_accepted"),
    [
        (False, None),
        (True, False),
        (True, None),
        (False, False),
    ],
)
@pytest.mark.asyncio
async def test_dual_region_blob_repairs_partial_write_matrix(
    primary_accepted: bool,
    secondary_accepted: bool | None,
) -> None:
    store = DualRegionBlobOutboxStore()
    event = make_event()
    await store._prepare(store.primary, event)
    if primary_accepted:
        await store._accept(store.primary, event)
    if secondary_accepted is not None:
        await store._prepare(store.secondary, event)
    if secondary_accepted:
        await store._accept(store.secondary, event)

    await store.repair_prepared(event.event_id)
    await store.repair_prepared(event.event_id)

    assert store.primary.records[event.event_id].accepted is True
    assert store.secondary.records[event.event_id].accepted is True


@pytest.mark.asyncio
async def test_dual_region_blob_prepared_records_are_hidden_from_claims() -> None:
    store = DualRegionBlobOutboxStore()
    event = make_event()
    await store._prepare(store.primary, event)
    store.records = store.primary.records

    assert await store.claim_batch(limit=10) == []


@pytest.mark.asyncio
async def test_blob_store_uses_injected_clock_for_lifecycle_timestamps() -> None:
    occurred_at = datetime(2026, 5, 22, 9, 30, tzinfo=UTC)
    clock = FixedClock(occurred_at)
    store = BlobOutboxStore(clock=clock)
    event = make_event("clocked")

    receipt = await store.put(event)
    claimed = (await store.claim_batch(limit=1))[0]
    claimed_at = store.records[event.event_id].claimed_at
    await store.mark_failed(
        claimed,
        error_type="Fatal",
        error_message="stop",
    )

    assert receipt.accepted_at == occurred_at
    assert claimed_at == occurred_at
    assert store.records[event.event_id].failed_at == occurred_at


def test_cosmos_rpo_zero_validation_rejects_session_consistency() -> None:
    with pytest.raises(ConfigurationError):
        CosmosStrongOutboxStore(
            CosmosConfiguration(
                consistency="Session",
                regions=("westus", "eastus"),
                certified_mode=True,
            )
        )


def test_cosmos_and_sql_adapters_do_not_inherit_test_store() -> None:
    assert not issubclass(BlobOutboxStore, FakeOutboxStore)
    assert not issubclass(DualRegionBlobOutboxStore, FakeOutboxStore)
    assert not issubclass(CosmosStrongOutboxStore, FakeOutboxStore)
    assert not issubclass(AzureSqlSyncOutboxStore, FakeOutboxStore)
    assert not issubclass(SqlAlwaysOnOutboxStore, FakeOutboxStore)


def test_cosmos_partition_key_colocates_ordered_events() -> None:
    store = CosmosStrongOutboxStore(
        CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
    )
    event = make_event("event-1", ordering_key="customer-1")

    assert store.partition_key_for(event).startswith(f"{event.topic}#")


@pytest.mark.asyncio
async def test_cosmos_records_partition_keys_in_client() -> None:
    client = InMemoryCosmosOutboxClient()
    store = CosmosStrongOutboxStore(
        CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
        client=client,
    )
    event = make_event("event-1", ordering_key="customer-1")

    await store.put(event)

    partition_key = store.partition_key_for(event)
    assert client.partition_keys_by_event_id[event.event_id] == partition_key
    assert (partition_key, event.event_id) in client.records


@pytest.mark.parametrize(
    "store",
    [
        CosmosStrongOutboxStore(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        ),
        AzureSqlSyncOutboxStore(),
        SqlAlwaysOnOutboxStore(),
    ],
)
@pytest.mark.asyncio
async def test_provider_put_is_idempotent_for_compatible_duplicate(store: Any) -> None:
    event = make_event("same-event")

    first = await store.put(event)
    second = await store.put(event)

    assert second.event_id == first.event_id
    assert second.accepted_at == first.accepted_at


@pytest.mark.parametrize(
    "store",
    [
        CosmosStrongOutboxStore(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        ),
        AzureSqlSyncOutboxStore(),
        SqlAlwaysOnOutboxStore(),
    ],
)
@pytest.mark.asyncio
async def test_provider_put_rejects_incompatible_duplicate(store: Any) -> None:
    event = make_event("same-event")
    incompatible = replace(event, topic="other-topic")
    await store.put(event)

    with pytest.raises(DuplicateEventConflictError, match="incompatible"):
        await store.put(incompatible)


@pytest.mark.asyncio
async def test_cosmos_enforces_declared_payload_limit() -> None:
    store = CosmosStrongOutboxStore(
        CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
    )
    event = replace(make_event("oversized"), payload=b"x" * (2 * 1024 * 1024 + 1))

    with pytest.raises(ValidationError, match="payload"):
        await store.put(event)


@pytest.mark.asyncio
async def test_cosmos_claim_is_single_winner_with_shared_client_snapshots() -> None:
    client = InMemoryCosmosOutboxClient()
    first = CosmosStrongOutboxStore(
        CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
        client=client,
    )
    second = CosmosStrongOutboxStore(
        CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
        client=client,
    )
    event = make_event("cosmos-single-winner")
    await first.put(event)
    first_candidates = await first._claim_ordered_records()
    second_candidates = await second._claim_ordered_records()

    first_claim = await first._claim_from_candidates(first_candidates, limit=1)
    second_claim = await second._claim_from_candidates(second_candidates, limit=1)

    assert [claim.event.event_id for claim in first_claim] == [event.event_id]
    assert second_claim == []


@pytest.mark.asyncio
async def test_sql_claim_is_single_winner_with_shared_client_snapshots() -> None:
    client = InMemorySqlOutboxClient()
    first = AzureSqlSyncOutboxStore(client=client)
    second = AzureSqlSyncOutboxStore(client=client)
    event = make_event("sql-single-winner")
    await first.put(event)
    first_candidates = await first._claim_ordered_records()
    second_candidates = await second._claim_ordered_records()

    first_claim = await first._claim_from_candidates(first_candidates, limit=1)
    second_claim = await second._claim_from_candidates(second_candidates, limit=1)

    assert [claim.event.event_id for claim in first_claim] == [event.event_id]
    assert second_claim == []


@pytest.mark.parametrize(
    "store",
    [
        CosmosStrongOutboxStore(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        ),
        AzureSqlSyncOutboxStore(),
        SqlAlwaysOnOutboxStore(),
    ],
)
@pytest.mark.asyncio
async def test_provider_claim_retry_sent_failed_replay_and_cleanup_freeze(
    store: Any,
) -> None:
    retryable = make_event("retryable")
    failed = make_event("failed")
    sent = make_event("sent")
    await store.put(retryable)
    await store.put(failed)
    await store.put(sent)

    claimed = await store.claim_batch(limit=3)
    claims_by_id = {claim.event.event_id: claim for claim in claimed}
    await store.mark_pending_after_retryable_failure(
        claims_by_id["retryable"],
        error_type="Retryable",
        error_message="try again",
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    await store.mark_failed(
        claims_by_id["failed"],
        error_type="Fatal",
        error_message="stop",
    )
    await store.mark_sent(
        claims_by_id["sent"],
        PublishResult(partition=1, offset=2, published_at=datetime.now(UTC)),
    )

    replay = await store.failover_replay_candidates(
        failover_started_at=datetime.now(UTC),
        limit=10,
    )
    assert {claim.event.event_id for claim in replay} == {"retryable", "sent"}

    await store.freeze_cleanup(reason="replay")
    deleted = await store.cleanup_sent(
        now=datetime.now(UTC) + timedelta(hours=1),
        safety_margin=timedelta(seconds=0),
    )

    assert deleted == 0
    assert store.cleanup_frozen is True


@pytest.mark.parametrize(
    ("store", "record_getter"),
    [
        (
            CosmosStrongOutboxStore(
                CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
            ),
            lambda store, event_id: store.client.get(event_id),
        ),
        (
            AzureSqlSyncOutboxStore(),
            lambda store, event_id: store.client.get(event_id),
        ),
        (
            SqlAlwaysOnOutboxStore(),
            lambda store, event_id: store.client.get(event_id),
        ),
    ],
)
@pytest.mark.asyncio
async def test_provider_repair_failed_to_pending_clears_retry_state(
    store: Any,
    record_getter: Any,
) -> None:
    event = make_event("repair-reset")
    await store.put(event)
    claimed = (await store.claim_batch(limit=1))[0]
    await store.mark_pending_after_retryable_failure(
        claimed,
        error_type="Retryable",
        error_message="try again",
        next_attempt_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    retry_claim = (
        await store.failover_replay_candidates(
            failover_started_at=datetime.now(UTC),
            limit=1,
        )
    )[0]
    await store.mark_failed(
        retry_claim,
        error_type="Fatal",
        error_message="stop",
    )

    await store.repair_failed_to_pending(event_id=event.event_id)

    record: CosmosStoredEvent | SqlStoredEvent | None = await record_getter(
        store, event.event_id
    )
    assert record is not None
    assert record.status is OutboxStatus.PENDING
    assert record.failed_at is None
    assert record.attempt_count == 0
    assert record.last_error_type is None
    assert record.last_error is None
    assert record.next_attempt_at is None
    assert record.claim_token is None
    assert record.claimed_at is None


@pytest.mark.asyncio
async def test_memory_repair_unknown_event_is_noop() -> None:
    store = FakeOutboxStore()

    await store.repair_failed_to_pending(event_id="missing")


def test_sql_schema_contains_required_indexes() -> None:
    assert SQL_PENDING_INDEX_NAME in SQL_SCHEMA
    assert SQL_REPLAY_INDEX_NAME in SQL_SCHEMA
    assert SQL_ORDERED_INDEX_NAME in SQL_SCHEMA


@pytest.mark.asyncio
async def test_azure_sql_sync_wait_timeout_is_retryable() -> None:
    store = AzureSqlSyncOutboxStore(AzureSqlSyncConfiguration(sync_wait_succeeds=False))

    with pytest.raises(RetryableStoreError):
        await store.put(make_event())


@pytest.mark.asyncio
async def test_azure_sql_sync_wait_runs_after_compatible_put() -> None:
    client = InMemorySqlOutboxClient(sync_wait_succeeds=True)
    store = AzureSqlSyncOutboxStore(client=client)
    event = make_event()

    await store.put(event)

    assert client.sync_wait_count == 1


def test_always_on_requires_synchronized_secondary() -> None:
    with pytest.raises(ValueError):
        SqlAlwaysOnOutboxStore(required_synchronized_secondaries=0)


@pytest.mark.asyncio
async def test_always_on_requires_configured_secondaries_on_put() -> None:
    store = SqlAlwaysOnOutboxStore(
        required_synchronized_secondaries=2,
        client=InMemorySqlOutboxClient(synchronized_secondaries=1),
    )

    with pytest.raises(RetryableStoreError, match="secondary"):
        await store.put(make_event())
