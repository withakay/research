from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest

from durable_outbox.core import AdminActionStatus, ConfigurationError, ValidationError
from durable_outbox.core.errors import (
    ClaimConflictError,
    DuplicateEventConflictError,
    RetryableStoreError,
)
from durable_outbox.core.model import OutboxEvent, OutboxStatus, PublishResult
from durable_outbox.core.store import DurableOutboxStore
from durable_outbox.stores import blob_geo
from durable_outbox.stores.blob_geo import (
    MAX_BLOB_PAYLOAD_BYTES,
    BlobObject,
    BlobOutboxStore,
    DualRegionBlobOutboxStore,
    InMemoryBlobClient,
    _decode_record,
    _encode_record,
    _event_fingerprint,
    blob_metadata,
    event_blob_name,
    ordering_lock_blob_name,
    payload_blob_name,
    state_blob_name,
)
from durable_outbox.stores.cosmos import (
    CosmosConfiguration,
    CosmosStoredEvent,
    CosmosStrongOutboxStore,
    InMemoryCosmosOutboxClient,
)
from durable_outbox.stores.memory import (
    CleanupFreezeState,
    MemoryOutboxStore,
    StoredEvent,
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
from durable_outbox.telemetry import InMemoryMetrics
from durable_outbox.testing import FakeOutboxStore, FixedClock
from durable_outbox.testing.provider_contract import (
    ProviderContract,
    make_event,
    make_ordered_event,
    run_provider_contract,
)

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping


class InterruptingClock(FixedClock):
    def __init__(self, now: datetime) -> None:
        super().__init__(now)
        self._fail_after: int | None = None
        self._calls = 0

    def interrupt_after(self, successful_calls: int) -> None:
        self._fail_after = successful_calls
        self._calls = 0

    def utcnow(self) -> datetime:
        if self._fail_after is not None:
            if self._calls >= self._fail_after:
                raise RuntimeError("clock interrupted")
            self._calls += 1
        return self.now


class ConflictOnceCosmosClient(InMemoryCosmosOutboxClient):
    def __init__(self) -> None:
        super().__init__()
        self.remaining_conflicts = 0

    async def replace(
        self,
        record: Any,
        *,
        expected_version: int,
    ) -> Any:
        if self.remaining_conflicts > 0:
            self.remaining_conflicts -= 1
            raise ClaimConflictError("record version precondition failed")
        return await super().replace(record, expected_version=expected_version)


class ConflictOnceSqlClient(InMemorySqlOutboxClient):
    def __init__(self) -> None:
        super().__init__()
        self.remaining_conflicts = 0

    async def replace(
        self,
        record: Any,
        *,
        expected_version: int,
    ) -> Any:
        if self.remaining_conflicts > 0:
            self.remaining_conflicts -= 1
            raise ClaimConflictError("record version precondition failed")
        return await super().replace(record, expected_version=expected_version)


class CountingCosmosClient(InMemoryCosmosOutboxClient):
    def __init__(self) -> None:
        super().__init__()
        self.list_records_calls = 0
        self.claim_batch_pending_calls = 0
        self.failover_replay_candidate_calls = 0

    async def list_records(self) -> Any:
        self.list_records_calls += 1
        return await super().list_records()

    async def claim_batch_pending(
        self,
        *,
        limit: int,
        now: datetime,
        claim_timeout: timedelta,
    ) -> Any:
        self.claim_batch_pending_calls += 1
        return await super().claim_batch_pending(
            limit=limit,
            now=now,
            claim_timeout=claim_timeout,
        )

    async def list_failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> Any:
        self.failover_replay_candidate_calls += 1
        return await super().list_failover_replay_candidates(
            failover_started_at=failover_started_at,
            limit=limit,
            exclude_event_ids=exclude_event_ids,
        )


class CountingSqlClient(InMemorySqlOutboxClient):
    def __init__(self) -> None:
        super().__init__()
        self.list_records_calls = 0
        self.claim_batch_pending_calls = 0
        self.failover_replay_candidate_calls = 0

    async def list_records(self) -> Any:
        self.list_records_calls += 1
        return await super().list_records()

    async def claim_batch_pending(
        self,
        *,
        limit: int,
        now: datetime,
        claim_timeout: timedelta,
    ) -> Any:
        self.claim_batch_pending_calls += 1
        return await super().claim_batch_pending(
            limit=limit,
            now=now,
            claim_timeout=claim_timeout,
        )

    async def list_failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> Any:
        self.failover_replay_candidate_calls += 1
        return await super().list_failover_replay_candidates(
            failover_started_at=failover_started_at,
            limit=limit,
            exclude_event_ids=exclude_event_ids,
        )


class FailingDeleteBlobClient(InMemoryBlobClient):
    def __init__(self) -> None:
        super().__init__()
        self.fail_deletes = False

    async def delete_blob(self, name: str, *, if_match: str | None = None) -> bool:
        if self.fail_deletes and name.startswith("outbox/v1/state/"):
            raise RuntimeError("secondary cleanup failed")
        return await super().delete_blob(name, if_match=if_match)


class ConcurrentDeleteBlobClient(InMemoryBlobClient):
    def __init__(self) -> None:
        super().__init__()
        self.in_flight_deletes = 0
        self.max_in_flight_deletes = 0

    async def delete_blob(self, name: str, *, if_match: str | None = None) -> bool:
        self.in_flight_deletes += 1
        self.max_in_flight_deletes = max(
            self.max_in_flight_deletes,
            self.in_flight_deletes,
        )
        try:
            await asyncio.sleep(0)
            return await super().delete_blob(name, if_match=if_match)
        finally:
            self.in_flight_deletes -= 1


class ClaimScanCountingBlobClient(InMemoryBlobClient):
    def __init__(self) -> None:
        super().__init__()
        self.get_blob_calls = 0
        self.list_with_content_values: list[bool] = []

    async def get_blob(self, name: str) -> BlobObject | None:
        self.get_blob_calls += 1
        return await super().get_blob(name)

    async def list_blobs(
        self,
        *,
        prefix: str,
        with_content: bool = True,
    ) -> list[BlobObject]:
        self.list_with_content_values.append(with_content)
        return await super().list_blobs(prefix=prefix, with_content=with_content)


class PutRecordingBlobClient(InMemoryBlobClient):
    def __init__(self) -> None:
        super().__init__()
        self.puts: list[tuple[str, bytes]] = []

    async def put_blob(
        self,
        name: str,
        content: bytes,
        metadata: Mapping[str, str],
        *,
        if_none_match: bool = False,
        if_match: str | None = None,
    ) -> BlobObject:
        self.puts.append((name, bytes(content)))
        return await super().put_blob(
            name,
            content,
            metadata,
            if_none_match=if_none_match,
            if_match=if_match,
        )


class CleanupCountingCosmosClient(InMemoryCosmosOutboxClient):
    def __init__(self) -> None:
        super().__init__()
        self.list_records_calls = 0
        self.cleanup_candidate_limits: list[int | None] = []

    async def list_records(self) -> tuple[CosmosStoredEvent, ...]:
        self.list_records_calls += 1
        return tuple(await super().list_records())

    async def list_cleanup_candidates(
        self,
        *,
        now: datetime,
        safety_margin: timedelta,
        limit: int | None = None,
    ) -> tuple[CosmosStoredEvent, ...]:
        self.cleanup_candidate_limits.append(limit)
        return tuple(
            await super().list_cleanup_candidates(
                now=now,
                safety_margin=safety_margin,
                limit=limit,
            )
        )


class CleanupCountingSqlClient(InMemorySqlOutboxClient):
    def __init__(self) -> None:
        super().__init__()
        self.list_records_calls = 0
        self.cleanup_candidate_limits: list[int | None] = []

    async def list_records(self) -> tuple[SqlStoredEvent, ...]:
        self.list_records_calls += 1
        return tuple(await super().list_records())

    async def list_cleanup_candidates(
        self,
        *,
        now: datetime,
        safety_margin: timedelta,
        limit: int | None = None,
    ) -> tuple[SqlStoredEvent, ...]:
        self.cleanup_candidate_limits.append(limit)
        return tuple(
            await super().list_cleanup_candidates(
                now=now,
                safety_margin=safety_margin,
                limit=limit,
            )
        )


class FailingPutBlobClient(InMemoryBlobClient):
    def __init__(self) -> None:
        super().__init__()
        self.remaining_event_put_failures = 0

    async def put_blob(
        self,
        name: str,
        content: bytes,
        metadata: Mapping[str, str],
        *,
        if_none_match: bool = False,
        if_match: str | None = None,
    ) -> Any:
        if self.remaining_event_put_failures > 0 and name.startswith(
            "outbox/v1/state/"
        ):
            self.remaining_event_put_failures -= 1
            raise RuntimeError("standby mirror update failed")
        return await super().put_blob(
            name,
            content,
            metadata,
            if_none_match=if_none_match,
            if_match=if_match,
        )


class FailingNthEventPutBlobClient(InMemoryBlobClient):
    def __init__(self, *, fail_on_event_put: int) -> None:
        super().__init__()
        self.fail_on_event_put = fail_on_event_put
        self.event_put_count = 0

    async def put_blob(
        self,
        name: str,
        content: bytes,
        metadata: Mapping[str, str],
        *,
        if_none_match: bool = False,
        if_match: str | None = None,
    ) -> Any:
        if name.startswith("outbox/v1/state/"):
            self.event_put_count += 1
            if self.event_put_count == self.fail_on_event_put:
                raise RuntimeError("secondary accept failed")
        return await super().put_blob(
            name,
            content,
            metadata,
            if_none_match=if_none_match,
            if_match=if_match,
        )


class CoordinatedDualRegionBlobOutboxStore(DualRegionBlobOutboxStore):
    def __init__(self) -> None:
        super().__init__(
            primary_client=InMemoryBlobClient(),
            secondary_client=InMemoryBlobClient(),
        )
        self.phase_events = {
            "prepare": asyncio.Event(),
            "accept": asyncio.Event(),
        }
        self.phase_starts: dict[str, list[str]] = {
            "prepare": [],
            "accept": [],
        }
        self.timeline: list[str] = []

    async def _prepare(self, region: BlobOutboxStore, event: Any) -> None:
        await self._enter_phase("prepare", region)
        await super()._prepare(region, event)
        self.timeline.append(f"prepare-finish:{region.environment}")

    async def _accept(self, region: BlobOutboxStore, event: Any) -> None:
        await self._enter_phase("accept", region)
        await super()._accept(region, event)
        self.timeline.append(f"accept-finish:{region.environment}")

    async def _enter_phase(self, phase: str, region: BlobOutboxStore) -> None:
        self.phase_starts[phase].append(region.environment)
        self.timeline.append(f"{phase}-start:{region.environment}")
        if len(self.phase_starts[phase]) == 2:
            self.phase_events[phase].set()
        await asyncio.wait_for(self.phase_events[phase].wait(), timeout=0.05)


def test_store_package_exports_are_importable() -> None:
    from durable_outbox import stores

    for name in stores.__all__:
        assert getattr(stores, name).__name__ == name


def test_store_protocol_includes_admin_and_cleanup_contracts() -> None:
    assert hasattr(DurableOutboxStore, "cleanup_sent")
    assert hasattr(DurableOutboxStore, "repair_failed_to_pending")
    assert hasattr(DurableOutboxStore, "replay_event")


def test_production_adapters_require_explicit_clients() -> None:
    with pytest.raises(ConfigurationError, match="for_testing"):
        BlobOutboxStore()
    with pytest.raises(ConfigurationError, match="for_testing"):
        DualRegionBlobOutboxStore()
    with pytest.raises(ConfigurationError, match="for_testing"):
        CosmosStrongOutboxStore(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        )
    with pytest.raises(ConfigurationError, match="for_testing"):
        AzureSqlSyncOutboxStore()
    with pytest.raises(ConfigurationError, match="for_testing"):
        SqlAlwaysOnOutboxStore()


def test_for_testing_adapters_use_non_production_store_names() -> None:
    stores = [
        BlobOutboxStore.for_testing(),
        DualRegionBlobOutboxStore.for_testing(),
        CosmosStrongOutboxStore.for_testing(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        ),
        AzureSqlSyncOutboxStore.for_testing(),
        SqlAlwaysOnOutboxStore.for_testing(),
    ]

    assert all(store.capabilities.store_name.startswith("InMemory") for store in stores)


@pytest.mark.parametrize(
    ("store", "expected_witness"),
    [
        (FakeOutboxStore(), ("memory:process",)),
        (BlobOutboxStore.for_testing(environment="unit"), ("blob:unit",)),
        (
            DualRegionBlobOutboxStore.for_testing(environment="unit"),
            ("blob:unit-primary", "blob:unit-secondary"),
        ),
        (
            CosmosStrongOutboxStore.for_testing(
                CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
            ),
            ("cosmos:westus", "cosmos:eastus"),
        ),
        (
            AzureSqlSyncOutboxStore.for_testing(),
            ("azure-sql:primary", "azure-sql:sync-secondary"),
        ),
        (
            SqlAlwaysOnOutboxStore.for_testing(
                required_synchronized_secondaries=2,
                synchronized_secondaries=2,
            ),
            (
                "sql-always-on:primary",
                "sql-always-on:sync-secondary-1",
                "sql-always-on:sync-secondary-2",
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_accept_receipts_include_durability_witness(
    store: Any,
    expected_witness: tuple[str, ...],
) -> None:
    receipt = await store.put(make_event("receipt-witness"))

    assert receipt.durability_witness == expected_witness


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
    store = BlobOutboxStore.for_testing()
    event = make_event("same-event")

    first = await store.put(event)
    second = await store.put(event)

    assert second.event_id == first.event_id
    assert second.accepted_at == first.accepted_at


@pytest.mark.asyncio
async def test_blob_put_rejects_incompatible_duplicate() -> None:
    store = BlobOutboxStore.for_testing()
    event = make_event("same-event")
    incompatible = replace(event, topic="other-topic")
    await store.put(event)

    with pytest.raises(DuplicateEventConflictError, match="topic"):
        await store.put(incompatible)


@pytest.mark.asyncio
async def test_blob_claim_batch_avoids_full_record_clone_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = BlobOutboxStore.for_testing()
    event = make_event("blob-claim-no-clone")
    await store.put(event)

    def fail_clone(_record: StoredEvent) -> StoredEvent:
        raise AssertionError("claim_batch should snapshot mutated fields only")

    monkeypatch.setattr(blob_geo, "_clone_record", fail_clone)

    claimed = await store.claim_batch(limit=1)

    assert [claim.event.event_id for claim in claimed] == [event.event_id]


@pytest.mark.asyncio
async def test_blob_claim_batch_reuses_refreshed_event_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = BlobOutboxStore.for_testing()
    event = make_event("blob-claim-cached-fingerprint")
    await store.put(event)
    original = blob_geo._event_fingerprint
    calls = 0

    def counting_fingerprint(
        event: OutboxEvent,
        *,
        key: bytes | None = None,
    ) -> str:
        nonlocal calls
        calls += 1
        return original(event, key=key)

    monkeypatch.setattr(blob_geo, "_event_fingerprint", counting_fingerprint)

    claimed = await store.claim_batch(limit=1)

    assert [claim.event.event_id for claim in claimed] == [event.event_id]
    assert calls == 1


@pytest.mark.asyncio
async def test_blob_store_rejects_payloads_above_fingerprint_budget() -> None:
    store = BlobOutboxStore.for_testing()
    event = replace(
        make_event("oversized-blob-payload"),
        payload=b"x" * (MAX_BLOB_PAYLOAD_BYTES + 1),
    )

    with pytest.raises(ValidationError, match="max_payload_bytes"):
        await store.put(event)


@pytest.mark.asyncio
async def test_blob_load_rejects_tampered_fingerprint() -> None:
    client = InMemoryBlobClient()
    store = BlobOutboxStore(client=client)
    event = make_event("tamper")
    await store.put(event)
    blob = await client.get_blob(state_blob_name(event.event_id))
    assert blob is not None
    await client.put_blob(
        blob.name,
        blob.content,
        {**blob.metadata, "event_fingerprint": "bad"},
        if_match=blob.etag,
    )

    with pytest.raises(RetryableStoreError, match="fingerprint"):
        await store._load_record(event.event_id)


@pytest.mark.asyncio
async def test_blob_verification_recomputes_fingerprint_for_tampered_content() -> None:
    client = InMemoryBlobClient()
    store = BlobOutboxStore(client=client)
    event = make_event("tamper-content")
    await store.put(event)
    blob = await client.get_blob(payload_blob_name(event.event_id))
    assert blob is not None
    tampered_payload = b"tampered"
    await client.put_blob(
        blob.name,
        tampered_payload,
        blob.metadata,
        if_match=blob.etag,
    )

    with pytest.raises(RetryableStoreError, match="fingerprint"):
        await store._load_record(event.event_id)
    with pytest.raises(RetryableStoreError, match="fingerprint"):
        await store._refresh_records()


@pytest.mark.asyncio
async def test_blob_store_can_use_keyed_event_fingerprints() -> None:
    client = InMemoryBlobClient()
    store = BlobOutboxStore(client=client, fingerprint_key=b"secret")
    event = make_event("keyed-fingerprint")

    await store.put(event)

    blob = await client.get_blob(state_blob_name(event.event_id))
    assert blob is not None
    fingerprint = blob.metadata["event_fingerprint"]
    assert fingerprint != _event_fingerprint(event)

    same_key_store = BlobOutboxStore(client=client, fingerprint_key=b"secret")
    loaded = await same_key_store._load_record(event.event_id)
    assert loaded is not None
    assert loaded.event.event_id == event.event_id

    wrong_key_store = BlobOutboxStore(client=client, fingerprint_key=b"wrong")
    with pytest.raises(RetryableStoreError, match="fingerprint"):
        await wrong_key_store._load_record(event.event_id)


@pytest.mark.asyncio
async def test_blob_fingerprint_cache_is_store_local_by_key() -> None:
    event = make_event("same-object-different-keys")
    first_client = InMemoryBlobClient()
    second_client = InMemoryBlobClient()
    first_store = BlobOutboxStore(client=first_client, fingerprint_key=b"a")
    second_store = BlobOutboxStore(client=second_client, fingerprint_key=b"b")

    await first_store.put(event)
    await second_store.put(event)

    first_blob = await first_client.get_blob(state_blob_name(event.event_id))
    second_blob = await second_client.get_blob(state_blob_name(event.event_id))
    assert first_blob is not None
    assert second_blob is not None
    assert (
        first_blob.metadata["event_fingerprint"]
        != second_blob.metadata["event_fingerprint"]
    )
    assert await first_store._load_record(event.event_id) is not None
    assert await second_store._load_record(event.event_id) is not None


@pytest.mark.asyncio
async def test_blob_refresh_drops_stale_fingerprint_cache_for_missing_blob() -> None:
    client = InMemoryBlobClient()
    store = BlobOutboxStore(client=client)
    original = make_event("reuse-after-refresh")
    replacement = replace(original, payload=b"replacement")
    await store.put(original)
    blob = await client.get_blob(state_blob_name(original.event_id))
    assert blob is not None
    assert await client.delete_blob(blob.name, if_match=blob.etag)

    await store._refresh_records()
    await store.put(replacement)

    fresh_store = BlobOutboxStore(client=client)
    loaded = await fresh_store._load_record(replacement.event_id)
    assert loaded is not None
    assert loaded.event.payload == replacement.payload


@pytest.mark.asyncio
async def test_blob_cleanup_drops_stale_fingerprint_cache_for_reused_event_id() -> None:
    client = InMemoryBlobClient()
    store = BlobOutboxStore(client=client)
    original = make_event("reuse-after-cleanup")
    replacement = replace(original, payload=b"replacement")
    await store.put(original)
    claimed = (await store.claim_batch(limit=1))[0]
    await store.mark_sent(
        claimed,
        PublishResult(partition=0, offset=1, published_at=datetime.now(UTC)),
    )

    deleted = await store.cleanup_sent(
        now=original.expires_at + timedelta(seconds=1),
        safety_margin=timedelta(seconds=0),
    )
    await store.put(replacement)

    fresh_store = BlobOutboxStore(client=client)
    loaded = await fresh_store._load_record(replacement.event_id)
    assert deleted == 1
    assert loaded is not None
    assert loaded.event.payload == replacement.payload


@pytest.mark.asyncio
async def test_blob_decode_requires_created_and_expires_timestamps() -> None:
    client = InMemoryBlobClient()
    store = BlobOutboxStore(client=client)
    event = make_event("missing-created")
    await store.put(event)
    blob = await client.get_blob(state_blob_name(event.event_id))
    assert blob is not None
    encoded = json.loads(blob.content)
    encoded["event"]["created_at"] = None
    await client.put_blob(
        blob.name,
        json.dumps(encoded).encode(),
        blob.metadata,
        if_match=blob.etag,
    )

    with pytest.raises(RetryableStoreError, match="created_at"):
        await store._load_record(event.event_id)


@pytest.mark.parametrize(
    ("field_path", "value", "match"),
    [
        (("accepted",), "false", "accepted"),
        (("claim_token",), 123, "claim_token"),
        (("attempt_count",), True, "attempt_count"),
        (("event", "headers", "x-bad"), 123, "headers"),
        (("publish_result", "metadata", "partition"), 1, "metadata"),
    ],
)
def test_blob_decode_rejects_invalid_field_types(
    field_path: tuple[str, ...],
    value: Any,
    match: str,
) -> None:
    event = make_event("invalid-field")
    stored = StoredEvent(
        event=event,
        status=OutboxStatus.SENT,
        publish_result=PublishResult(
            partition=1,
            offset=2,
            published_at=datetime.now(UTC),
            metadata={"partition": "0"},
        ),
    )
    encoded = json.loads(_encode_record(stored))
    target = encoded
    for field in field_path[:-1]:
        target = target[field]
    target[field_path[-1]] = value

    with pytest.raises(RetryableStoreError, match=match):
        _decode_record(json.dumps(encoded).encode())


@pytest.mark.asyncio
async def test_blob_refresh_evicts_records_deleted_from_backend() -> None:
    client = InMemoryBlobClient()
    first = BlobOutboxStore(client=client)
    second = BlobOutboxStore(client=client)
    event = make_event("deleted-from-backend")
    await first.put(event)
    await second._refresh_records()
    blob = await client.get_blob(state_blob_name(event.event_id))
    assert blob is not None

    deleted = await client.delete_blob(blob.name)
    await second._refresh_records()

    assert deleted is True
    assert event.event_id not in second.records
    assert event.event_id not in second._record_etags


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
    store = DualRegionBlobOutboxStore.for_testing()
    event = make_event()

    receipt = await store.put(event)

    assert receipt.rpo_zero is True
    assert store.primary.records[event.event_id].accepted is True
    assert store.secondary.records[event.event_id].accepted is True


@pytest.mark.asyncio
async def test_dual_region_blob_put_runs_prepare_and_accept_phases_concurrently() -> (
    None
):
    store = CoordinatedDualRegionBlobOutboxStore()
    event = make_event("parallel-dual-region-put")

    await store.put(event)

    assert set(store.phase_starts["prepare"]) == {
        "default-primary",
        "default-secondary",
    }
    assert set(store.phase_starts["accept"]) == {
        "default-primary",
        "default-secondary",
    }
    prepare_finishes = [
        index
        for index, entry in enumerate(store.timeline)
        if entry.startswith("prepare-finish:")
    ]
    accept_starts = [
        index
        for index, entry in enumerate(store.timeline)
        if entry.startswith("accept-start:")
    ]
    assert max(prepare_finishes) < min(accept_starts)


@pytest.mark.asyncio
async def test_dual_region_blob_put_does_not_cache_secondary_accept_failure() -> None:
    secondary_client = FailingNthEventPutBlobClient(fail_on_event_put=2)
    store = DualRegionBlobOutboxStore(
        primary_client=InMemoryBlobClient(),
        secondary_client=secondary_client,
    )
    event = make_event("secondary-accept-failure")

    with pytest.raises(RuntimeError, match="secondary accept failed"):
        await store.put(event)

    assert store.primary.records[event.event_id].accepted is True
    assert store.secondary.records[event.event_id].accepted is False
    secondary_blob = await secondary_client.get_blob(state_blob_name(event.event_id))
    assert secondary_blob is not None
    assert secondary_blob.metadata["accepted"] == "false"


@pytest.mark.asyncio
async def test_dual_region_records_view_is_read_only_snapshot() -> None:
    store = DualRegionBlobOutboxStore.for_testing()
    event = make_event("records-view")
    await store.put(event)
    records: Any = store.records

    with pytest.raises(TypeError):
        records[event.event_id] = records[event.event_id]
    records[event.event_id].accepted = False

    assert store.primary.records[event.event_id].accepted is True


@pytest.mark.asyncio
async def test_dual_region_blob_can_promote_secondary_for_dispatch() -> None:
    store = DualRegionBlobOutboxStore.for_testing()
    event = make_event("secondary-active")
    await store.put(event)

    store.promote_secondary()
    claimed = await store.claim_batch(limit=1)
    await store.mark_sent(
        claimed[0],
        PublishResult(partition=1, offset=2, published_at=datetime.now(UTC)),
    )

    assert [claim.event.event_id for claim in claimed] == [event.event_id]
    assert store.secondary.records[event.event_id].status is OutboxStatus.SENT
    assert store.primary.records[event.event_id].status is OutboxStatus.SENT


@pytest.mark.asyncio
async def test_dual_region_blob_failover_replay_uses_active_secondary() -> None:
    store = DualRegionBlobOutboxStore.for_testing()
    event = make_event("secondary-replay")
    await store._prepare(store.secondary, event)
    await store._accept(store.secondary, event)

    store.promote_secondary()
    candidates = await store.failover_replay_candidates(
        failover_started_at=datetime.now(UTC),
        limit=10,
    )

    assert [claim.event.event_id for claim in candidates] == [event.event_id]


@pytest.mark.asyncio
async def test_dual_region_blob_reconciles_prepared_records_before_failover_replay() -> (
    None
):
    store = DualRegionBlobOutboxStore.for_testing()
    event = make_event("prepared-replay")
    await store._prepare(store.primary, event)
    await store._prepare(store.secondary, event)

    prepared = await store.list_prepared_event_ids()
    candidates = await store.failover_replay_candidates(
        failover_started_at=datetime.now(UTC),
        limit=10,
    )

    assert prepared == ("prepared-replay",)
    assert [claim.event.event_id for claim in candidates] == [event.event_id]
    assert store.primary.records[event.event_id].accepted is True
    assert store.secondary.records[event.event_id].accepted is True


@pytest.mark.asyncio
async def test_dual_region_blob_repair_copies_missing_region() -> None:
    store = DualRegionBlobOutboxStore.for_testing()
    event = make_event()
    await store._prepare(store.primary, event)
    await store._accept(store.primary, event)

    await store.repair_prepared(event.event_id)

    assert store.secondary.records[event.event_id].accepted is True


@pytest.mark.asyncio
async def test_blob_accept_prepared_preserves_existing_accepted_at() -> None:
    occurred_at = datetime(2026, 5, 22, 9, 30, tzinfo=UTC)
    repaired_at = occurred_at + timedelta(hours=1)
    clock = FixedClock(occurred_at)
    store = BlobOutboxStore.for_testing(clock=clock)
    event = make_event("accepted-at-repair")
    await store._put_prepared(event)
    await store._accept_prepared(event)
    first_accepted_at = store.records[event.event_id].accepted_at

    store.records[event.event_id].accepted = False
    await store._save_record(store.records[event.event_id])
    clock.now = repaired_at
    await store._accept_prepared(event)

    assert first_accepted_at == occurred_at
    assert store.records[event.event_id].accepted_at == occurred_at


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
    store = DualRegionBlobOutboxStore.for_testing()
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
    store = DualRegionBlobOutboxStore.for_testing()
    event = make_event()
    await store._prepare(store.primary, event)

    assert await store.claim_batch(limit=10) == []


@pytest.mark.asyncio
async def test_dual_region_cleanup_preserves_active_when_standby_delete_fails() -> None:
    primary_client = InMemoryBlobClient()
    secondary_client = FailingDeleteBlobClient()
    store = DualRegionBlobOutboxStore(
        primary_client=primary_client,
        secondary_client=secondary_client,
    )
    event = make_event("cleanup-secondary-failure")
    await store.put(event)
    claimed = (await store.claim_batch(limit=1))[0]
    await store.mark_sent(
        claimed,
        PublishResult(partition=1, offset=2, published_at=datetime.now(UTC)),
    )

    secondary_client.fail_deletes = True
    with pytest.raises(RuntimeError, match="secondary cleanup failed"):
        await store.cleanup_sent(
            now=datetime.now(UTC) + timedelta(hours=1),
            safety_margin=timedelta(seconds=0),
        )

    assert event.event_id in store.primary.records
    assert event.event_id in store.secondary.records


@pytest.mark.asyncio
async def test_dual_region_mirror_retries_transient_standby_update_failure() -> None:
    secondary_client = FailingPutBlobClient()
    store = DualRegionBlobOutboxStore(
        primary_client=InMemoryBlobClient(),
        secondary_client=secondary_client,
    )
    event = make_event("mirror-retry")
    await store.put(event)
    claimed = (await store.claim_batch(limit=1))[0]

    secondary_client.remaining_event_put_failures = 1
    await store.mark_sent(
        claimed,
        PublishResult(partition=1, offset=2, published_at=datetime.now(UTC)),
    )

    assert store.secondary.records[event.event_id].status is OutboxStatus.SENT
    assert await store.pending_mirror_event_ids() == ()


@pytest.mark.asyncio
async def test_dual_region_mirror_queues_reconciliation_after_repeated_failure() -> (
    None
):
    secondary_client = FailingPutBlobClient()
    metrics = InMemoryMetrics()
    store = DualRegionBlobOutboxStore(
        primary_client=InMemoryBlobClient(),
        secondary_client=secondary_client,
        metrics=metrics,
    )
    event = make_event("mirror-queue")
    await store.put(event)
    claimed = (await store.claim_batch(limit=1))[0]

    secondary_client.remaining_event_put_failures = 3
    with pytest.raises(RetryableStoreError, match="mirror"):
        await store.mark_sent(
            claimed,
            PublishResult(partition=1, offset=2, published_at=datetime.now(UTC)),
        )

    assert await store.pending_mirror_event_ids() == (event.event_id,)
    secondary_client.remaining_event_put_failures = 0
    repaired = await store.reconcile_mirror_updates()

    assert repaired == 1
    assert await store.pending_mirror_event_ids() == ()
    assert store.secondary.records[event.event_id].status is OutboxStatus.SENT
    assert (
        metrics.counts[
            (
                "outbox_blob_mirror_update_failures_total",
                (
                    ("active_region", "primary"),
                    ("error_type", "RuntimeError"),
                    ("standby_region", "secondary"),
                ),
            )
        ]
        == 3
    )
    assert (
        metrics.counts[
            (
                "outbox_blob_mirror_updates_queued_total",
                (
                    ("active_region", "primary"),
                    ("standby_region", "secondary"),
                ),
            )
        ]
        == 1
    )


@pytest.mark.asyncio
async def test_blob_store_uses_injected_clock_for_lifecycle_timestamps() -> None:
    occurred_at = datetime(2026, 5, 22, 9, 30, tzinfo=UTC)
    clock = FixedClock(occurred_at)
    store = BlobOutboxStore.for_testing(clock=clock)
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
    store = CosmosStrongOutboxStore.for_testing(
        CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
    )
    event = make_event("event-1", ordering_key="customer-1")

    assert store.partition_key_for(event).startswith(f"{event.topic}#")


def test_cosmos_unordered_partition_key_uses_stable_bucket() -> None:
    store = CosmosStrongOutboxStore.for_testing(
        CosmosConfiguration(
            consistency="Strong",
            regions=("westus", "eastus"),
            unordered_buckets=16,
        )
    )
    event = make_event("event-1")

    assert store.partition_key_for(event) == f"{event.topic}#9"


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
    "contract",
    [
        ProviderContract(store_factory=BlobOutboxStore.for_testing),
        ProviderContract(store_factory=DualRegionBlobOutboxStore.for_testing),
        ProviderContract(
            store_factory=lambda: CosmosStrongOutboxStore.for_testing(
                CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
            )
        ),
        ProviderContract(store_factory=AzureSqlSyncOutboxStore.for_testing),
        ProviderContract(store_factory=SqlAlwaysOnOutboxStore.for_testing),
    ],
)
@pytest.mark.asyncio
async def test_builtin_adapters_pass_reusable_provider_contract(
    contract: ProviderContract,
) -> None:
    await run_provider_contract(contract)


@pytest.mark.parametrize(
    "store",
    [
        CosmosStrongOutboxStore.for_testing(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        ),
        AzureSqlSyncOutboxStore.for_testing(),
        SqlAlwaysOnOutboxStore.for_testing(),
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
        CosmosStrongOutboxStore.for_testing(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        ),
        AzureSqlSyncOutboxStore.for_testing(),
        SqlAlwaysOnOutboxStore.for_testing(),
    ],
)
@pytest.mark.asyncio
async def test_provider_put_rejects_incompatible_duplicate(store: Any) -> None:
    event = make_event("same-event")
    incompatible = replace(event, topic="other-topic")
    await store.put(event)

    with pytest.raises(DuplicateEventConflictError, match="topic"):
        await store.put(incompatible)


@pytest.mark.asyncio
async def test_cosmos_enforces_declared_payload_limit() -> None:
    store = CosmosStrongOutboxStore.for_testing(
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
        CosmosStrongOutboxStore.for_testing(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        ),
        AzureSqlSyncOutboxStore.for_testing(),
        SqlAlwaysOnOutboxStore.for_testing(),
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


async def _record_from_store(store: Any, event_id: str) -> Any:
    if isinstance(store, BlobOutboxStore | MemoryOutboxStore):
        return store.records[event_id]
    if hasattr(store, "client"):
        return await store.client.get(event_id)
    return store.records[event_id]


@pytest.mark.parametrize(
    "store_factory",
    [
        lambda clock: MemoryOutboxStore(clock=clock),
        lambda clock: BlobOutboxStore.for_testing(clock=clock),
        lambda clock: CosmosStrongOutboxStore.for_testing(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
            clock=clock,
        ),
        lambda clock: AzureSqlSyncOutboxStore.for_testing(clock=clock),
        lambda clock: SqlAlwaysOnOutboxStore.for_testing(clock=clock),
    ],
)
@pytest.mark.asyncio
async def test_provider_cleanup_sent_honors_max_per_tick(
    store_factory: Any,
) -> None:
    now = datetime.now(UTC)
    store = store_factory(FixedClock(now))
    events = [make_event(f"cleanup-bound-{index}") for index in range(3)]
    for event in events:
        await store.put(event)
    claimed = await store.claim_batch(limit=3)
    for offset, claim in enumerate(claimed):
        await store.mark_sent(
            claim,
            PublishResult(partition=0, offset=offset, published_at=now),
        )

    first_deleted = await store.cleanup_sent(
        now=events[0].expires_at + timedelta(seconds=1),
        safety_margin=timedelta(seconds=0),
        batch_size=1,
        max_per_tick=2,
    )
    second_deleted = await store.cleanup_sent(
        now=events[0].expires_at + timedelta(seconds=1),
        safety_margin=timedelta(seconds=0),
        batch_size=1,
        max_per_tick=2,
    )

    assert first_deleted == 2
    assert second_deleted == 1


@pytest.mark.asyncio
async def test_blob_cleanup_sent_parallelizes_deletes_with_batch_limit() -> None:
    now = datetime.now(UTC)
    client = ConcurrentDeleteBlobClient()
    store = BlobOutboxStore(client=client, clock=FixedClock(now))
    events = [make_event(f"blob-cleanup-batch-{index}") for index in range(4)]
    for event in events:
        await store.put(event)
    claimed = await store.claim_batch(limit=4)
    for offset, claim in enumerate(claimed):
        await store.mark_sent(
            claim,
            PublishResult(partition=0, offset=offset, published_at=now),
        )

    deleted = await store.cleanup_sent(
        now=events[0].expires_at + timedelta(seconds=1),
        safety_margin=timedelta(seconds=0),
        batch_size=2,
        max_per_tick=3,
    )

    assert deleted == 3
    assert client.max_in_flight_deletes == 2
    assert await client.get_blob(state_blob_name(events[3].event_id)) is not None


@pytest.mark.asyncio
async def test_blob_claim_batch_skips_retained_sent_blobs_during_scan() -> None:
    now = datetime.now(UTC)
    client = ClaimScanCountingBlobClient()
    setup_store = BlobOutboxStore(client=client, clock=FixedClock(now))
    for index in range(10):
        event = make_event(f"retained-sent-{index}")
        await setup_store.put(event)
        claimed = (await setup_store.claim_batch(limit=1))[0]
        await setup_store.mark_sent(
            claimed,
            PublishResult(partition=0, offset=index, published_at=now),
        )
    pending = [make_event(f"pending-claim-{index}") for index in range(3)]
    for event in pending:
        await setup_store.put(event)
    client.get_blob_calls = 0
    client.list_with_content_values.clear()
    claim_store = BlobOutboxStore(client=client, clock=FixedClock(now))

    claimed = await claim_store.claim_batch(limit=2)

    assert len(claimed) == 2
    assert {claim.event.event_id for claim in claimed} <= {
        "pending-claim-0",
        "pending-claim-1",
        "pending-claim-2",
    }
    assert client.list_with_content_values == [False, False]
    assert client.get_blob_calls == 4


@pytest.mark.asyncio
async def test_blob_replay_stream_skips_retained_records_during_scan() -> None:
    now = datetime.now(UTC)
    client = ClaimScanCountingBlobClient()
    setup_store = BlobOutboxStore(client=client, clock=FixedClock(now))
    for index in range(10):
        event = make_event(f"retained-replay-sent-{index}")
        await setup_store.put(event)
        claimed = (await setup_store.claim_batch(limit=1))[0]
        await setup_store.mark_sent(
            claimed,
            PublishResult(partition=0, offset=index, published_at=now),
        )
    pending = [make_event(f"pending-replay-{index}") for index in range(3)]
    for event in pending:
        await setup_store.put(event)
    client.get_blob_calls = 0
    client.list_with_content_values.clear()
    replay_store = BlobOutboxStore(client=client, clock=FixedClock(now))

    claimed = [
        item
        async for item in replay_store.iter_failover_replay_candidates(
            failover_started_at=now,
            limit=2,
        )
    ]

    assert len(claimed) == 2
    assert client.list_with_content_values == [False, False]
    assert client.get_blob_calls <= 6


@pytest.mark.asyncio
async def test_blob_state_transitions_do_not_reupload_payload_bytes() -> None:
    now = datetime.now(UTC)
    client = PutRecordingBlobClient()
    store = BlobOutboxStore(client=client, clock=FixedClock(now))
    payload = b"x" * 4096
    event = replace(make_event("split-storage"), payload=payload)

    await store.put(event)
    claimed = (await store.claim_batch(limit=1))[0]
    await store.mark_sent(
        claimed,
        PublishResult(partition=0, offset=1, published_at=now),
    )

    payload_name = payload_blob_name(event.event_id)
    state_name = state_blob_name(event.event_id)
    payload_puts = [content for name, content in client.puts if name == payload_name]
    state_puts = [content for name, content in client.puts if name == state_name]

    assert payload_puts == [payload]
    assert len(state_puts) >= 3
    assert all(payload not in content for content in state_puts)
    assert all(len(content) < len(payload) for content in state_puts)
    assert await client.get_blob(payload_name) is not None
    assert await client.get_blob(state_name) is not None
    assert await client.get_blob(event_blob_name(event.event_id)) is None


@pytest.mark.asyncio
async def test_blob_store_reads_legacy_single_blob_records() -> None:
    now = datetime.now(UTC)
    client = InMemoryBlobClient()
    event = make_event("legacy-single-blob")
    record = StoredEvent(event=event, accepted_at=now)
    metadata = dict(blob_metadata(event, environment="default"))
    metadata["event_fingerprint"] = _event_fingerprint(event)
    await client.put_blob(
        event_blob_name(event.event_id),
        _encode_record(record),
        metadata,
    )
    store = BlobOutboxStore(client=client, clock=FixedClock(now))

    claimed = await store.claim_batch(limit=1)

    assert [claim.event.event_id for claim in claimed] == [event.event_id]
    assert claimed[0].event.payload == event.payload
    assert await client.get_blob(state_blob_name(event.event_id)) is not None
    assert await client.get_blob(payload_blob_name(event.event_id)) is not None


@pytest.mark.parametrize(
    ("store", "client"),
    [
        (
            CosmosStrongOutboxStore(
                CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
                client=(cosmos_client := CleanupCountingCosmosClient()),
            ),
            cosmos_client,
        ),
        (
            AzureSqlSyncOutboxStore(client=(sql_client := CleanupCountingSqlClient())),
            sql_client,
        ),
    ],
)
@pytest.mark.asyncio
async def test_database_cleanup_uses_bounded_cleanup_candidates(
    store: Any,
    client: Any,
) -> None:
    now = datetime.now(UTC)
    events = [make_event(f"db-cleanup-bound-{index}") for index in range(4)]
    for event in events:
        await store.put(event)
    claimed = await store.claim_batch(limit=4)
    for offset, claim in enumerate(claimed):
        await store.mark_sent(
            claim,
            PublishResult(partition=0, offset=offset, published_at=now),
        )
    client.list_records_calls = 0

    deleted = await store.cleanup_sent(
        now=events[0].expires_at + timedelta(seconds=1),
        safety_margin=timedelta(seconds=0),
        batch_size=2,
        max_per_tick=3,
    )

    assert deleted == 3
    assert client.list_records_calls == 0
    assert client.cleanup_candidate_limits == [3]


@pytest.mark.parametrize(
    "store_factory",
    [
        lambda clock: MemoryOutboxStore(clock=clock),
        lambda clock: BlobOutboxStore.for_testing(clock=clock),
        lambda clock: CosmosStrongOutboxStore.for_testing(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
            clock=clock,
        ),
        lambda clock: AzureSqlSyncOutboxStore.for_testing(clock=clock),
        lambda clock: SqlAlwaysOnOutboxStore.for_testing(clock=clock),
    ],
)
@pytest.mark.asyncio
async def test_provider_failover_replay_candidates_rolls_back_on_interruption(
    store_factory: Any,
) -> None:
    clock = InterruptingClock(datetime.now(UTC))
    store = store_factory(clock)
    events = [make_event("rollback-1"), make_event("rollback-2")]
    for event in events:
        await store.put(event)

    clock.interrupt_after(1)
    with pytest.raises(RuntimeError, match="clock interrupted"):
        await store.failover_replay_candidates(
            failover_started_at=datetime.now(UTC),
            limit=10,
        )

    for event in events:
        record = await _record_from_store(store, event.event_id)
        assert record.status is OutboxStatus.PENDING
        assert record.claim_token is None
        assert record.claimed_at is None
        assert record.attempt_count == 0


@pytest.mark.parametrize(
    ("first_store", "second_store"),
    [
        (
            BlobOutboxStore(client=(blob_client := InMemoryBlobClient())),
            BlobOutboxStore(client=blob_client),
        ),
        (
            CosmosStrongOutboxStore(
                CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
                client=(cosmos_client := InMemoryCosmosOutboxClient()),
            ),
            CosmosStrongOutboxStore(
                CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
                client=cosmos_client,
            ),
        ),
        (
            AzureSqlSyncOutboxStore(client=(sql_client := InMemorySqlOutboxClient())),
            AzureSqlSyncOutboxStore(client=sql_client),
        ),
    ],
)
@pytest.mark.asyncio
async def test_cleanup_freeze_survives_backend_reopen(
    first_store: Any,
    second_store: Any,
) -> None:
    event = make_event("persisted-freeze")
    await first_store.put(event)
    claimed = (await first_store.claim_batch(limit=1))[0]
    await first_store.mark_sent(
        claimed,
        PublishResult(partition=1, offset=2, published_at=datetime.now(UTC)),
    )

    await first_store.freeze_cleanup(reason="failover")
    frozen_deleted = await second_store.cleanup_sent(
        now=datetime.now(UTC) + timedelta(hours=1),
        safety_margin=timedelta(seconds=0),
    )
    await second_store.resume_cleanup()
    resumed_deleted = await second_store.cleanup_sent(
        now=datetime.now(UTC) + timedelta(hours=1),
        safety_margin=timedelta(seconds=0),
    )

    assert frozen_deleted == 0
    assert resumed_deleted == 1


@pytest.mark.asyncio
async def test_memory_cleanup_freeze_can_use_shared_state() -> None:
    cleanup_state = CleanupFreezeState()
    first_store = MemoryOutboxStore(cleanup_state=cleanup_state)
    second_store = MemoryOutboxStore(cleanup_state=cleanup_state)
    event = make_event("memory-persisted-freeze")
    await second_store.put(event)
    claimed = (await second_store.claim_batch(limit=1))[0]
    await second_store.mark_sent(
        claimed,
        PublishResult(partition=1, offset=2, published_at=datetime.now(UTC)),
    )

    await first_store.freeze_cleanup(reason="failover")
    deleted = await second_store.cleanup_sent(
        now=datetime.now(UTC) + timedelta(hours=1),
        safety_margin=timedelta(seconds=0),
    )

    assert deleted == 0
    assert second_store.cleanup_frozen is True


@pytest.mark.parametrize(
    ("store", "record_getter"),
    [
        (
            FakeOutboxStore(),
            lambda store, event_id: store.records.get(event_id),
        ),
        (
            BlobOutboxStore.for_testing(),
            lambda store, event_id: store.records.get(event_id),
        ),
        (
            DualRegionBlobOutboxStore.for_testing(),
            lambda store, event_id: store.primary.records.get(event_id),
        ),
        (
            CosmosStrongOutboxStore.for_testing(
                CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
            ),
            lambda store, event_id: store.client.get(event_id),
        ),
        (
            AzureSqlSyncOutboxStore.for_testing(),
            lambda store, event_id: store.client.get(event_id),
        ),
        (
            SqlAlwaysOnOutboxStore.for_testing(),
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

    repaired = await store.repair_failed_to_pending(event_id=event.event_id)

    record = await _maybe_await(record_getter(store, event.event_id))
    assert repaired is AdminActionStatus.SUCCESS
    assert record is not None
    assert record.status is OutboxStatus.PENDING
    assert record.failed_at is None
    assert record.attempt_count == 0
    assert record.last_error_type is None
    assert record.last_error is None
    assert record.next_attempt_at is None
    assert record.claim_token is None
    assert record.claimed_at is None


@pytest.mark.parametrize(
    ("store", "record_getter"),
    [
        (
            FakeOutboxStore(),
            lambda store, event_id: store.records.get(event_id),
        ),
        (
            BlobOutboxStore.for_testing(),
            lambda store, event_id: store.records.get(event_id),
        ),
        (
            DualRegionBlobOutboxStore.for_testing(),
            lambda store, event_id: store.primary.records.get(event_id),
        ),
        (
            CosmosStrongOutboxStore.for_testing(
                CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
            ),
            lambda store, event_id: store.client.get(event_id),
        ),
        (
            AzureSqlSyncOutboxStore.for_testing(),
            lambda store, event_id: store.client.get(event_id),
        ),
        (
            SqlAlwaysOnOutboxStore.for_testing(),
            lambda store, event_id: store.client.get(event_id),
        ),
    ],
)
@pytest.mark.asyncio
async def test_provider_replay_event_requeues_sent_event(
    store: Any,
    record_getter: Any,
) -> None:
    event = make_event("manual-replay")
    await store.put(event)
    claimed = (await store.claim_batch(limit=1))[0]
    await store.mark_sent(
        claimed,
        PublishResult(partition=1, offset=2, published_at=datetime.now(UTC)),
    )

    replayed = await store.replay_event(event_id=event.event_id)

    record = await _maybe_await(record_getter(store, event.event_id))
    assert replayed is AdminActionStatus.SUCCESS
    assert record is not None
    assert record.status is OutboxStatus.PENDING
    assert record.claim_token is None
    assert record.claimed_at is None
    assert record.next_attempt_at is None
    assert record.sent_at is None
    assert record.publish_result is None
    assert record.failed_at is None
    assert record.last_error_type is None
    assert record.last_error is None
    assert record.attempt_count == 1

    reclaimed = await store.claim_batch(limit=1)

    assert [claim.event.event_id for claim in reclaimed] == [event.event_id]
    assert reclaimed[0].attempt_count == 2


@pytest.mark.parametrize(
    ("store", "client"),
    [
        (
            CosmosStrongOutboxStore(
                CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
                client=CountingCosmosClient(),
            ),
            lambda store: store.client,
        ),
        (
            AzureSqlSyncOutboxStore(client=CountingSqlClient()),
            lambda store: store.client,
        ),
    ],
)
@pytest.mark.asyncio
async def test_sql_and_cosmos_claim_reuses_claim_candidate_list(
    store: Any,
    client: Any,
) -> None:
    await store.put(
        make_ordered_event(
            "candidate-list-first",
            ordering_key="customer-1",
            ordering_sequence=1,
        )
    )
    await store.put(
        make_ordered_event(
            "candidate-list-second",
            ordering_key="customer-1",
            ordering_sequence=2,
        )
    )

    claimed = await store.claim_batch(limit=10)

    assert [claim.event.event_id for claim in claimed] == ["candidate-list-first"]
    assert client(store).list_records_calls == 0
    assert client(store).claim_batch_pending_calls == 1


@pytest.mark.parametrize(
    ("store", "client"),
    [
        (
            CosmosStrongOutboxStore(
                CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
                client=CountingCosmosClient(),
            ),
            lambda store: store.client,
        ),
        (
            AzureSqlSyncOutboxStore(client=CountingSqlClient()),
            lambda store: store.client,
        ),
    ],
)
@pytest.mark.asyncio
async def test_sql_and_cosmos_failover_replay_uses_replay_candidate_list(
    store: Any,
    client: Any,
) -> None:
    first = make_event("replay-candidate-list-first")
    second = make_event("replay-candidate-list-second")
    await store.put(first)
    await store.put(second)

    claimed = await store.failover_replay_candidates(
        failover_started_at=datetime.now(UTC),
        limit=1,
        exclude_event_ids={first.event_id},
    )

    assert [claim.event.event_id for claim in claimed] == [second.event_id]
    assert client(store).list_records_calls == 0
    assert client(store).failover_replay_candidate_calls == 1


@pytest.mark.parametrize(
    ("store", "client"),
    [
        (
            CosmosStrongOutboxStore(
                CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
                client=ConflictOnceCosmosClient(),
            ),
            lambda store: store.client,
        ),
        (
            AzureSqlSyncOutboxStore(client=ConflictOnceSqlClient()),
            lambda store: store.client,
        ),
    ],
)
@pytest.mark.asyncio
async def test_sql_and_cosmos_repair_retry_cas_conflict(
    store: Any,
    client: Any,
) -> None:
    event = make_event("cas-repair")
    await store.put(event)
    claimed = (await store.claim_batch(limit=1))[0]
    await store.mark_failed(
        claimed,
        error_type="Fatal",
        error_message="stop",
    )
    client(store).remaining_conflicts = 1

    repaired = await store.repair_failed_to_pending(event_id=event.event_id)

    assert repaired is AdminActionStatus.SUCCESS
    record = await client(store).get(event.event_id)
    assert record.status is OutboxStatus.PENDING
    assert client(store).remaining_conflicts == 0


@pytest.mark.parametrize(
    ("store", "client"),
    [
        (
            CosmosStrongOutboxStore(
                CosmosConfiguration(consistency="Strong", regions=("westus", "eastus")),
                client=ConflictOnceCosmosClient(),
            ),
            lambda store: store.client,
        ),
        (
            AzureSqlSyncOutboxStore(client=ConflictOnceSqlClient()),
            lambda store: store.client,
        ),
    ],
)
@pytest.mark.asyncio
async def test_sql_and_cosmos_mark_sent_retry_cas_conflict(
    store: Any,
    client: Any,
) -> None:
    event = make_event("cas-mark-sent")
    await store.put(event)
    claimed = (await store.claim_batch(limit=1))[0]
    client(store).remaining_conflicts = 1

    await store.mark_sent(
        claimed,
        PublishResult(partition=1, offset=2, published_at=datetime.now(UTC)),
    )

    record = await client(store).get(event.event_id)
    assert record.status is OutboxStatus.SENT
    assert client(store).remaining_conflicts == 0


@pytest.mark.parametrize(
    "store",
    [
        FakeOutboxStore(),
        BlobOutboxStore.for_testing(),
        DualRegionBlobOutboxStore.for_testing(),
        CosmosStrongOutboxStore.for_testing(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        ),
        AzureSqlSyncOutboxStore.for_testing(),
        SqlAlwaysOnOutboxStore.for_testing(),
    ],
)
@pytest.mark.asyncio
async def test_provider_admin_actions_return_not_found_for_missing_event(
    store: Any,
) -> None:
    assert (
        await store.repair_failed_to_pending(event_id="missing")
        is AdminActionStatus.NOT_FOUND
    )
    assert await store.replay_event(event_id="missing") is AdminActionStatus.NOT_FOUND


@pytest.mark.parametrize(
    "store",
    [
        FakeOutboxStore(),
        BlobOutboxStore.for_testing(),
        DualRegionBlobOutboxStore.for_testing(),
        CosmosStrongOutboxStore.for_testing(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        ),
        AzureSqlSyncOutboxStore.for_testing(),
        SqlAlwaysOnOutboxStore.for_testing(),
    ],
)
@pytest.mark.asyncio
async def test_provider_repair_failed_reports_wrong_state_for_non_failed_event(
    store: Any,
) -> None:
    event = make_event("wrong-state-repair")
    await store.put(event)

    repaired = await store.repair_failed_to_pending(event_id=event.event_id)

    assert repaired is AdminActionStatus.WRONG_STATE


@pytest.mark.asyncio
async def test_memory_repair_unknown_event_is_noop() -> None:
    store = FakeOutboxStore()

    assert (
        await store.repair_failed_to_pending(event_id="missing")
        is AdminActionStatus.NOT_FOUND
    )


def test_sql_schema_contains_required_indexes() -> None:
    assert SQL_PENDING_INDEX_NAME in SQL_SCHEMA
    assert SQL_REPLAY_INDEX_NAME in SQL_SCHEMA
    assert SQL_ORDERED_INDEX_NAME in SQL_SCHEMA
    assert "accepted_at_utc     DATETIME2 NULL" in SQL_SCHEMA
    assert "publishing_mode      NVARCHAR(32)  NOT NULL" in SQL_SCHEMA
    assert "ordering_key         NVARCHAR(1024) NULL" in SQL_SCHEMA
    assert "published_at_utc     DATETIME2 NULL" in SQL_SCHEMA
    assert "publish_metadata_json NVARCHAR(MAX) NULL" in SQL_SCHEMA
    assert "last_error          NVARCHAR(2048) NULL" in SQL_SCHEMA


@pytest.mark.asyncio
async def test_azure_sql_sync_wait_timeout_is_retryable() -> None:
    store = AzureSqlSyncOutboxStore.for_testing(
        AzureSqlSyncConfiguration(sync_wait_succeeds=False)
    )

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
    with pytest.raises(ValueError, match="synchronized secondary"):
        SqlAlwaysOnOutboxStore(required_synchronized_secondaries=0)


@pytest.mark.asyncio
async def test_always_on_requires_configured_secondaries_on_put() -> None:
    store = SqlAlwaysOnOutboxStore(
        required_synchronized_secondaries=2,
        client=InMemorySqlOutboxClient(synchronized_secondaries=1),
    )

    with pytest.raises(RetryableStoreError, match="secondary"):
        await store.put(make_event())


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value
