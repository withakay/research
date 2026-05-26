from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import TYPE_CHECKING, Any, TypedDict

import pytest

from durable_outbox.core import ConfigurationError
from durable_outbox.core.errors import ClaimConflictError, DuplicateEventConflictError
from durable_outbox.core.model import OutboxEvent, OutboxStatus, PublishingMode
from durable_outbox.testing.provider_contract import make_event
from durable_outbox_cosmos_store import CosmosConfiguration, CosmosStoredEvent
from durable_outbox_cosmos_store.azure import (
    AzureCosmosOutboxClient,
    decode_cosmos_item,
    encode_cosmos_item,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class QueryCall(TypedDict):
    query: str
    parameters: list[dict[str, object]]
    partition_key: str
    max_item_count: int | None
    kwargs: dict[str, object]


class FakeContainer:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, object]] = {}
        self.query_results: dict[str, list[dict[str, object]]] = {}
        self.queries: list[QueryCall] = []
        self.created: list[dict[str, object]] = []
        self.replaced: list[tuple[str, dict[str, object], dict[str, object]]] = []
        self.deleted: list[tuple[str, str]] = []
        self.upserted: list[dict[str, object]] = []
        self.paged_queries: list[FakePagedQuery] = []
        self.next_etag = 1
        self.fail_replace = False

    async def read_item(self, *, item: str, partition_key: str) -> dict[str, object]:
        try:
            return dict(self.items[(partition_key, item)])
        except KeyError as exc:
            raise ResourceNotFoundError(item) from exc

    async def create_item(self, body: dict[str, object]) -> dict[str, object]:
        partition_key = str(body["pk"])
        item_id = str(body["id"])
        if (partition_key, item_id) in self.items:
            raise ResourceExistsError(item_id)
        stored = dict(body)
        stored["_etag"] = f'"{self.next_etag}"'
        self.next_etag += 1
        self.items[(partition_key, item_id)] = stored
        self.created.append(body)
        return dict(stored)

    async def upsert_item(self, body: dict[str, object]) -> dict[str, object]:
        partition_key = str(body["pk"])
        item_id = str(body["id"])
        stored = dict(body)
        stored["_etag"] = '"registry"'
        self.items[(partition_key, item_id)] = stored
        self.upserted.append(body)
        return dict(stored)

    async def replace_item(
        self,
        *,
        item: str,
        body: dict[str, object],
        etag: str | None,
        match_condition: object | None,
    ) -> dict[str, object]:
        _ = match_condition
        if self.fail_replace:
            raise ResourceModifiedError("etag mismatch")
        partition_key = str(body["pk"])
        current = self.items[(partition_key, item)]
        if etag is not None and current.get("_etag") != etag:
            raise ResourceModifiedError("etag mismatch")
        stored = dict(body)
        stored["_etag"] = f'"{self.next_etag}"'
        self.next_etag += 1
        self.items[(partition_key, item)] = stored
        self.replaced.append((item, body, {"etag": etag}))
        return dict(stored)

    async def delete_item(self, *, item: str, partition_key: str) -> None:
        self.deleted.append((item, partition_key))
        self.items.pop((partition_key, item), None)

    def query_items(
        self,
        query: str,
        *,
        parameters: list[dict[str, object]] | None = None,
        partition_key: str,
        max_item_count: int | None = None,
        **kwargs: object,
    ) -> FakePagedQuery:
        self.queries.append(
            {
                "query": query,
                "parameters": parameters or [],
                "partition_key": partition_key,
                "max_item_count": max_item_count,
                "kwargs": kwargs,
            }
        )
        paged_query = FakePagedQuery(
            self.query_results.get(partition_key, []),
            page_size=max_item_count or 1,
        )
        self.paged_queries.append(paged_query)
        return paged_query


class FakePagedQuery:
    def __init__(self, items: list[dict[str, object]], *, page_size: int = 1) -> None:
        self.items = items
        self.page_size = page_size
        self.pages_yielded = 0
        self.items_yielded = 0

    async def __aiter__(self) -> AsyncIterator[dict[str, object]]:
        for item in self.items:
            self.items_yielded += 1
            yield dict(item)

    async def by_page(
        self,
        continuation_token: str | None = None,
    ) -> AsyncIterator[AsyncIterator[dict[str, object]]]:
        start = int(continuation_token or "0")
        for index in range(start, len(self.items), self.page_size):
            self.pages_yielded += 1
            page = self.items[index : index + self.page_size]

            async def page_items(
                values: list[dict[str, object]] = page,
            ) -> AsyncIterator[dict[str, object]]:
                for item in values:
                    self.items_yielded += 1
                    yield dict(item)

            yield page_items()


class ResourceNotFoundError(Exception):
    pass


class ResourceModifiedError(Exception):
    pass


class ResourceExistsError(Exception):
    pass


class FakeDatabase:
    def __init__(self, container: FakeContainer) -> None:
        self.container = container

    def get_container_client(self, container_name: str) -> FakeContainer:
        assert container_name == "outbox"
        return self.container


class FakeCosmosClient:
    def __init__(self, container: FakeContainer) -> None:
        self.container = container
        self.closed = False
        self.account: dict[str, object] = {
            "consistencyPolicy": {"defaultConsistencyLevel": "Strong"},
            "readLocations": [{"name": "westus"}, {"name": "eastus"}],
            "writeLocations": [{"name": "westus"}],
        }

    @classmethod
    def from_connection_string(cls, connection_string: str) -> FakeCosmosClient:
        assert connection_string == "AccountEndpoint=https://example;"
        return cls(FakeContainer())

    def get_database_client(self, database_name: str) -> FakeDatabase:
        assert database_name == "db"
        return FakeDatabase(self.container)

    async def read_account(self) -> dict[str, object]:
        return self.account

    async def close(self) -> None:
        self.closed = True


class FakeCosmosModule:
    class aio:
        CosmosClient = FakeCosmosClient


class FakeAzureCoreModule:
    class MatchConditions:
        IfNotModified = object()


def test_cosmos_azure_module_does_not_import_azure_at_import_time() -> None:
    module = import_module("durable_outbox_cosmos_store.azure")

    assert module.AzureCosmosOutboxClient is AzureCosmosOutboxClient


def test_azure_cosmos_client_reports_missing_optional_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import_module = import_module

    def fail_azure_import(name: str) -> Any:
        if name == "azure.cosmos.aio":
            raise ModuleNotFoundError("No module named 'azure'")
        return real_import_module(name)

    monkeypatch.setattr(
        "durable_outbox_cosmos_store.azure.import_module", fail_azure_import
    )

    with pytest.raises(ConfigurationError, match="durable-outbox-cosmos-store"):
        AzureCosmosOutboxClient.from_connection_string(
            "AccountEndpoint=https://example;",
            database_name="db",
            container_name="outbox",
        )


def test_cosmos_record_round_trips_through_encoder() -> None:
    now = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    event = OutboxEvent(
        event_id="cosmos-round-trip",
        topic="durable.outbox.outputs",
        payload=b"payload",
        key=b"key",
        headers={"traceparent": b"00-abc"},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
        ordering_key="customer-1",
        ordering_sequence=9,
        publishing_mode=PublishingMode.ORDERED,
        schema_id="schema-1",
        schema_version="1",
    )
    record = CosmosStoredEvent(
        event=event,
        partition_key="durable.outbox.outputs#abc",
        version=7,
        etag='"etag"',
        status=OutboxStatus.IN_FLIGHT,
        accepted_at=now,
        attempt_count=2,
        claim_token="claim-token",  # noqa: S106
        claimed_at=now,
        next_attempt_at=now + timedelta(seconds=10),
        last_error_type="RetryablePublishError",
        last_error="temporary",
    )

    decoded = decode_cosmos_item(encode_cosmos_item(record) | {"_etag": '"etag"'})

    assert decoded == record


def test_from_connection_string_wires_database_and_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_import(name: str) -> Any:
        if name == "azure.cosmos.aio":
            return FakeCosmosModule.aio
        if name == "azure.core":
            return FakeAzureCoreModule
        return import_module(name)

    monkeypatch.setattr("durable_outbox_cosmos_store.azure.import_module", fake_import)

    client = AzureCosmosOutboxClient.from_connection_string(
        "AccountEndpoint=https://example;",
        database_name="db",
        container_name="outbox",
    )

    assert isinstance(client.container, FakeContainer)


@pytest.mark.asyncio
async def test_azure_cosmos_insert_get_replace_and_delete_use_partition_key() -> None:
    container = FakeContainer()
    client = AzureCosmosOutboxClient(container)
    event = make_event("cosmos-point")
    record = CosmosStoredEvent(event=event, partition_key="topic#0")

    inserted = await client.insert(record)
    fetched = await client.get(event.event_id)
    replaced = await client.replace(inserted, expected_version=inserted.version)
    await client.delete(event.event_id)

    assert inserted.etag == '"2"'
    assert fetched == inserted
    assert replaced.etag == '"4"'
    assert container.created[0]["id"] == _event_index_id(event.event_id)
    assert container.created[0]["pk"] == "__control__"
    assert container.created[0]["kind"] == "event_index"
    assert container.created[0]["event_id"] == event.event_id
    assert container.created[0]["target_id"] == event.event_id
    assert container.created[0]["partition_key"] == "topic#0"
    assert container.created[0]["state"] == "reserved"
    assert isinstance(container.created[0]["fingerprint"], str)
    assert container.created[1]["pk"] == "topic#0"
    assert container.replaced[0][0] == _event_index_id(event.event_id)
    assert container.replaced[0][1]["state"] == "committed"
    assert container.replaced[0][2]["etag"] == '"1"'
    assert container.replaced[1][2]["etag"] == '"2"'
    assert container.deleted == [
        (event.event_id, "topic#0"),
        (_event_index_id(event.event_id), "__control__"),
    ]


@pytest.mark.asyncio
async def test_azure_cosmos_insert_persists_partition_registry_item() -> None:
    container = FakeContainer()
    client = AzureCosmosOutboxClient(container)

    await client.insert(
        CosmosStoredEvent(
            event=make_event("cosmos-registry-insert"),
            partition_key="durable.outbox.outputs#0",
        )
    )

    assert container.upserted == [
        {
            "id": "partition#durable.outbox.outputs#0",
            "pk": "__control__",
            "kind": "partition_registry",
            "partition_key": "durable.outbox.outputs#0",
        }
    ]


@pytest.mark.asyncio
async def test_azure_cosmos_get_resolves_partition_from_event_index_after_restart() -> (
    None
):
    container = FakeContainer()
    first_client = AzureCosmosOutboxClient(container)
    event = make_event("cosmos-indexed-get")
    inserted = await first_client.insert(
        CosmosStoredEvent(event=event, partition_key="durable.outbox.outputs#0")
    )
    restarted_client = AzureCosmosOutboxClient(container)

    fetched = await restarted_client.get(event.event_id)

    assert fetched == inserted
    assert restarted_client.partition_keys_by_event_id[event.event_id] == (
        "durable.outbox.outputs#0"
    )


@pytest.mark.asyncio
async def test_azure_cosmos_compatible_duplicate_insert_uses_event_index() -> None:
    container = FakeContainer()
    first_client = AzureCosmosOutboxClient(container)
    second_client = AzureCosmosOutboxClient(container)
    event = make_event("cosmos-index-duplicate")
    inserted = await first_client.insert(
        CosmosStoredEvent(event=event, partition_key="durable.outbox.outputs#0")
    )

    duplicate = await second_client.insert(
        CosmosStoredEvent(event=event, partition_key="durable.outbox.outputs#1")
    )

    assert duplicate == inserted
    event_creates = [
        item
        for item in container.created
        if item.get("kind") != "event_index" and item["id"] == event.event_id
    ]
    assert len(event_creates) == 1


@pytest.mark.asyncio
async def test_azure_cosmos_incompatible_duplicate_insert_uses_event_index() -> None:
    container = FakeContainer()
    first_client = AzureCosmosOutboxClient(container)
    second_client = AzureCosmosOutboxClient(container)
    event = make_event("cosmos-index-incompatible")
    await first_client.insert(
        CosmosStoredEvent(event=event, partition_key="durable.outbox.outputs#0")
    )
    changed = replace(event, payload=b'{"changed":true}')

    with pytest.raises(DuplicateEventConflictError, match="payload"):
        await second_client.insert(
            CosmosStoredEvent(event=changed, partition_key="durable.outbox.outputs#1")
        )


@pytest.mark.asyncio
async def test_azure_cosmos_get_removes_stale_event_index() -> None:
    container = FakeContainer()
    event_id = "cosmos-stale-index"
    container.items[("__control__", _event_index_id(event_id))] = {
        "id": _event_index_id(event_id),
        "pk": "__control__",
        "kind": "event_index",
        "event_id": event_id,
        "target_id": event_id,
        "partition_key": "durable.outbox.outputs#0",
        "fingerprint": "fingerprint",
        "state": "committed",
    }
    client = AzureCosmosOutboxClient(container)

    assert await client.get(event_id) is None
    assert container.deleted == [(_event_index_id(event_id), "__control__")]


@pytest.mark.asyncio
async def test_azure_cosmos_repairs_reserved_event_index_without_target() -> None:
    container = FakeContainer()
    event = make_event("cosmos-repair-reserved-index")
    container.items[("__control__", _event_index_id(event.event_id))] = {
        "id": _event_index_id(event.event_id),
        "pk": "__control__",
        "kind": "event_index",
        "event_id": event.event_id,
        "target_id": event.event_id,
        "partition_key": "durable.outbox.outputs#0",
        "fingerprint": "fingerprint",
        "state": "reserved",
    }
    client = AzureCosmosOutboxClient(container)

    repaired = await client.repair_event_index(event.event_id)
    inserted = await client.insert(
        CosmosStoredEvent(event=event, partition_key="durable.outbox.outputs#0")
    )

    assert repaired is True
    assert container.deleted == [(_event_index_id(event.event_id), "__control__")]
    assert inserted.event.event_id == event.event_id


@pytest.mark.asyncio
async def test_azure_cosmos_repair_keeps_event_index_when_target_exists() -> None:
    container = FakeContainer()
    client = AzureCosmosOutboxClient(container)
    event = make_event("cosmos-repair-keeps-index")
    await client.insert(
        CosmosStoredEvent(event=event, partition_key="durable.outbox.outputs#0")
    )
    container.deleted.clear()

    repaired = await client.repair_event_index(event.event_id)

    assert repaired is False
    assert container.deleted == []


@pytest.mark.asyncio
async def test_azure_cosmos_replace_conflict_maps_to_claim_conflict() -> None:
    container = FakeContainer()
    client = AzureCosmosOutboxClient(container)
    inserted = await client.insert(
        CosmosStoredEvent(event=make_event("cosmos-conflict"), partition_key="topic#0")
    )
    container.fail_replace = True

    with pytest.raises(ClaimConflictError):
        await client.replace(inserted, expected_version=inserted.version)


@pytest.mark.asyncio
async def test_azure_cosmos_validate_account_checks_rpo_zero_shape() -> None:
    container = FakeContainer()
    sdk_client = FakeCosmosClient(container)
    client = AzureCosmosOutboxClient(container, account_client=sdk_client)

    await client.validate_account(
        CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
    )

    sdk_client.account["consistencyPolicy"] = {"defaultConsistencyLevel": "Session"}
    with pytest.raises(ConfigurationError, match="strong"):
        await client.validate_account(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        )


@pytest.mark.asyncio
async def test_azure_cosmos_validate_account_rejects_single_region_and_multi_write() -> (
    None
):
    container = FakeContainer()
    sdk_client = FakeCosmosClient(container)
    client = AzureCosmosOutboxClient(container, account_client=sdk_client)
    sdk_client.account["readLocations"] = [{"name": "westus"}]
    sdk_client.account["writeLocations"] = [{"name": "westus"}]

    with pytest.raises(ConfigurationError, match="regions"):
        await client.validate_account(
            CosmosConfiguration(consistency="Strong", regions=("westus", "eastus"))
        )

    sdk_client.account["readLocations"] = [{"name": "westus"}, {"name": "eastus"}]
    sdk_client.account["writeLocations"] = [{"name": "westus"}, {"name": "eastus"}]
    with pytest.raises(ConfigurationError, match="single-write"):
        await client.validate_account(
            CosmosConfiguration(
                consistency="Strong",
                regions=("westus", "eastus"),
                multi_write=True,
            )
        )


@pytest.mark.asyncio
async def test_azure_cosmos_candidate_queries_remain_explicitly_unsupported() -> None:
    client = AzureCosmosOutboxClient(FakeContainer())

    with pytest.raises(ConfigurationError, match="cross-partition"):
        await client.list_records()


@pytest.mark.asyncio
async def test_azure_cosmos_claim_uses_partition_scoped_queries() -> None:
    now = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    container = FakeContainer()
    claimable = _record(
        "claimable",
        partition_key="durable.outbox.outputs#0",
        created_at=now - timedelta(minutes=3),
    )
    fresh_blocker = _record(
        "fresh-blocker",
        partition_key="durable.outbox.outputs#1",
        created_at=now - timedelta(minutes=2),
        status=OutboxStatus.IN_FLIGHT,
        claimed_at=now - timedelta(seconds=10),
        ordering_key="customer-1",
    )
    container.query_results = {
        "durable.outbox.outputs#0": [encode_cosmos_item(claimable) | {"_etag": '"1"'}],
        "durable.outbox.outputs#1": [
            encode_cosmos_item(fresh_blocker) | {"_etag": '"2"'}
        ],
    }
    client = AzureCosmosOutboxClient(
        container,
        use_partition_registry=False,
        known_partition_keys=("durable.outbox.outputs#1", "durable.outbox.outputs#0"),
    )

    records = await client.claim_batch_pending(
        limit=1,
        now=now,
        claim_timeout=timedelta(minutes=5),
    )

    assert [record.event.event_id for record in records] == [
        "claimable",
        "fresh-blocker",
    ]
    assert [query["partition_key"] for query in container.queries] == [
        "durable.outbox.outputs#0",
        "durable.outbox.outputs#1",
    ]
    assert all(
        "enable_cross_partition_query" not in query["kwargs"]
        for query in container.queries
    )
    assert "ORDER BY c.topic" in str(container.queries[0]["query"])
    assert "@stale_claimed_before_epoch_ms" in {
        item["name"] for item in container.queries[0]["parameters"]
    }


@pytest.mark.asyncio
async def test_azure_cosmos_replay_uses_partition_scoped_queries() -> None:
    now = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    first = _record(
        "ordered-first",
        partition_key="durable.outbox.outputs#ordered",
        created_at=now - timedelta(minutes=3),
        ordering_key="customer-1",
        ordering_sequence=1,
    )
    second = _record(
        "ordered-second",
        partition_key="durable.outbox.outputs#ordered",
        created_at=now - timedelta(minutes=2),
        ordering_key="customer-1",
        ordering_sequence=2,
    )
    expired = _record(
        "expired",
        partition_key="durable.outbox.outputs#ordered",
        created_at=now - timedelta(minutes=4),
        expires_at=now - timedelta(minutes=1),
    )
    container = FakeContainer()
    container.query_results = {
        "durable.outbox.outputs#ordered": [
            encode_cosmos_item(expired) | {"_etag": '"1"'},
            encode_cosmos_item(first) | {"_etag": '"2"'},
            encode_cosmos_item(second) | {"_etag": '"3"'},
        ],
    }
    client = AzureCosmosOutboxClient(
        container,
        use_partition_registry=False,
        known_partition_keys=("durable.outbox.outputs#ordered",),
    )

    records = await client.list_failover_replay_candidates(
        failover_started_at=now,
        limit=5,
        exclude_event_ids={"already-seen"},
    )

    assert [record.event.event_id for record in records] == ["ordered-first"]
    assert container.queries[0]["partition_key"] == "durable.outbox.outputs#ordered"
    assert "expires_at_epoch_ms" in str(container.queries[0]["query"])
    assert "@exclude_event_ids" in {
        item["name"] for item in container.queries[0]["parameters"]
    }


@pytest.mark.asyncio
async def test_azure_cosmos_streams_replay_candidates_without_materializing_partitions() -> (
    None
):
    now = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    earlier = _record(
        "cosmos-stream-earlier",
        partition_key="durable.outbox.outputs#2",
        created_at=now - timedelta(minutes=3),
    )
    later = _record(
        "cosmos-stream-later",
        partition_key="durable.outbox.outputs#1",
        created_at=now - timedelta(minutes=2),
    )
    skipped_same_scope = _record(
        "cosmos-stream-same-scope",
        partition_key="durable.outbox.outputs#2",
        created_at=now - timedelta(minutes=1),
        ordering_key="customer-1",
    )
    first_same_scope = _record(
        "cosmos-stream-first-scope",
        partition_key="durable.outbox.outputs#1",
        created_at=now - timedelta(minutes=4),
        ordering_key="customer-1",
    )
    container = FakeContainer()
    container.query_results = {
        "durable.outbox.outputs#1": [
            encode_cosmos_item(first_same_scope) | {"_etag": '"1"'},
            encode_cosmos_item(later) | {"_etag": '"2"'},
        ],
        "durable.outbox.outputs#2": [
            encode_cosmos_item(earlier) | {"_etag": '"3"'},
            encode_cosmos_item(skipped_same_scope) | {"_etag": '"4"'},
        ],
    }
    client = AzureCosmosOutboxClient(
        container,
        use_partition_registry=False,
        known_partition_keys=("durable.outbox.outputs#1", "durable.outbox.outputs#2"),
    )

    records = [
        record
        async for record in client.iter_failover_replay_candidates(
            failover_started_at=now,
            limit=3,
            exclude_event_ids={"already-seen"},
        )
    ]

    assert [record.event.event_id for record in records] == [
        "cosmos-stream-first-scope",
        "cosmos-stream-earlier",
        "cosmos-stream-later",
    ]
    assert [query["partition_key"] for query in container.queries] == [
        "durable.outbox.outputs#1",
        "durable.outbox.outputs#2",
    ]
    assert all(query["max_item_count"] == 3 for query in container.queries)


@pytest.mark.asyncio
async def test_azure_cosmos_replay_stream_stops_before_later_pages() -> None:
    now = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    container = FakeContainer()
    container.query_results = {
        "durable.outbox.outputs#0": [
            encode_cosmos_item(
                _record(
                    f"cosmos-stream-page-{index}",
                    partition_key="durable.outbox.outputs#0",
                    created_at=now + timedelta(seconds=index),
                )
            )
            | {"_etag": f'"{index}"'}
            for index in range(5)
        ]
    }
    client = AzureCosmosOutboxClient(
        container,
        use_partition_registry=False,
        known_partition_keys=("durable.outbox.outputs#0",),
    )

    records = [
        record
        async for record in client.iter_failover_replay_candidates(
            failover_started_at=now,
            limit=1,
            page_size=1,
        )
    ]

    assert [record.event.event_id for record in records] == ["cosmos-stream-page-0"]
    assert container.paged_queries[0].pages_yielded == 1


@pytest.mark.asyncio
async def test_azure_cosmos_cleanup_uses_partition_scoped_queries() -> None:
    now = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    expired_sent = _record(
        "expired-sent",
        partition_key="durable.outbox.outputs#0",
        status=OutboxStatus.SENT,
        expires_at=now - timedelta(hours=2),
    )
    fresh_sent = _record(
        "fresh-sent",
        partition_key="durable.outbox.outputs#0",
        status=OutboxStatus.SENT,
        expires_at=now - timedelta(minutes=1),
    )
    container = FakeContainer()
    container.query_results = {
        "durable.outbox.outputs#0": [
            encode_cosmos_item(fresh_sent) | {"_etag": '"1"'},
            encode_cosmos_item(expired_sent) | {"_etag": '"2"'},
        ],
    }
    client = AzureCosmosOutboxClient(
        container,
        use_partition_registry=False,
        known_partition_keys=("durable.outbox.outputs#0",),
    )

    records = await client.list_cleanup_candidates(
        now=now,
        safety_margin=timedelta(minutes=30),
        limit=1,
    )

    assert [record.event.event_id for record in records] == ["expired-sent"]
    assert container.queries[0]["partition_key"] == "durable.outbox.outputs#0"
    assert "status = @sent_status" in str(container.queries[0]["query"])


@pytest.mark.asyncio
async def test_azure_cosmos_queries_return_empty_without_known_partitions() -> None:
    container = FakeContainer()
    client = AzureCosmosOutboxClient(container)

    assert (
        await client.claim_batch_pending(
            limit=1,
            now=datetime.now(UTC),
            claim_timeout=timedelta(minutes=5),
        )
        == ()
    )
    assert (
        await client.list_failover_replay_candidates(
            failover_started_at=datetime.now(UTC),
            limit=1,
        )
        == ()
    )
    assert (
        await client.list_cleanup_candidates(
            now=datetime.now(UTC),
            safety_margin=timedelta(seconds=0),
        )
        == ()
    )
    assert [query["partition_key"] for query in container.queries] == [
        "__control__",
        "__control__",
        "__control__",
    ]


@pytest.mark.asyncio
async def test_azure_cosmos_queries_load_registered_partitions_before_querying() -> (
    None
):
    now = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    registered_partition = "durable.outbox.outputs#registry"
    claimable = _record(
        "registered-claimable",
        partition_key=registered_partition,
        created_at=now - timedelta(minutes=3),
    )
    container = FakeContainer()
    container.query_results = {
        "__control__": [
            {
                "id": f"partition#{registered_partition}",
                "pk": "__control__",
                "kind": "partition_registry",
                "partition_key": registered_partition,
            }
        ],
        registered_partition: [encode_cosmos_item(claimable) | {"_etag": '"1"'}],
    }
    client = AzureCosmosOutboxClient(container)

    records = await client.claim_batch_pending(
        limit=1,
        now=now,
        claim_timeout=timedelta(minutes=5),
    )

    assert [record.event.event_id for record in records] == ["registered-claimable"]
    assert [query["partition_key"] for query in container.queries] == [
        "__control__",
        registered_partition,
    ]


@pytest.mark.asyncio
async def test_azure_cosmos_claim_stops_after_bounded_query_page() -> None:
    now = datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    partition_key = "durable.outbox.outputs#bounded"
    first = _record(
        "bounded-claim-first",
        partition_key=partition_key,
        created_at=now - timedelta(minutes=3),
    )
    second = _record(
        "bounded-claim-second",
        partition_key=partition_key,
        created_at=now - timedelta(minutes=2),
    )
    container = FakeContainer()
    container.query_results = {
        partition_key: [
            encode_cosmos_item(first) | {"_etag": '"1"'},
            encode_cosmos_item(second) | {"_etag": '"2"'},
        ],
    }
    client = AzureCosmosOutboxClient(
        container,
        use_partition_registry=False,
        known_partition_keys=(partition_key,),
    )

    records = await client.claim_batch_pending(
        limit=1,
        now=now,
        claim_timeout=timedelta(minutes=5),
    )

    assert [record.event.event_id for record in records] == ["bounded-claim-first"]
    assert container.queries[0]["max_item_count"] == 1
    assert container.paged_queries[0].pages_yielded == 1
    assert container.paged_queries[0].items_yielded == 1


def _record(
    event_id: str,
    *,
    partition_key: str,
    created_at: datetime | None = None,
    expires_at: datetime | None = None,
    status: OutboxStatus = OutboxStatus.PENDING,
    claimed_at: datetime | None = None,
    ordering_key: str | None = None,
    ordering_sequence: int | None = None,
) -> CosmosStoredEvent:
    now = created_at or (
        expires_at - timedelta(hours=1)
        if expires_at is not None
        else datetime(2026, 5, 26, 12, 0, tzinfo=UTC)
    )
    event = replace(
        make_event(event_id, ordering_key=ordering_key),
        created_at=now,
        expires_at=expires_at or now + timedelta(hours=1),
        publishing_mode=(
            PublishingMode.ORDERED
            if ordering_key is not None
            else PublishingMode.UNORDERED
        ),
        ordering_sequence=ordering_sequence,
    )
    return CosmosStoredEvent(
        event=event,
        partition_key=partition_key,
        version=1,
        etag='"1"',
        status=status,
        claimed_at=claimed_at,
    )


def _event_index_id(event_id: str) -> str:
    return f"event#{hashlib.sha256(event_id.encode('utf-8')).hexdigest()}"
