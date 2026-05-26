from __future__ import annotations

from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any

import pytest

from durable_outbox.core import ConfigurationError
from durable_outbox.core.errors import ClaimConflictError
from durable_outbox.core.model import OutboxEvent, OutboxStatus, PublishingMode
from durable_outbox.stores.cosmos import CosmosConfiguration, CosmosStoredEvent
from durable_outbox.stores.cosmos_azure import (
    AzureCosmosOutboxClient,
    decode_cosmos_item,
    encode_cosmos_item,
)
from durable_outbox.testing.provider_contract import make_event


class FakeContainer:
    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, object]] = {}
        self.created: list[dict[str, object]] = []
        self.replaced: list[tuple[str, dict[str, object], dict[str, object]]] = []
        self.deleted: list[tuple[str, str]] = []
        self.next_etag = 1
        self.fail_replace = False

    async def read_item(self, *, item: str, partition_key: str) -> dict[str, object]:
        return dict(self.items[(partition_key, item)])

    async def create_item(self, body: dict[str, object]) -> dict[str, object]:
        partition_key = str(body["pk"])
        item_id = str(body["id"])
        stored = dict(body)
        stored["_etag"] = f'"{self.next_etag}"'
        self.next_etag += 1
        self.items[(partition_key, item_id)] = stored
        self.created.append(body)
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


class ResourceNotFoundError(Exception):
    pass


class ResourceModifiedError(Exception):
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
    module = import_module("durable_outbox.stores.cosmos_azure")

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
        "durable_outbox.stores.cosmos_azure.import_module", fail_azure_import
    )

    with pytest.raises(ConfigurationError, match="durable-outbox\\[azure\\]"):
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

    monkeypatch.setattr("durable_outbox.stores.cosmos_azure.import_module", fake_import)

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

    assert inserted.etag == '"1"'
    assert fetched == inserted
    assert replaced.etag == '"2"'
    assert container.created[0]["pk"] == "topic#0"
    assert container.replaced[0][2]["etag"] == '"1"'
    assert container.deleted == [(event.event_id, "topic#0")]


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
    with pytest.raises(ConfigurationError, match="partition-scoped"):
        await client.claim_batch_pending(
            limit=1,
            now=datetime.now(UTC),
            claim_timeout=timedelta(minutes=5),
        )
    with pytest.raises(ConfigurationError, match="partition-scoped"):
        await client.list_failover_replay_candidates(
            failover_started_at=datetime.now(UTC),
            limit=1,
        )
    with pytest.raises(ConfigurationError, match="partition-scoped"):
        await client.list_cleanup_candidates(
            now=datetime.now(UTC),
            safety_margin=timedelta(seconds=0),
        )
