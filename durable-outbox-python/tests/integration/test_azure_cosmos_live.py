from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from durable_outbox.core import DuplicateEventConflictError, OutboxEvent
from durable_outbox.core.model import PublishResult
from durable_outbox_cosmos_store import (
    CosmosConfiguration,
    CosmosStoredEvent,
    CosmosStrongOutboxStore,
)
from durable_outbox_cosmos_store.azure import AzureCosmosOutboxClient

pytestmark = pytest.mark.integration


def _connection_settings() -> tuple[str, str, str]:
    if os.environ.get("DURABLE_OUTBOX_COSMOS_LIVE") != "1":
        pytest.skip("set DURABLE_OUTBOX_COSMOS_LIVE=1 to run live Cosmos tests")
    connection_string = os.environ.get("DURABLE_OUTBOX_COSMOS_CONNECTION_STRING", "")
    database_name = os.environ.get("DURABLE_OUTBOX_COSMOS_DATABASE", "")
    container_name = os.environ.get("DURABLE_OUTBOX_COSMOS_CONTAINER", "")
    if not connection_string or not database_name or not container_name:
        pytest.fail(
            "DURABLE_OUTBOX_COSMOS_CONNECTION_STRING, "
            "DURABLE_OUTBOX_COSMOS_DATABASE, and DURABLE_OUTBOX_COSMOS_CONTAINER are "
            "required when DURABLE_OUTBOX_COSMOS_LIVE=1"
        )
    return connection_string, database_name, container_name


def _client() -> AzureCosmosOutboxClient:
    connection_string, database_name, container_name = _connection_settings()
    return AzureCosmosOutboxClient.from_connection_string(
        connection_string,
        database_name=database_name,
        container_name=container_name,
    )


def _config(*, certified_mode: bool = False) -> CosmosConfiguration:
    regions = tuple(
        region.strip()
        for region in os.environ.get("DURABLE_OUTBOX_COSMOS_REGIONS", "local").split(
            ","
        )
        if region.strip()
    )
    return CosmosConfiguration(
        consistency=os.environ.get("DURABLE_OUTBOX_COSMOS_CONSISTENCY", "Session"),
        regions=regions or ("local",),
        multi_write=os.environ.get("DURABLE_OUTBOX_COSMOS_MULTI_WRITE") == "1",
        certified_mode=certified_mode,
        unordered_buckets=4,
    )


def _event(
    event_id: str,
    *,
    payload: bytes | None = None,
    expired: bool = False,
) -> OutboxEvent:
    now = datetime.now(UTC)
    created_at = now - timedelta(hours=2) if expired else now
    expires_at = now - timedelta(hours=1) if expired else now + timedelta(minutes=15)
    return OutboxEvent(
        event_id=event_id,
        topic="durable.outbox.live.cosmos",
        payload=payload or f'{{"event_id":"{event_id}"}}'.encode(),
        key=event_id.encode(),
        headers={"content-type": b"application/json"},
        created_at=created_at,
        expires_at=expires_at,
    )


def _event_index_id(event_id: str) -> str:
    return f"event#{hashlib.sha256(event_id.encode()).hexdigest()}"


@pytest.mark.asyncio
async def test_live_cosmos_store_restart_duplicate_claim_and_cleanup_paths() -> None:
    event_id = f"cosmos-live-{uuid4().hex}"
    expired_event_id = f"cosmos-live-expired-{uuid4().hex}"
    client = _client()
    restarted_client = _client()
    store = CosmosStrongOutboxStore(_config(), client=client)
    restarted_store = CosmosStrongOutboxStore(_config(), client=restarted_client)
    try:
        event = _event(event_id)
        receipt = await store.put(event)
        duplicate = await restarted_store.put(event)
        with pytest.raises(DuplicateEventConflictError):
            await restarted_store.put(_event(event_id, payload=b"different"))

        fetched_after_restart = await restarted_client.get(event_id)
        assert fetched_after_restart is not None
        assert fetched_after_restart.event == event
        assert receipt.event_id == duplicate.event_id == event_id
        replay_candidates = [
            record
            async for record in restarted_client.iter_failover_replay_candidates(
                failover_started_at=event.created_at - timedelta(seconds=1),
                limit=1,
                page_size=1,
            )
        ]
        assert [record.event.event_id for record in replay_candidates] == [event_id]

        expired = _event(expired_event_id, expired=True)
        await restarted_store.put(expired)
        claimed = await restarted_store.claim_batch(limit=1)
        assert [item.event.event_id for item in claimed] == [expired_event_id]
        await restarted_store.mark_sent(
            claimed[0],
            PublishResult(
                partition=0,
                offset=1,
                published_at=datetime.now(UTC),
                metadata={"provider": "cosmos"},
            ),
        )
        deleted = await restarted_store.cleanup_sent(
            now=datetime.now(UTC),
            safety_margin=timedelta(),
            max_per_tick=10,
        )
        assert deleted == 1
        assert await restarted_client.get(expired_event_id) is None
    finally:
        await restarted_client.delete(event_id)
        await restarted_client.delete(expired_event_id)
        await client.close()
        await restarted_client.close()


@pytest.mark.asyncio
async def test_live_cosmos_account_validation_when_certification_env_is_set() -> None:
    if os.environ.get("DURABLE_OUTBOX_COSMOS_CERTIFY_ACCOUNT") != "1":
        pytest.skip("set DURABLE_OUTBOX_COSMOS_CERTIFY_ACCOUNT=1 for account checks")
    client = _client()
    try:
        await client.validate_account(_config(certified_mode=True))
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_live_cosmos_repairs_dangling_event_index() -> None:
    event_id = f"cosmos-live-repair-{uuid4().hex}"
    client = _client()
    try:
        event = _event(event_id)
        partition_key = CosmosStrongOutboxStore(
            _config(), client=client
        ).partition_key_for(event)
        await client.insert(CosmosStoredEvent(event=event, partition_key=partition_key))
        await client.delete(event_id)
        repaired = await client.repair_event_index(event_id)
        assert repaired is False

        await client.container.create_item(
            {
                "id": _event_index_id(event_id),
                "pk": "__control__",
                "kind": "event_index",
                "event_id": event_id,
                "target_id": event_id,
                "partition_key": partition_key,
                "fingerprint": "live-test",
                "state": "reserved",
            }
        )
        assert await client.repair_event_index(event_id) is True
    finally:
        await client.delete(event_id)
        await client.close()
