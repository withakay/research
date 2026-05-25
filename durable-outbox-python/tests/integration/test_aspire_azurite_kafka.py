from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from durable_outbox.core import OutboxDispatcher, OutboxEvent
from durable_outbox.sinks.file import FileSink
from durable_outbox.sinks.kafka import KafkaProducerConfig, KafkaSink
from durable_outbox.stores.azure_blob import AzureBlobClient
from durable_outbox.stores.blob_geo import BlobOutboxStore

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration


def _connection_string() -> str:
    return (
        os.environ.get("DURABLE_OUTBOX_AZURITE_CONNECTION_STRING")
        or os.environ.get("ConnectionStrings__blobs")  # noqa: SIM112
        or os.environ.get("ConnectionStrings__storage")  # noqa: SIM112
        or ""
    )


def _container_name() -> str:
    return os.environ.get("DURABLE_OUTBOX_AZURITE_CONTAINER", "durable-outbox")


def _kafka_bootstrap_servers() -> str:
    return (
        os.environ.get("DURABLE_OUTBOX_KAFKA_BOOTSTRAP_SERVERS")
        or os.environ.get("ConnectionStrings__kafka")  # noqa: SIM112
        or ""
    )


def _event(event_id: str) -> OutboxEvent:
    now = datetime.now(UTC)
    return OutboxEvent(
        event_id=event_id,
        topic=os.environ.get("DURABLE_OUTBOX_KAFKA_TOPIC", "durable-outbox-it"),
        payload=json.dumps({"event_id": event_id}).encode(),
        key=event_id.encode(),
        headers={"content-type": b"application/json"},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
    )


@pytest.mark.asyncio
async def test_azurite_blob_store_dispatches_to_local_file(tmp_path: Path) -> None:
    connection_string = _connection_string()
    if not connection_string:
        pytest.skip("set DURABLE_OUTBOX_AZURITE_CONNECTION_STRING or run via Aspire")

    client = AzureBlobClient.from_connection_string(
        connection_string,
        container_name=_container_name(),
    )
    await client.ensure_container()
    store = BlobOutboxStore(client=client, environment="integration")
    sink = FileSink(tmp_path / "published.jsonl")
    event = _event("azurite-file-1")

    try:
        receipt = await store.put(event)
        summary = await OutboxDispatcher(store, sink).run_once(limit=10)
    finally:
        await sink.aclose()
        await client.close()

    rows = [
        json.loads(line)
        for line in (tmp_path / "published.jsonl").read_text().splitlines()
    ]
    assert receipt.event_id == event.event_id
    assert summary.sent == 1
    assert rows[0]["event_id"] == event.event_id


@pytest.mark.asyncio
async def test_azurite_blob_store_dispatches_to_real_kafka() -> None:
    connection_string = _connection_string()
    bootstrap_servers = _kafka_bootstrap_servers()
    if not connection_string or not bootstrap_servers:
        pytest.skip(
            "set Azurite and Kafka connection env vars, or run through Aspire AppHost"
        )

    client = AzureBlobClient.from_connection_string(
        connection_string,
        container_name=_container_name(),
    )
    await client.ensure_container()
    store = BlobOutboxStore(client=client, environment="integration")
    sink = KafkaSink.from_config(
        KafkaProducerConfig(
            {"bootstrap.servers": bootstrap_servers},
            certified_mode=False,
        ),
        delivery_timeout_seconds=30,
    )
    event = _event("azurite-kafka-1")

    try:
        await store.put(event)
        summary = await OutboxDispatcher(store, sink).run_once(limit=10)
    finally:
        sink.close()
        await client.close()

    assert summary.sent == 1
