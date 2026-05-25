import asyncio
import json
import os
import time
from collections.abc import Iterable
from typing import Protocol, cast
from uuid import uuid4

import httpx
import pytest
from confluent_kafka import Consumer, KafkaError
from durable_outbox.stores.azure_blob import AzureBlobClient

pytestmark = pytest.mark.integration


class KafkaMessage(Protocol):
    def topic(self) -> str: ...

    def key(self) -> bytes | None: ...

    def value(self) -> bytes | None: ...

    def headers(self) -> list[tuple[str, str | bytes | None]] | None: ...

    def error(self) -> KafkaError | None: ...


def _base_url() -> str:
    return os.environ.get("DURABLE_OUTBOX_FASTAPI_BASE_URL", "http://127.0.0.1:18088")


def _blob_connection_string() -> str:
    return (
        os.environ.get("DURABLE_OUTBOX_AZURITE_CONNECTION_STRING")
        or os.environ.get("ConnectionStrings__blobs")
        or os.environ.get("ConnectionStrings__storage")
        or ""
    )


def _kafka_bootstrap_servers() -> str:
    return (
        os.environ.get("DURABLE_OUTBOX_KAFKA_BOOTSTRAP_SERVERS")
        or os.environ.get("ConnectionStrings__kafka")
        or ""
    )


def _container_name() -> str:
    return os.environ.get("DURABLE_OUTBOX_AZURITE_CONTAINER", "durable-outbox-fastapi")


@pytest.mark.asyncio
async def test_http_publish_persists_to_azurite_and_delivers_to_kafka() -> None:
    blob_connection_string = _blob_connection_string()
    kafka_bootstrap_servers = _kafka_bootstrap_servers()
    if not blob_connection_string or not kafka_bootstrap_servers:
        pytest.skip("run through Aspire or set Blob and Kafka connection env vars")

    topic = f"fastapi-it-{uuid4().hex}"
    payload = {"order_id": "order-1", "amount": 42}
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap_servers,
            "group.id": f"durable-outbox-fastapi-it-{uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])

    async with httpx.AsyncClient(base_url=_base_url(), timeout=30) as client:
        await _wait_for_health(client)
        response = await client.post(
            f"/topics/{topic}/messages",
            json=payload,
            headers={"x-message-key": "order-1"},
        )

    assert response.status_code == 202
    body = response.json()
    event_id = body["event_id"]
    assert body["topic"] == topic
    assert body["dispatch"]["sent"] == 1
    assert body["dispatch"]["failed"] == 0

    try:
        message = _consume_event(consumer, event_id=event_id)
    finally:
        consumer.close()

    assert message.topic() == topic
    assert message.key() == b"order-1"
    assert (
        message.value()
        == json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )
    assert _header_value(message.headers() or [], "event_id") == event_id.encode()

    blob_client = AzureBlobClient.from_connection_string(
        blob_connection_string,
        container_name=_container_name(),
    )
    try:
        blobs = await blob_client.list_blobs(prefix="outbox/v1/events/")
    finally:
        await blob_client.close()
    matching = [blob for blob in blobs if blob.metadata.get("event_id") == event_id]
    assert len(matching) == 1
    assert matching[0].metadata["status"] == "SENT"


async def _wait_for_health(client: httpx.AsyncClient) -> None:
    deadline = time.monotonic() + 30
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = await client.get("/healthz")
            if response.status_code == 200:
                return
        except httpx.HTTPError as exc:
            last_error = exc
        await asyncio.sleep(0.25)
    if last_error is not None:
        raise AssertionError("FastAPI service did not become healthy") from last_error
    raise AssertionError("FastAPI service did not become healthy")


def _consume_event(consumer: Consumer, *, event_id: str) -> KafkaMessage:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is None:
            continue
        error = message.error()
        if error is not None:
            if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                continue
            raise AssertionError(str(error))
        kafka_message = cast(KafkaMessage, message)
        if (
            _header_value(kafka_message.headers() or [], "event_id")
            == event_id.encode()
        ):
            return kafka_message
    raise AssertionError(f"Kafka message for {event_id} was not delivered")


def _header_value(
    headers: Iterable[tuple[str, str | bytes | None]], name: str
) -> bytes | None:
    for header_name, value in headers:
        if header_name == name:
            if isinstance(value, str):
                return value.encode()
            return value
    return None
