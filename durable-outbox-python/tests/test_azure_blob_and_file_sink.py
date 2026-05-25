import base64
import json
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest

from durable_outbox.core import ConfigurationError
from durable_outbox.sinks.file import FileSink
from durable_outbox.stores.azure_blob import AzureBlobClient
from durable_outbox.stores.blob_geo import BlobObject
from durable_outbox.testing.provider_contract import make_event


@dataclass(slots=True)
class BlobProperties:
    metadata: Mapping[str, str]
    etag: str


class Download:
    def __init__(self, content: bytes) -> None:
        self.content = content

    async def readall(self) -> bytes:
        return self.content


class BlobItem:
    def __init__(self, name: str) -> None:
        self.name = name


class Blob:
    def __init__(self, container: Container, name: str) -> None:
        self.container = container
        self.name = name

    async def get_blob_properties(self) -> BlobProperties:
        blob = self.container.blobs[self.name]
        return BlobProperties(metadata=blob.metadata, etag=blob.etag)

    async def download_blob(self) -> Download:
        return Download(self.container.blobs[self.name].content)

    async def upload_blob(
        self,
        data: bytes,
        *,
        overwrite: bool,
        metadata: Mapping[str, str],
        **kwargs: Any,
    ) -> None:
        _ = kwargs
        version = self.container.versions.get(self.name, 0) + 1
        self.container.versions[self.name] = version
        self.container.blobs[self.name] = BlobObject(
            name=self.name,
            content=data,
            metadata=dict(metadata),
            etag=f'"{version}"',
        )
        assert overwrite in {False, True}

    async def delete_blob(self, **kwargs: Any) -> None:
        _ = kwargs
        del self.container.blobs[self.name]


class Container:
    def __init__(self) -> None:
        self.blobs: dict[str, BlobObject] = {}
        self.versions: dict[str, int] = {}
        self.created = False

    async def create_container(self) -> None:
        self.created = True

    def get_blob_client(self, name: str) -> Blob:
        return Blob(self, name)

    def list_blobs(self, *, name_starts_with: str) -> AsyncIterator[BlobItem]:
        async def items() -> AsyncIterator[BlobItem]:
            for name in sorted(self.blobs):
                if name.startswith(name_starts_with):
                    yield BlobItem(name)

        return items()


@pytest.mark.asyncio
async def test_azure_blob_client_adapts_container_protocol() -> None:
    container = Container()
    client = AzureBlobClient(container)

    await client.ensure_container()
    written = await client.put_blob(
        "outbox/v1/events/one.json",
        b"payload",
        {"event_id": "one"},
    )
    read = await client.get_blob("outbox/v1/events/one.json")
    listed = await client.list_blobs(prefix="outbox/v1/events/")

    assert container.created is True
    assert written.etag == '"1"'
    assert read == written
    assert listed == [written]


def test_azure_blob_client_reports_missing_optional_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import_module = import_module

    def fail_azure_import(name: str) -> Any:
        if name == "azure.storage.blob.aio":
            raise ModuleNotFoundError("No module named 'azure'")
        return real_import_module(name)

    monkeypatch.setattr(
        "durable_outbox.stores.azure_blob.import_module", fail_azure_import
    )

    with pytest.raises(ConfigurationError, match="durable-outbox\\[azure\\]"):
        AzureBlobClient.from_connection_string(
            "UseDevelopmentStorage=true",
            container_name="outbox",
        )


@pytest.mark.asyncio
async def test_azure_blob_client_reports_missing_core_optional_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import_module = import_module

    def fail_azure_core_import(name: str) -> Any:
        if name == "azure.core":
            raise ModuleNotFoundError("No module named 'azure'")
        return real_import_module(name)

    monkeypatch.setattr(
        "durable_outbox.stores.azure_blob.import_module", fail_azure_core_import
    )

    client = AzureBlobClient(Container())
    with pytest.raises(ConfigurationError, match="durable-outbox\\[azure\\]"):
        await client.put_blob(
            "outbox/v1/events/one.json",
            b"payload",
            {"event_id": "one"},
            if_match='"1"',
        )


@pytest.mark.asyncio
async def test_file_sink_appends_kafka_like_jsonl_records(tmp_path: Path) -> None:
    path = tmp_path / "published" / "events.jsonl"
    event = make_event("event-1", ordering_key="customer-1")
    sink = FileSink(path)

    first = await sink.publish(event)
    second = await sink.publish(make_event("event-2"))

    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert first.partition == 0
    assert first.offset == 0
    assert second.offset == 1
    assert rows[0]["event_id"] == "event-1"
    assert rows[0]["topic"] == event.topic
    assert rows[0]["payload_base64"] == base64.b64encode(event.payload).decode("ascii")
    assert rows[0]["headers"]["content-type"] == base64.b64encode(
        b"application/json"
    ).decode("ascii")
