from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from importlib import import_module
from typing import TYPE_CHECKING, Any

import pytest

from durable_outbox.core import ConfigurationError
from durable_outbox.core.errors import RetryableStoreError
from durable_outbox.sinks.file import FileSink
from durable_outbox.stores.azure_blob import MAX_BLOB_DOWNLOAD_BYTES, AzureBlobClient
from durable_outbox.stores.blob_geo import BlobObject
from durable_outbox.testing.provider_contract import make_event

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping
    from pathlib import Path


@dataclass(slots=True)
class BlobProperties:
    metadata: Mapping[str, str]
    etag: str
    size: int


class Download:
    def __init__(self, content: bytes) -> None:
        self.content = content

    async def readall(self) -> bytes:
        return self.content


class BlobItem:
    def __init__(self, blob: BlobObject) -> None:
        self.name = blob.name
        self.metadata = blob.metadata
        self.etag = blob.etag


class NameOnlyBlobItem:
    def __init__(self, name: str) -> None:
        self.name = name


class Blob:
    def __init__(self, container: Container, name: str) -> None:
        self.container = container
        self.name = name

    async def get_blob_properties(self) -> BlobProperties:
        self.container.property_reads += 1
        blob = self.container.blobs[self.name]
        return BlobProperties(
            metadata=blob.metadata,
            etag=blob.etag,
            size=self.container.reported_sizes.get(self.name, len(blob.content)),
        )

    async def download_blob(self) -> Download:
        self.container.downloads += 1
        return Download(self.container.blobs[self.name].content)

    async def upload_blob(
        self,
        data: bytes,
        *,
        overwrite: bool,
        metadata: Mapping[str, str],
        **kwargs: Any,
    ) -> Mapping[str, str]:
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
        return {"etag": self.container.blobs[self.name].etag}

    async def delete_blob(self, **kwargs: Any) -> None:
        _ = kwargs
        del self.container.blobs[self.name]


class Container:
    def __init__(self) -> None:
        self.blobs: dict[str, BlobObject] = {}
        self.versions: dict[str, int] = {}
        self.created = False
        self.property_reads = 0
        self.downloads = 0
        self.reported_sizes: dict[str, int] = {}

    async def create_container(self) -> None:
        self.created = True

    def get_blob_client(self, name: str) -> Blob:
        return Blob(self, name)

    def list_blobs(
        self,
        *,
        name_starts_with: str,
        include: list[str] | None = None,
    ) -> AsyncIterator[BlobItem | NameOnlyBlobItem]:
        async def items() -> AsyncIterator[BlobItem | NameOnlyBlobItem]:
            for name in sorted(self.blobs):
                if name.startswith(name_starts_with):
                    if include == ["metadata"]:
                        yield BlobItem(self.blobs[name])
                    else:
                        yield NameOnlyBlobItem(name)

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


@pytest.mark.asyncio
async def test_azure_blob_client_can_list_metadata_without_downloading_content() -> (
    None
):
    container = Container()
    client = AzureBlobClient(container)
    written = await client.put_blob(
        "outbox/v1/events/one.json",
        b"payload",
        {"event_id": "one", "status": "PENDING"},
    )

    listed = await client.list_blobs(
        prefix="outbox/v1/events/",
        with_content=False,
    )

    assert listed == [
        BlobObject(
            name=written.name,
            content=b"",
            metadata={"event_id": "one", "status": "PENDING"},
            etag=written.etag,
        )
    ]
    assert container.property_reads == 0
    assert container.downloads == 0


@pytest.mark.asyncio
async def test_azure_blob_client_put_uses_upload_response_without_readback() -> None:
    container = Container()
    client = AzureBlobClient(container)

    written = await client.put_blob(
        "outbox/v1/events/one.json",
        b"payload",
        {"event_id": "one"},
    )

    assert written == BlobObject(
        name="outbox/v1/events/one.json",
        content=b"payload",
        metadata={"event_id": "one"},
        etag='"1"',
    )
    assert container.property_reads == 0
    assert container.downloads == 0


@pytest.mark.asyncio
async def test_azure_blob_client_rejects_oversized_blob_before_download() -> None:
    container = Container()
    client = AzureBlobClient(container)
    written = await client.put_blob(
        "outbox/v1/events/large.json",
        b"payload",
        {"event_id": "large"},
    )
    container.reported_sizes[written.name] = MAX_BLOB_DOWNLOAD_BYTES + 1

    with pytest.raises(RetryableStoreError, match="download"):
        await client.get_blob(written.name)

    assert container.downloads == 0


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
    await sink.aclose()

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


@pytest.mark.asyncio
async def test_file_sink_batches_fsync_until_interval_or_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fsync_calls = 0

    def fsync(fd: int) -> None:
        nonlocal fsync_calls
        _ = fd
        fsync_calls += 1

    monkeypatch.setattr("durable_outbox.sinks.file.os.fsync", fsync)
    sink = FileSink(
        tmp_path / "published" / "events.jsonl",
        fsync=True,
        fsync_interval_events=2,
    )

    await sink.publish(make_event("event-1"))
    assert fsync_calls == 0
    await sink.publish(make_event("event-2"))
    assert fsync_calls == 1
    await sink.publish(make_event("event-3"))
    assert fsync_calls == 1

    await sink.aclose()

    assert fsync_calls == 2
