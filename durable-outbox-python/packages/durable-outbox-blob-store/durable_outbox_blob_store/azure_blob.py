from collections.abc import Mapping
from importlib import import_module
from typing import TYPE_CHECKING, Any, Protocol, cast

from durable_outbox.core.errors import ConfigurationError, RetryableStoreError
from durable_outbox_blob_store.store import (
    BlobObject,
    BlobPreconditionFailedError,
)

_AZURE_EXTRA_MESSAGE = "Azure Blob support requires durable-outbox-blob-store"
MAX_BLOB_DOWNLOAD_BYTES = 16 * 1024 * 1024


class AzureContainerClientLike(Protocol):
    async def create_container(self) -> None: ...

    def get_blob_client(self, name: str) -> Any: ...

    def list_blobs(
        self,
        *,
        name_starts_with: str,
        include: list[str] | None = None,
    ) -> Any: ...


if TYPE_CHECKING:
    from azure.storage.blob.aio import ContainerClient as AzureSdkContainerClient

    type AzureContainerClient = AzureContainerClientLike | AzureSdkContainerClient
else:
    AzureContainerClient = AzureContainerClientLike


class AzureBlobClient:
    """Blob client adapter backed by Azure Blob Storage or Azurite."""

    def __init__(self, container_client: AzureContainerClient) -> None:
        self.container_client = container_client

    @classmethod
    def from_connection_string(
        cls,
        connection_string: str,
        *,
        container_name: str,
    ) -> AzureBlobClient:
        module = _import_azure_module("azure.storage.blob.aio")
        container_client = module.ContainerClient.from_connection_string(
            connection_string,
            container_name=container_name,
        )
        return cls(container_client)

    async def ensure_container(self) -> None:
        try:
            await self.container_client.create_container()
        except Exception as exc:
            if not _is_azure_error(exc, {"ResourceExistsError"}):
                raise

    async def close(self) -> None:
        close = getattr(self.container_client, "close", None)
        if close is not None:
            await close()

    async def __aenter__(self) -> AzureBlobClient:
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await self.close()

    async def get_blob(self, name: str) -> BlobObject | None:
        blob_client = self.container_client.get_blob_client(name)
        try:
            properties = await blob_client.get_blob_properties()
            _enforce_blob_download_size(properties)
            download = await blob_client.download_blob()
            content = _downloaded_blob_content(await download.readall())
            if len(content) > MAX_BLOB_DOWNLOAD_BYTES:
                raise RetryableStoreError(
                    "blob download exceeds max_blob_download_bytes="
                    f"{MAX_BLOB_DOWNLOAD_BYTES}"
                )
        except Exception as exc:
            if _is_azure_error(exc, {"ResourceNotFoundError"}):
                return None
            raise
        return BlobObject(
            name=name,
            content=content,
            metadata=dict(cast("Mapping[str, str]", properties.metadata or {})),
            etag=str(properties.etag),
        )

    async def put_blob(
        self,
        name: str,
        content: bytes,
        metadata: Mapping[str, str],
        *,
        if_none_match: bool = False,
        if_match: str | None = None,
    ) -> BlobObject:
        blob_client = self.container_client.get_blob_client(name)
        kwargs: dict[str, Any] = {}
        overwrite = not if_none_match
        if if_none_match:
            kwargs["if_none_match"] = "*"
        if if_match is not None:
            kwargs["etag"] = if_match
            kwargs["match_condition"] = _if_not_modified()
            overwrite = True
        try:
            response = await blob_client.upload_blob(
                content,
                overwrite=overwrite,
                metadata=dict(metadata),
                **kwargs,
            )
        except Exception as exc:
            if _is_azure_error(
                exc,
                {
                    "ResourceExistsError",
                    "ResourceModifiedError",
                    "ResourceNotModifiedError",
                    "ResourceNotFoundError",
                },
            ):
                raise BlobPreconditionFailedError(str(exc)) from exc
            raise
        return BlobObject(
            name=name,
            content=bytes(content),
            metadata=dict(metadata),
            etag=_upload_response_etag(response),
        )

    async def delete_blob(self, name: str, *, if_match: str | None = None) -> bool:
        blob_client = self.container_client.get_blob_client(name)
        kwargs: dict[str, Any] = {}
        if if_match is not None:
            kwargs["etag"] = if_match
            kwargs["match_condition"] = _if_not_modified()
        try:
            await blob_client.delete_blob(**kwargs)
        except Exception as exc:
            if _is_azure_error(exc, {"ResourceNotFoundError"}):
                return False
            if _is_azure_error(
                exc,
                {"ResourceModifiedError", "ResourceNotModifiedError"},
            ):
                raise BlobPreconditionFailedError(str(exc)) from exc
            raise
        return True

    async def list_blobs(
        self,
        *,
        prefix: str,
        with_content: bool = True,
    ) -> list[BlobObject]:
        blobs: list[BlobObject] = []
        async for item in self._list_blob_items(
            prefix=prefix, with_content=with_content
        ):
            name = str(item.name)
            if not with_content:
                blobs.append(
                    BlobObject(
                        name=name,
                        content=b"",
                        metadata=_blob_item_metadata(item),
                        etag=_blob_item_etag(item),
                    )
                )
                continue
            blob = await self.get_blob(name)
            if blob is not None:
                blobs.append(blob)
        return blobs

    def _list_blob_items(self, *, prefix: str, with_content: bool) -> Any:
        if with_content:
            return self.container_client.list_blobs(name_starts_with=prefix)
        try:
            return self.container_client.list_blobs(
                name_starts_with=prefix,
                include=["metadata"],
            )
        except TypeError:
            return self.container_client.list_blobs(name_starts_with=prefix)


def _if_not_modified() -> Any:
    module = _import_azure_module("azure.core")
    return module.MatchConditions.IfNotModified


def _import_azure_module(name: str) -> Any:
    try:
        module: Any = import_module(name)
    except ModuleNotFoundError as exc:
        raise ConfigurationError(_AZURE_EXTRA_MESSAGE) from exc
    return module


def _upload_response_etag(response: object) -> str:
    if isinstance(response, Mapping):
        response_mapping = cast("Mapping[str, object]", response)
        etag = response_mapping.get("etag")
        if isinstance(etag, str):
            return etag
    etag = getattr(response, "etag", None)
    if isinstance(etag, str):
        return etag
    raise BlobPreconditionFailedError("uploaded blob response missing etag")


def _blob_item_metadata(item: object) -> dict[str, str]:
    metadata = getattr(item, "metadata", None)
    if isinstance(metadata, Mapping):
        return dict(cast("Mapping[str, str]", metadata))
    return {}


def _blob_item_etag(item: object) -> str:
    etag = getattr(item, "etag", None)
    return str(etag) if etag is not None else ""


def _enforce_blob_download_size(properties: object) -> None:
    size = _blob_content_size(properties)
    if size is not None and size > MAX_BLOB_DOWNLOAD_BYTES:
        raise RetryableStoreError(
            f"blob download exceeds max_blob_download_bytes={MAX_BLOB_DOWNLOAD_BYTES}"
        )


def _blob_content_size(properties: object) -> int | None:
    for attribute in ("size", "content_length"):
        size = getattr(properties, attribute, None)
        if isinstance(size, int):
            return size
    return None


def _downloaded_blob_content(content: object) -> bytes:
    if isinstance(content, bytes):
        return content
    if isinstance(content, bytearray | memoryview):
        return bytes(content)
    raise RetryableStoreError("blob download content must be bytes")


def _is_azure_error(exc: Exception, names: set[str]) -> bool:
    return exc.__class__.__name__ in names
