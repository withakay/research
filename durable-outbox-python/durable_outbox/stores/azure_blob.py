from collections.abc import Mapping
from importlib import import_module
from typing import Any, cast

from durable_outbox.core.errors import ConfigurationError
from durable_outbox.stores.blob_geo import (
    BlobObject,
    BlobPreconditionFailedError,
)

_AZURE_EXTRA_MESSAGE = (
    "Azure Blob support requires the azure extra: install durable-outbox[azure]"
)


class AzureBlobClient:
    """Blob client adapter backed by Azure Blob Storage or Azurite."""

    def __init__(self, container_client: Any) -> None:
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
            download = await blob_client.download_blob()
            content = await download.readall()
        except Exception as exc:
            if _is_azure_error(exc, {"ResourceNotFoundError"}):
                return None
            raise
        return BlobObject(
            name=name,
            content=bytes(content),
            metadata=dict(cast(Mapping[str, str], properties.metadata or {})),
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
            await blob_client.upload_blob(
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
        blob = await self.get_blob(name)
        if blob is None:
            raise BlobPreconditionFailedError("uploaded blob was not readable")
        return blob

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

    async def list_blobs(self, *, prefix: str) -> list[BlobObject]:
        blobs: list[BlobObject] = []
        async for item in self.container_client.list_blobs(name_starts_with=prefix):
            name = str(item.name)
            blob = await self.get_blob(name)
            if blob is not None:
                blobs.append(blob)
        return blobs


def _if_not_modified() -> Any:
    module = _import_azure_module("azure.core")
    return module.MatchConditions.IfNotModified


def _import_azure_module(name: str) -> Any:
    try:
        module: Any = import_module(name)
    except ModuleNotFoundError as exc:
        raise ConfigurationError(_AZURE_EXTRA_MESSAGE) from exc
    return module


def _is_azure_error(exc: Exception, names: set[str]) -> bool:
    return exc.__class__.__name__ in names
