from typing import Any

__all__ = [
    "AzureBlobClient",
    "AzureSqlSyncOutboxStore",
    "BlobOutboxStore",
    "CosmosStrongOutboxStore",
    "DualRegionBlobOutboxStore",
    "MemoryOutboxStore",
    "SqlAlwaysOnOutboxStore",
]


def __getattr__(name: str) -> Any:
    if name == "AzureBlobClient":
        from durable_outbox.stores.azure_blob import AzureBlobClient

        return AzureBlobClient
    if name in {"BlobOutboxStore", "DualRegionBlobOutboxStore"}:
        from durable_outbox.stores.blob_geo import (
            BlobOutboxStore,
            DualRegionBlobOutboxStore,
        )

        return {
            "BlobOutboxStore": BlobOutboxStore,
            "DualRegionBlobOutboxStore": DualRegionBlobOutboxStore,
        }[name]
    if name == "CosmosStrongOutboxStore":
        from durable_outbox.stores.cosmos import CosmosStrongOutboxStore

        return CosmosStrongOutboxStore
    if name == "MemoryOutboxStore":
        from durable_outbox.stores.memory import MemoryOutboxStore

        return MemoryOutboxStore
    if name in {"AzureSqlSyncOutboxStore", "SqlAlwaysOnOutboxStore"}:
        from durable_outbox.stores.sql import (
            AzureSqlSyncOutboxStore,
            SqlAlwaysOnOutboxStore,
        )

        return {
            "AzureSqlSyncOutboxStore": AzureSqlSyncOutboxStore,
            "SqlAlwaysOnOutboxStore": SqlAlwaysOnOutboxStore,
        }[name]
    raise AttributeError(name)
