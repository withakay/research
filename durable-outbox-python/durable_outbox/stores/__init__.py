from typing import Any

__all__ = [
    "AzureBlobClient",
    "AzureCosmosOutboxClient",
    "AzureSqlSyncOutboxStore",
    "BlobOrderingLockBackend",
    "BlobOutboxStore",
    "CosmosStrongOutboxStore",
    "DualRegionBlobOutboxStore",
    "MemoryOutboxStore",
    "PyodbcSqlConnectionSettings",
    "PyodbcSqlOutboxClient",
    "SqlAlwaysOnOutboxStore",
]


def __getattr__(name: str) -> Any:
    if name == "AzureBlobClient":
        from durable_outbox.stores.azure_blob import AzureBlobClient

        return AzureBlobClient
    if name == "AzureCosmosOutboxClient":
        from durable_outbox.stores.cosmos_azure import AzureCosmosOutboxClient

        return AzureCosmosOutboxClient
    if name in {
        "BlobOrderingLockBackend",
        "BlobOutboxStore",
        "DualRegionBlobOutboxStore",
    }:
        from durable_outbox.stores.blob_geo import (
            BlobOrderingLockBackend,
            BlobOutboxStore,
            DualRegionBlobOutboxStore,
        )

        return {
            "BlobOrderingLockBackend": BlobOrderingLockBackend,
            "BlobOutboxStore": BlobOutboxStore,
            "DualRegionBlobOutboxStore": DualRegionBlobOutboxStore,
        }[name]
    if name == "CosmosStrongOutboxStore":
        from durable_outbox.stores.cosmos import CosmosStrongOutboxStore

        return CosmosStrongOutboxStore
    if name == "MemoryOutboxStore":
        from durable_outbox.stores.memory import MemoryOutboxStore

        return MemoryOutboxStore
    if name in {"PyodbcSqlConnectionSettings", "PyodbcSqlOutboxClient"}:
        from durable_outbox.stores.sql_pyodbc import (
            PyodbcSqlConnectionSettings,
            PyodbcSqlOutboxClient,
        )

        return {
            "PyodbcSqlConnectionSettings": PyodbcSqlConnectionSettings,
            "PyodbcSqlOutboxClient": PyodbcSqlOutboxClient,
        }[name]
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
