from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, cast

from durable_outbox.core.errors import ConfigurationError
from durable_outbox_blob_store.azure_blob import AzureBlobClient
from durable_outbox_blob_store.store import (
    MAX_BLOB_PAYLOAD_BYTES,
    BlobClientProtocol,
    BlobObject,
    BlobOrderingLockBackend,
    BlobOutboxStore,
    BlobPreconditionFailedError,
    DualRegionBlobOutboxStore,
    InMemoryBlobClient,
    RegionWrite,
    blob_metadata,
    cleanup_freeze_blob_name,
    event_blob_name,
    ordering_lock_blob_name,
    payload_blob_name,
    state_blob_name,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from durable_outbox.core.time import Clock
    from durable_outbox.telemetry.metrics import MetricsAdapter

__all__ = [
    "MAX_BLOB_PAYLOAD_BYTES",
    "AzureBlobClient",
    "BlobClientProtocol",
    "BlobObject",
    "BlobOrderingLockBackend",
    "BlobOutboxStore",
    "BlobPreconditionFailedError",
    "DualRegionBlobOutboxStore",
    "InMemoryBlobClient",
    "RegionWrite",
    "blob_metadata",
    "build_blob_store",
    "build_dual_region_blob_store",
    "cleanup_freeze_blob_name",
    "event_blob_name",
    "ordering_lock_blob_name",
    "payload_blob_name",
    "state_blob_name",
]


def build_blob_store(config: Mapping[str, object]) -> BlobOutboxStore:
    """Build a Blob store from durable outbox plugin configuration."""

    client = _client(config, "client")
    return BlobOutboxStore(
        client=client,
        claim_timeout=_optional_timedelta(config, "claim_timeout"),
        clock=cast("Clock | None", config.get("clock")),
        environment=_optional_str(config, "environment", default="default"),
        fingerprint_key=cast("bytes | None", config.get("fingerprint_key")),
    )


def build_dual_region_blob_store(
    config: Mapping[str, object],
) -> DualRegionBlobOutboxStore:
    """Build a dual-region Blob store from durable outbox plugin configuration."""

    return DualRegionBlobOutboxStore(
        primary_client=_client(config, "primary"),
        secondary_client=_client(config, "secondary"),
        claim_timeout=_optional_timedelta(config, "claim_timeout"),
        clock=cast("Clock | None", config.get("clock")),
        metrics=cast("MetricsAdapter | None", config.get("metrics")),
        environment=_optional_str(config, "environment", default="default"),
        fingerprint_key=cast("bytes | None", config.get("fingerprint_key")),
    )


def _client(config: Mapping[str, object], name: str) -> BlobClientProtocol:
    value = config.get(name)
    if (value is None or isinstance(value, dict)) and name == "client":
        return InMemoryBlobClient()
    if value is None:
        raise ConfigurationError(f"Blob store plugin requires {name!r} client config")
    return cast("BlobClientProtocol", value)


def _optional_timedelta(
    config: Mapping[str, object],
    name: str,
) -> timedelta:
    value = config.get(name)
    if value is None:
        return timedelta(minutes=5)
    if isinstance(value, timedelta):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return timedelta(seconds=value)
    raise ConfigurationError(
        f"Blob store plugin config {name!r} must be a timedelta or seconds int"
    )


def _optional_str(config: Mapping[str, object], name: str, *, default: str) -> str:
    value = config.get(name, default)
    if isinstance(value, str):
        return value
    raise ConfigurationError(f"Blob store plugin config {name!r} must be a string")
