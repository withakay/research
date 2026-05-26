from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, cast

from durable_outbox.core.errors import ConfigurationError
from durable_outbox_cosmos_store.azure import (
    AzureCosmosOutboxClient,
    decode_cosmos_item,
    encode_cosmos_item,
)
from durable_outbox_cosmos_store.store import (
    CosmosConfiguration,
    CosmosOutboxClient,
    CosmosReplayStreamClient,
    CosmosStoredEvent,
    CosmosStrongOutboxStore,
    InMemoryCosmosOutboxClient,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from durable_outbox.core.time import Clock

__all__ = [
    "AzureCosmosOutboxClient",
    "CosmosConfiguration",
    "CosmosOutboxClient",
    "CosmosReplayStreamClient",
    "CosmosStoredEvent",
    "CosmosStrongOutboxStore",
    "InMemoryCosmosOutboxClient",
    "build_cosmos_store",
    "decode_cosmos_item",
    "encode_cosmos_item",
]


def build_cosmos_store(config: Mapping[str, object]) -> CosmosStrongOutboxStore:
    """Build a Cosmos store from durable outbox plugin configuration."""

    client = config.get("client")
    if client is None or isinstance(client, dict):
        client = InMemoryCosmosOutboxClient()
    return CosmosStrongOutboxStore(
        _cosmos_configuration(config),
        client=cast("CosmosOutboxClient", client),
        claim_timeout=_optional_timedelta(config, "claim_timeout"),
        clock=cast("Clock | None", config.get("clock")),
    )


def _cosmos_configuration(config: Mapping[str, object]) -> CosmosConfiguration:
    value = config.get("configuration")
    if isinstance(value, CosmosConfiguration):
        return value
    return CosmosConfiguration(
        consistency=_optional_str(config, "consistency", default="Strong"),
        regions=_optional_str_tuple(config, "regions", default=("local", "standby")),
        multi_write=_optional_bool(config, "multi_write", default=False),
        certified_mode=_optional_bool(config, "certified_mode", default=True),
        unordered_buckets=_optional_int(config, "unordered_buckets", default=16),
    )


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
        f"Cosmos store plugin config {name!r} must be a timedelta or seconds int"
    )


def _optional_str(config: Mapping[str, object], name: str, *, default: str) -> str:
    value = config.get(name, default)
    if isinstance(value, str):
        return value
    raise ConfigurationError(f"Cosmos store plugin config {name!r} must be a string")


def _optional_str_tuple(
    config: Mapping[str, object],
    name: str,
    *,
    default: tuple[str, ...],
) -> tuple[str, ...]:
    value = config.get(name, default)
    if isinstance(value, tuple | list) and all(isinstance(item, str) for item in value):
        return tuple(cast("tuple[str, ...] | list[str]", value))
    raise ConfigurationError(
        f"Cosmos store plugin config {name!r} must be a sequence of strings"
    )


def _optional_bool(config: Mapping[str, object], name: str, *, default: bool) -> bool:
    value = config.get(name, default)
    if isinstance(value, bool):
        return value
    raise ConfigurationError(f"Cosmos store plugin config {name!r} must be a bool")


def _optional_int(config: Mapping[str, object], name: str, *, default: int) -> int:
    value = config.get(name, default)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise ConfigurationError(f"Cosmos store plugin config {name!r} must be an int")
