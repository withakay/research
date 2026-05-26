from __future__ import annotations

import base64
from collections.abc import Collection, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any, Protocol, cast

from durable_outbox.core.errors import (
    ClaimConflictError,
    ConfigurationError,
    RetryableStoreError,
)
from durable_outbox.core.model import OutboxEvent, OutboxStatus, PublishingMode
from durable_outbox.core.validation import require_positive_limit
from durable_outbox.stores.cosmos import (
    CosmosConfiguration,
    CosmosStoredEvent,
)

_AZURE_EXTRA_MESSAGE = (
    "Azure Cosmos support requires the azure extra: install durable-outbox[azure]"
)
_CONTROL_PARTITION_KEY = "__control__"
_CLEANUP_FREEZE_ID = "cleanup-freeze"


class CosmosContainerLike(Protocol):
    async def read_item(
        self, *, item: str, partition_key: str
    ) -> Mapping[str, object]: ...

    async def create_item(self, body: dict[str, object]) -> Mapping[str, object]: ...

    async def replace_item(
        self,
        *,
        item: str,
        body: dict[str, object],
        etag: str | None,
        match_condition: object | None,
    ) -> Mapping[str, object]: ...

    async def delete_item(self, *, item: str, partition_key: str) -> None: ...


class CosmosAccountClientLike(Protocol):
    async def read_account(self) -> Mapping[str, object]: ...

    async def close(self) -> None: ...


class AzureCosmosOutboxClient:
    """Azure Cosmos point-operation client.

    This slice deliberately supports point insert/read/replace/delete and
    account validation only. Partition-scoped claim, replay, and cleanup
    candidate queries remain explicit future work.
    """

    def __init__(
        self,
        container: CosmosContainerLike,
        *,
        account_client: CosmosAccountClientLike | None = None,
    ) -> None:
        self.container = container
        self.account_client = account_client
        self.partition_keys_by_event_id: dict[str, str] = {}

    @classmethod
    def from_connection_string(
        cls,
        connection_string: str,
        *,
        database_name: str,
        container_name: str,
    ) -> AzureCosmosOutboxClient:
        module = _import_azure_module("azure.cosmos.aio")
        account_client = module.CosmosClient.from_connection_string(connection_string)
        database = account_client.get_database_client(database_name)
        container = database.get_container_client(container_name)
        return cls(
            cast("CosmosContainerLike", container),
            account_client=cast("CosmosAccountClientLike", account_client),
        )

    async def validate_account(self, config: CosmosConfiguration) -> None:
        if self.account_client is None:
            raise ConfigurationError(
                "Cosmos account validation requires account client"
            )
        try:
            account = await self.account_client.read_account()
        except Exception as exc:
            raise RetryableStoreError("Cosmos account validation failed") from exc
        _validate_account_shape(account, config)

    async def close(self) -> None:
        if self.account_client is not None:
            await self.account_client.close()

    async def get(self, event_id: str) -> CosmosStoredEvent | None:
        partition_key = self.partition_keys_by_event_id.get(event_id)
        if partition_key is None:
            return None
        try:
            item = await self.container.read_item(
                item=event_id,
                partition_key=partition_key,
            )
        except Exception as exc:
            if _is_azure_error(
                exc, {"CosmosResourceNotFoundError", "ResourceNotFoundError"}
            ):
                return None
            raise
        record = decode_cosmos_item(item)
        self.partition_keys_by_event_id[record.event.event_id] = record.partition_key
        return record

    async def insert(self, record: CosmosStoredEvent) -> CosmosStoredEvent:
        item = await self.container.create_item(encode_cosmos_item(record))
        stored = decode_cosmos_item(item)
        self.partition_keys_by_event_id[stored.event.event_id] = stored.partition_key
        return stored

    async def replace(
        self,
        record: CosmosStoredEvent,
        *,
        expected_version: int,
    ) -> CosmosStoredEvent:
        _ = expected_version
        try:
            item = await self.container.replace_item(
                item=record.event.event_id,
                body=encode_cosmos_item(record),
                etag=record.etag,
                match_condition=_if_not_modified(),
            )
        except Exception as exc:
            if _is_azure_error(
                exc,
                {"CosmosAccessConditionFailedError", "ResourceModifiedError"},
            ):
                raise ClaimConflictError("record etag precondition failed") from exc
            raise
        stored = decode_cosmos_item(item)
        self.partition_keys_by_event_id[stored.event.event_id] = stored.partition_key
        return stored

    async def list_records(self) -> Sequence[CosmosStoredEvent]:
        raise ConfigurationError(
            "AzureCosmosOutboxClient does not support cross-partition list_records(); "
            "partition-scoped Cosmos queries are not implemented"
        )

    async def claim_batch_pending(
        self,
        *,
        limit: int,
        now: datetime,
        claim_timeout: timedelta,
    ) -> Sequence[CosmosStoredEvent]:
        _ = now, claim_timeout
        require_positive_limit(limit)
        raise ConfigurationError(
            "AzureCosmosOutboxClient requires partition-scoped Cosmos claim queries"
        )

    async def list_failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> Sequence[CosmosStoredEvent]:
        _ = failover_started_at, exclude_event_ids
        require_positive_limit(limit)
        raise ConfigurationError(
            "AzureCosmosOutboxClient requires partition-scoped Cosmos replay queries"
        )

    async def list_cleanup_candidates(
        self,
        *,
        now: datetime,
        safety_margin: timedelta,
        limit: int | None = None,
    ) -> Sequence[CosmosStoredEvent]:
        _ = now, safety_margin, limit
        raise ConfigurationError(
            "AzureCosmosOutboxClient requires partition-scoped Cosmos cleanup queries"
        )

    async def delete(self, event_id: str) -> None:
        partition_key = self.partition_keys_by_event_id.pop(event_id, None)
        if partition_key is None:
            return
        try:
            await self.container.delete_item(item=event_id, partition_key=partition_key)
        except Exception as exc:
            if _is_azure_error(
                exc, {"CosmosResourceNotFoundError", "ResourceNotFoundError"}
            ):
                return
            raise

    async def get_cleanup_freeze_reason(self) -> str | None:
        try:
            item = await self.container.read_item(
                item=_CLEANUP_FREEZE_ID,
                partition_key=_CONTROL_PARTITION_KEY,
            )
        except Exception as exc:
            if _is_azure_error(
                exc, {"CosmosResourceNotFoundError", "ResourceNotFoundError"}
            ):
                return None
            raise
        reason = item.get("reason")
        if reason is None or isinstance(reason, str):
            return reason
        raise RetryableStoreError("Cosmos cleanup freeze reason must be a string")

    async def set_cleanup_freeze(self, reason: str) -> None:
        await self.container.create_item(
            {
                "id": _CLEANUP_FREEZE_ID,
                "pk": _CONTROL_PARTITION_KEY,
                "kind": "cleanup_freeze",
                "reason": reason,
            }
        )

    async def clear_cleanup_freeze(self) -> None:
        try:
            await self.container.delete_item(
                item=_CLEANUP_FREEZE_ID,
                partition_key=_CONTROL_PARTITION_KEY,
            )
        except Exception as exc:
            if _is_azure_error(
                exc, {"CosmosResourceNotFoundError", "ResourceNotFoundError"}
            ):
                return
            raise


def encode_cosmos_item(record: CosmosStoredEvent) -> dict[str, object]:
    event = record.event
    publish_result = record.publish_result
    return {
        "id": event.event_id,
        "pk": record.partition_key,
        "event_id": event.event_id,
        "topic": event.topic,
        "key": _encode_optional_bytes(event.key),
        "headers": _encode_headers(event.headers),
        "payload": _encode_bytes(event.payload),
        "schema_id": event.schema_id,
        "schema_version": event.schema_version,
        "ordering_key": event.ordering_key,
        "ordering_key_hash": _hash_optional(event.ordering_key),
        "ordering_sequence": event.ordering_sequence,
        "publishing_mode": event.publishing_mode.value,
        "created_at_epoch_ms": _epoch_ms(event.created_at),
        "expires_at_epoch_ms": _epoch_ms(event.expires_at),
        "accepted_at_epoch_ms": _optional_epoch_ms(record.accepted_at),
        "status": record.status.value,
        "attempt_count": record.attempt_count,
        "claim_id": record.claim_token,
        "claimed_at_epoch_ms": _optional_epoch_ms(record.claimed_at),
        "next_attempt_at_epoch_ms": _optional_epoch_ms(record.next_attempt_at),
        "sent_at_epoch_ms": _optional_epoch_ms(record.sent_at),
        "kafka_partition": publish_result.partition if publish_result else None,
        "kafka_offset": publish_result.offset if publish_result else None,
        "published_at_epoch_ms": (
            _epoch_ms(publish_result.published_at) if publish_result else None
        ),
        "publish_metadata": dict(publish_result.metadata) if publish_result else None,
        "failed_at_epoch_ms": _optional_epoch_ms(record.failed_at),
        "last_error_type": record.last_error_type,
        "last_error": record.last_error,
        "version": record.version,
    }


def decode_cosmos_item(item: Mapping[str, object]) -> CosmosStoredEvent:
    from durable_outbox.core.model import PublishResult

    published_at = _optional_datetime(item, "published_at_epoch_ms")
    publish_result = None
    if published_at is not None:
        publish_result = PublishResult(
            partition=_optional_int(item, "kafka_partition"),
            offset=_optional_int(item, "kafka_offset"),
            published_at=published_at,
            metadata=_str_mapping(item.get("publish_metadata")),
        )
    event = OutboxEvent(
        event_id=_required_str(item, "event_id"),
        topic=_required_str(item, "topic"),
        payload=_decode_bytes(_required_str(item, "payload")),
        key=_decode_optional_bytes(_optional_str(item, "key")),
        headers=_decode_headers(_mapping(item, "headers")),
        created_at=_required_datetime(item, "created_at_epoch_ms"),
        expires_at=_required_datetime(item, "expires_at_epoch_ms"),
        ordering_key=_optional_str(item, "ordering_key"),
        ordering_sequence=_optional_int(item, "ordering_sequence"),
        publishing_mode=PublishingMode(
            _optional_str(item, "publishing_mode") or "UNORDERED"
        ),
        schema_id=_optional_str(item, "schema_id"),
        schema_version=_optional_str(item, "schema_version"),
    )
    return CosmosStoredEvent(
        event=event,
        partition_key=_required_str(item, "pk"),
        version=_required_int(item, "version"),
        etag=_optional_str(item, "_etag"),
        status=OutboxStatus(_required_str(item, "status")),
        accepted_at=_optional_datetime(item, "accepted_at_epoch_ms"),
        attempt_count=_required_int(item, "attempt_count"),
        claim_token=_optional_str(item, "claim_id"),
        claimed_at=_optional_datetime(item, "claimed_at_epoch_ms"),
        next_attempt_at=_optional_datetime(item, "next_attempt_at_epoch_ms"),
        sent_at=_optional_datetime(item, "sent_at_epoch_ms"),
        publish_result=publish_result,
        failed_at=_optional_datetime(item, "failed_at_epoch_ms"),
        last_error_type=_optional_str(item, "last_error_type"),
        last_error=_optional_str(item, "last_error"),
    )


def _import_azure_module(name: str) -> Any:
    try:
        module: Any = import_module(name)
    except ModuleNotFoundError as exc:
        raise ConfigurationError(_AZURE_EXTRA_MESSAGE) from exc
    return module


def _if_not_modified() -> object:
    module = _import_azure_module("azure.core")
    return module.MatchConditions.IfNotModified


def _validate_account_shape(
    account: Mapping[str, object],
    config: CosmosConfiguration,
) -> None:
    if not config.certified_mode:
        return
    consistency = _account_consistency(account)
    if consistency.lower() != "strong":
        raise ConfigurationError(
            "Cosmos certified RPO=0 mode requires strong consistency"
        )
    read_regions = _region_names(account.get("readLocations"))
    write_regions = _region_names(account.get("writeLocations"))
    required_regions = {region.lower() for region in config.regions}
    if not required_regions.issubset(read_regions | write_regions):
        raise ConfigurationError("Cosmos account regions do not match configuration")
    if len(read_regions | write_regions) < 2:
        raise ConfigurationError(
            "Cosmos certified RPO=0 mode requires multiple regions"
        )
    if config.multi_write or len(write_regions) != 1:
        raise ConfigurationError(
            "Cosmos certified RPO=0 mode requires single-write configuration"
        )


def _account_consistency(account: Mapping[str, object]) -> str:
    policy = account.get("consistencyPolicy")
    if isinstance(policy, Mapping):
        policy_mapping = cast("Mapping[str, object]", policy)
        value = policy_mapping.get("defaultConsistencyLevel")
        if isinstance(value, str):
            return value
    raise ConfigurationError("Cosmos account consistency policy is unavailable")


def _region_names(value: object) -> set[str]:
    if not isinstance(value, list):
        return set()
    regions: set[str] = set()
    for item in value:
        if isinstance(item, Mapping):
            item_mapping = cast("Mapping[str, object]", item)
            name = item_mapping.get("name")
            if isinstance(name, str):
                regions.add(name.lower())
    return regions


def _is_azure_error(exc: Exception, names: set[str]) -> bool:
    return type(exc).__name__ in names


def _encode_bytes(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _encode_optional_bytes(value: bytes | None) -> str | None:
    if value is None:
        return None
    return _encode_bytes(value)


def _decode_bytes(value: str) -> bytes:
    return base64.b64decode(value.encode("ascii"))


def _decode_optional_bytes(value: str | None) -> bytes | None:
    if value is None:
        return None
    return _decode_bytes(value)


def _encode_headers(headers: Mapping[str, bytes]) -> dict[str, str]:
    return {name: _encode_bytes(value) for name, value in sorted(headers.items())}


def _decode_headers(headers: Mapping[str, object]) -> dict[str, bytes]:
    decoded: dict[str, bytes] = {}
    for name, value in headers.items():
        if not isinstance(value, str):
            raise RetryableStoreError("Cosmos header values must be strings")
        decoded[name] = _decode_bytes(value)
    return decoded


def _epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _optional_epoch_ms(value: datetime | None) -> int | None:
    if value is None:
        return None
    return _epoch_ms(value)


def _required_datetime(item: Mapping[str, object], field_name: str) -> datetime:
    value = _required_int(item, field_name)
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def _optional_datetime(item: Mapping[str, object], field_name: str) -> datetime | None:
    value = item.get(field_name)
    if value is None:
        return None
    if not isinstance(value, int):
        raise RetryableStoreError(f"{field_name} must be an int epoch millisecond")
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def _required_str(item: Mapping[str, object], field_name: str) -> str:
    value = item.get(field_name)
    if not isinstance(value, str):
        raise RetryableStoreError(f"{field_name} must be a string")
    return value


def _optional_str(item: Mapping[str, object], field_name: str) -> str | None:
    value = item.get(field_name)
    if value is None or isinstance(value, str):
        return value
    raise RetryableStoreError(f"{field_name} must be a string")


def _required_int(item: Mapping[str, object], field_name: str) -> int:
    value = item.get(field_name)
    if isinstance(value, int):
        return value
    raise RetryableStoreError(f"{field_name} must be an int")


def _optional_int(item: Mapping[str, object], field_name: str) -> int | None:
    value = item.get(field_name)
    if value is None or isinstance(value, int):
        return value
    raise RetryableStoreError(f"{field_name} must be an int")


def _mapping(item: Mapping[str, object], field_name: str) -> Mapping[str, object]:
    value = item.get(field_name)
    if isinstance(value, Mapping):
        return cast("Mapping[str, object]", value)
    raise RetryableStoreError(f"{field_name} must be an object")


def _str_mapping(value: object) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise RetryableStoreError("Cosmos publish metadata must be an object")
    result: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str):
            raise RetryableStoreError("Cosmos publish metadata entries must be strings")
        result[key] = item
    return result


def _hash_optional(value: str | None) -> str | None:
    if value is None:
        return None
    from hashlib import sha256

    return sha256(value.encode("utf-8")).hexdigest()
