from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import AsyncIterable, AsyncIterator, Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any, Protocol, cast, runtime_checkable

from durable_outbox.core.claim import claim_order_key, is_claimable_record
from durable_outbox.core.duplicates import raise_if_incompatible_duplicate
from durable_outbox.core.errors import (
    ClaimConflictError,
    ConfigurationError,
    DuplicateEventConflictError,
    RetryableStoreError,
)
from durable_outbox.core.model import OutboxEvent, OutboxStatus, PublishingMode
from durable_outbox.core.ordering import ordering_scope
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
_EVENT_INDEX_KIND = "event_index"
_EVENT_INDEX_ID_PREFIX = "event#"
_EVENT_INDEX_RESERVED = "reserved"
_EVENT_INDEX_COMMITTED = "committed"
_PARTITION_REGISTRY_KIND = "partition_registry"
_PARTITION_REGISTRY_ID_PREFIX = "partition#"
_CLAIM_CANDIDATES_QUERY = """
SELECT * FROM c
WHERE c.pk = @partition_key
AND (
    (c.status = @pending_status AND (
        NOT IS_DEFINED(c.next_attempt_at_epoch_ms)
        OR IS_NULL(c.next_attempt_at_epoch_ms)
        OR c.next_attempt_at_epoch_ms <= @now_epoch_ms
    ))
    OR (c.status = @in_flight_status
        AND IS_DEFINED(c.claimed_at_epoch_ms)
        AND c.claimed_at_epoch_ms <= @stale_claimed_before_epoch_ms)
    OR (c.status = @in_flight_status
        AND IS_DEFINED(c.claimed_at_epoch_ms)
        AND c.claimed_at_epoch_ms > @stale_claimed_before_epoch_ms
        AND IS_DEFINED(c.ordering_key)
        AND NOT IS_NULL(c.ordering_key))
)
ORDER BY c.topic, c.ordering_key, c.ordering_sequence, c.created_at_epoch_ms
"""
_CLEANUP_CANDIDATES_QUERY = """
SELECT * FROM c
WHERE c.pk = @partition_key
AND c.status = @sent_status
AND c.expires_at_epoch_ms < @expires_before_epoch_ms
ORDER BY c.expires_at_epoch_ms
"""
_REPLAY_CANDIDATES_QUERY = """
SELECT * FROM c
WHERE c.pk = @partition_key
AND c.status IN (@pending_status, @in_flight_status, @sent_status)
AND c.expires_at_epoch_ms >= @failover_started_at_epoch_ms
AND NOT ARRAY_CONTAINS(@exclude_event_ids, c.event_id)
ORDER BY c.created_at_epoch_ms
"""
_PARTITION_REGISTRY_QUERY = """
SELECT * FROM c
WHERE c.pk = @partition_key
AND c.kind = @kind
ORDER BY c.partition_key
"""


def _query_parameter(name: str, value: object) -> dict[str, object]:
    return {"name": name, "value": value}


class CosmosContainerLike(Protocol):
    async def read_item(
        self, *, item: str, partition_key: str
    ) -> Mapping[str, object]: ...

    async def create_item(self, body: dict[str, object]) -> Mapping[str, object]: ...

    async def upsert_item(self, body: dict[str, object]) -> Mapping[str, object]: ...

    async def replace_item(
        self,
        *,
        item: str,
        body: dict[str, object],
        etag: str | None,
        match_condition: object | None,
    ) -> Mapping[str, object]: ...

    async def delete_item(self, *, item: str, partition_key: str) -> None: ...

    def query_items(
        self,
        query: str,
        *,
        parameters: list[dict[str, object]] | None = None,
        partition_key: str,
        max_item_count: int | None = None,
    ) -> AsyncIterable[Mapping[str, object]]: ...


@runtime_checkable
class CosmosPagedQueryLike(Protocol):
    def by_page(
        self,
        continuation_token: str | None = None,
    ) -> AsyncIterator[AsyncIterator[Mapping[str, object]]]: ...


class CosmosAccountClientLike(Protocol):
    async def read_account(self) -> Mapping[str, object]: ...

    async def close(self) -> None: ...


@dataclass(slots=True)
class _PartitionReplayStream:
    items: AsyncIterator[Mapping[str, object]]
    current: CosmosStoredEvent | None


class AzureCosmosOutboxClient:
    """Azure Cosmos point-operation client.

    The client supports point insert/read/replace/delete, account validation,
    persisted partition discovery, event-id indexing, bounded partition-scoped
    candidate queries, and paged failover replay iteration. Live-provider
    execution remains the certification boundary for exact Azure SDK behavior.
    """

    def __init__(
        self,
        container: CosmosContainerLike,
        *,
        account_client: CosmosAccountClientLike | None = None,
        known_partition_keys: Collection[str] = (),
        use_partition_registry: bool = True,
    ) -> None:
        self.container = container
        self.account_client = account_client
        self.partition_keys_by_event_id: dict[str, str] = {}
        self.known_partition_keys: set[str] = set(known_partition_keys)
        self.use_partition_registry = use_partition_registry

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
            partition_key = await self._lookup_partition_key_for_event_id(event_id)
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
                await self._delete_event_index(event_id)
                self.partition_keys_by_event_id.pop(event_id, None)
                return None
            raise
        record = decode_cosmos_item(item)
        await self._remember_partition_key(record)
        return record

    async def insert(self, record: CosmosStoredEvent) -> CosmosStoredEvent:
        index_item: Mapping[str, object]
        try:
            index_item = await self._create_event_index(record)
        except Exception as exc:
            if _is_azure_error(
                exc, {"CosmosResourceExistsError", "ResourceExistsError"}
            ):
                existing = await self._record_from_event_index(record.event.event_id)
                if existing is None:
                    raise RetryableStoreError(
                        "Cosmos event index exists without target event"
                    ) from exc
                try:
                    raise_if_incompatible_duplicate(existing.event, record.event)
                except DuplicateEventConflictError as duplicate_exc:
                    raise duplicate_exc from exc
                return existing
            raise
        try:
            item = await self.container.create_item(encode_cosmos_item(record))
        except Exception:
            await self._delete_event_index(record.event.event_id)
            raise
        stored = decode_cosmos_item(item)
        await self._commit_event_index(stored, index_item=index_item)
        await self._remember_partition_key(stored)
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
        await self._remember_partition_key(stored)
        return stored

    async def list_records(self) -> Sequence[CosmosStoredEvent]:
        raise ConfigurationError(
            "AzureCosmosOutboxClient does not support cross-partition list_records(); "
            "use known partition-scoped candidate queries instead"
        )

    async def claim_batch_pending(
        self,
        *,
        limit: int,
        now: datetime,
        claim_timeout: timedelta,
    ) -> Sequence[CosmosStoredEvent]:
        require_positive_limit(limit)
        records: list[CosmosStoredEvent] = []
        claimable_seen = 0
        stale_claimed_before = now - claim_timeout
        parameters = [
            _query_parameter("@pending_status", OutboxStatus.PENDING.value),
            _query_parameter("@in_flight_status", OutboxStatus.IN_FLIGHT.value),
            _query_parameter("@now_epoch_ms", _epoch_ms(now)),
            _query_parameter(
                "@stale_claimed_before_epoch_ms",
                _epoch_ms(stale_claimed_before),
            ),
        ]
        for record in sorted(
            [
                record
                async for record in self._iter_known_partition_candidates(
                    _CLAIM_CANDIDATES_QUERY,
                    parameters=parameters,
                    max_item_count=limit,
                    max_records_per_partition=limit,
                )
            ],
            key=claim_order_key,
        ):
            if is_claimable_record(record, now=now, claim_timeout=claim_timeout):
                if claimable_seen >= limit:
                    continue
                records.append(record)
                claimable_seen += 1
            elif _fresh_ordering_blocker(
                record,
                now=now,
                claim_timeout=claim_timeout,
            ):
                records.append(record)
        records.sort(key=claim_order_key)
        return tuple(records)

    async def list_failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> Sequence[CosmosStoredEvent]:
        require_positive_limit(limit)
        return tuple(
            [
                record
                async for record in self.iter_failover_replay_candidates(
                    failover_started_at=failover_started_at,
                    limit=limit,
                    exclude_event_ids=exclude_event_ids,
                )
            ]
        )

    async def iter_failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
        page_size: int = 100,
    ) -> AsyncIterator[CosmosStoredEvent]:
        require_positive_limit(limit)
        require_positive_limit(page_size, field_name="page_size")
        parameters = [
            _query_parameter("@pending_status", OutboxStatus.PENDING.value),
            _query_parameter("@in_flight_status", OutboxStatus.IN_FLIGHT.value),
            _query_parameter("@sent_status", OutboxStatus.SENT.value),
            _query_parameter(
                "@failover_started_at_epoch_ms",
                _epoch_ms(failover_started_at),
            ),
            _query_parameter("@exclude_event_ids", tuple(sorted(exclude_event_ids))),
        ]
        yielded = 0
        locked_ordering_scopes: set[str] = set()
        streams = [
            stream
            async for stream in self._iter_replay_partition_streams(
                parameters=parameters,
                page_size=min(limit, page_size),
                failover_started_at=failover_started_at,
                exclude_event_ids=exclude_event_ids,
                locked_ordering_scopes=locked_ordering_scopes,
            )
            if stream.current is not None
        ]
        while streams and yielded < limit:
            stream = min(streams, key=_partition_stream_key)
            current = stream.current
            if current is None:
                streams.remove(stream)
                continue
            yield current
            yielded += 1
            scoped_key = ordering_scope(current.event)
            if scoped_key is not None:
                locked_ordering_scopes.add(scoped_key)
            if yielded >= limit:
                break
            stream.current = await self._next_replay_candidate(
                stream.items,
                failover_started_at=failover_started_at,
                exclude_event_ids=exclude_event_ids,
                locked_ordering_scopes=locked_ordering_scopes,
            )
            if stream.current is None:
                streams.remove(stream)

    async def list_cleanup_candidates(
        self,
        *,
        now: datetime,
        safety_margin: timedelta,
        limit: int | None = None,
    ) -> Sequence[CosmosStoredEvent]:
        expires_before = now - safety_margin
        records = [
            record
            for record in await self._query_known_partitions(
                _CLEANUP_CANDIDATES_QUERY,
                parameters=[
                    _query_parameter("@sent_status", OutboxStatus.SENT.value),
                    _query_parameter(
                        "@expires_before_epoch_ms",
                        _epoch_ms(expires_before),
                    ),
                ],
                max_item_count=limit,
            )
            if record.status is OutboxStatus.SENT
            and now > record.event.expires_at + safety_margin
        ]
        records.sort(key=lambda item: item.event.expires_at)
        if limit is not None:
            records = records[:limit]
        return tuple(records)

    async def delete(self, event_id: str) -> None:
        partition_key = self.partition_keys_by_event_id.pop(event_id, None)
        if partition_key is None:
            partition_key = await self._lookup_partition_key_for_event_id(event_id)
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
        if all(
            partition_key != key for key in self.partition_keys_by_event_id.values()
        ):
            self.known_partition_keys.discard(partition_key)
        await self._delete_event_index(event_id)

    async def repair_event_index(self, event_id: str) -> bool:
        """Remove a dangling event index whose target event no longer exists."""
        partition_key = await self._lookup_partition_key_for_event_id(event_id)
        if partition_key is None:
            return False
        try:
            await self.container.read_item(
                item=event_id,
                partition_key=partition_key,
            )
        except Exception as exc:
            if _is_azure_error(
                exc, {"CosmosResourceNotFoundError", "ResourceNotFoundError"}
            ):
                await self._delete_event_index(event_id)
                self.partition_keys_by_event_id.pop(event_id, None)
                return True
            raise
        return False

    async def add_known_partition_key(self, partition_key: str) -> None:
        await self._remember_partition_key_value(partition_key, persist=True)

    def add_known_partition_key_local(self, partition_key: str) -> None:
        self.known_partition_keys.add(partition_key)

    async def _remember_partition_key(self, record: CosmosStoredEvent) -> None:
        self.partition_keys_by_event_id[record.event.event_id] = record.partition_key
        await self._remember_partition_key_value(record.partition_key, persist=True)

    async def _create_event_index(
        self,
        record: CosmosStoredEvent,
    ) -> Mapping[str, object]:
        try:
            return await self.container.create_item(
                {
                    "id": _event_index_id(record.event.event_id),
                    "pk": _CONTROL_PARTITION_KEY,
                    "kind": _EVENT_INDEX_KIND,
                    "event_id": record.event.event_id,
                    "target_id": record.event.event_id,
                    "partition_key": record.partition_key,
                    "fingerprint": _event_index_fingerprint(record.event),
                    "state": _EVENT_INDEX_RESERVED,
                }
            )
        except Exception as exc:
            if _is_azure_error(
                exc, {"CosmosResourceExistsError", "ResourceExistsError"}
            ):
                raise
            raise RetryableStoreError("Cosmos event index create failed") from exc

    async def _commit_event_index(
        self,
        record: CosmosStoredEvent,
        *,
        index_item: Mapping[str, object],
    ) -> None:
        try:
            await self.container.replace_item(
                item=_event_index_id(record.event.event_id),
                body={
                    "id": _event_index_id(record.event.event_id),
                    "pk": _CONTROL_PARTITION_KEY,
                    "kind": _EVENT_INDEX_KIND,
                    "event_id": record.event.event_id,
                    "target_id": record.event.event_id,
                    "partition_key": record.partition_key,
                    "fingerprint": _event_index_fingerprint(record.event),
                    "state": _EVENT_INDEX_COMMITTED,
                },
                etag=_optional_str(index_item, "_etag"),
                match_condition=_if_not_modified(),
            )
        except Exception as exc:
            if _is_azure_error(
                exc,
                {"CosmosAccessConditionFailedError", "ResourceModifiedError"},
            ):
                raise ClaimConflictError(
                    "event index etag precondition failed"
                ) from exc
            raise RetryableStoreError("Cosmos event index commit failed") from exc

    async def _lookup_partition_key_for_event_id(self, event_id: str) -> str | None:
        try:
            item = await self.container.read_item(
                item=_event_index_id(event_id),
                partition_key=_CONTROL_PARTITION_KEY,
            )
        except Exception as exc:
            if _is_azure_error(
                exc, {"CosmosResourceNotFoundError", "ResourceNotFoundError"}
            ):
                return None
            raise
        partition_key = _partition_key_from_event_index_item(item, event_id=event_id)
        self.partition_keys_by_event_id[event_id] = partition_key
        await self._remember_partition_key_value(partition_key, persist=False)
        return partition_key

    async def _delete_event_index(self, event_id: str) -> None:
        try:
            await self.container.delete_item(
                item=_event_index_id(event_id),
                partition_key=_CONTROL_PARTITION_KEY,
            )
        except Exception as exc:
            if _is_azure_error(
                exc, {"CosmosResourceNotFoundError", "ResourceNotFoundError"}
            ):
                return
            raise

    async def _record_from_event_index(self, event_id: str) -> CosmosStoredEvent | None:
        partition_key = await self._lookup_partition_key_for_event_id(event_id)
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
        await self._remember_partition_key(record)
        return record

    async def _remember_partition_key_value(
        self,
        partition_key: str,
        *,
        persist: bool,
    ) -> None:
        if partition_key in self.known_partition_keys:
            return
        self.known_partition_keys.add(partition_key)
        if persist and self.use_partition_registry:
            await self._persist_partition_key(partition_key)

    async def _persist_partition_key(self, partition_key: str) -> None:
        try:
            await self.container.upsert_item(
                {
                    "id": f"{_PARTITION_REGISTRY_ID_PREFIX}{partition_key}",
                    "pk": _CONTROL_PARTITION_KEY,
                    "kind": _PARTITION_REGISTRY_KIND,
                    "partition_key": partition_key,
                }
            )
        except Exception as exc:
            raise RetryableStoreError(
                "Cosmos partition registry update failed"
            ) from exc

    async def load_registered_partition_keys(self) -> Sequence[str]:
        records: list[str] = []
        items = self.container.query_items(
            query=_PARTITION_REGISTRY_QUERY,
            parameters=[
                _query_parameter("@kind", _PARTITION_REGISTRY_KIND),
                _query_parameter("@partition_key", _CONTROL_PARTITION_KEY),
            ],
            partition_key=_CONTROL_PARTITION_KEY,
        )
        async for item in items:
            partition_key = _partition_key_from_registry_item(item)
            if partition_key is None:
                continue
            await self._remember_partition_key_value(partition_key, persist=False)
            records.append(partition_key)
        return tuple(records)

    async def _query_known_partitions(
        self,
        query: str,
        *,
        parameters: list[dict[str, object]],
        max_item_count: int | None,
    ) -> Sequence[CosmosStoredEvent]:
        records = [
            record
            async for record in self._iter_known_partition_candidates(
                query,
                parameters=parameters,
                max_item_count=max_item_count,
                max_records_per_partition=None,
            )
        ]
        return tuple(records)

    async def _iter_known_partition_candidates(
        self,
        query: str,
        *,
        parameters: list[dict[str, object]],
        max_item_count: int | None,
        max_records_per_partition: int | None,
    ) -> AsyncIterator[CosmosStoredEvent]:
        if self.use_partition_registry:
            await self.load_registered_partition_keys()
        for partition_key in sorted(self.known_partition_keys):
            partition_parameters = [
                *parameters,
                _query_parameter("@partition_key", partition_key),
            ]
            items = self.container.query_items(
                query=query,
                parameters=partition_parameters,
                partition_key=partition_key,
                max_item_count=max_item_count,
            )
            iterator = _iter_query_items(items)
            yielded = 0
            while (
                max_records_per_partition is None or yielded < max_records_per_partition
            ):
                try:
                    item = await anext(iterator)
                except StopAsyncIteration:
                    break
                yielded += 1
                record = decode_cosmos_item(item)
                self.partition_keys_by_event_id[record.event.event_id] = (
                    record.partition_key
                )
                await self._remember_partition_key_value(
                    record.partition_key,
                    persist=False,
                )
                yield record

    async def _iter_replay_partition_streams(
        self,
        *,
        parameters: list[dict[str, object]],
        page_size: int,
        failover_started_at: datetime,
        exclude_event_ids: Collection[str],
        locked_ordering_scopes: set[str],
    ) -> AsyncIterator[_PartitionReplayStream]:
        if self.use_partition_registry:
            await self.load_registered_partition_keys()
        for partition_key in sorted(self.known_partition_keys):
            partition_parameters = [
                *parameters,
                _query_parameter("@partition_key", partition_key),
            ]
            items = self.container.query_items(
                query=_REPLAY_CANDIDATES_QUERY,
                parameters=partition_parameters,
                partition_key=partition_key,
                max_item_count=page_size,
            )
            iterator = _iter_query_items(items)
            yield _PartitionReplayStream(
                items=iterator,
                current=await self._next_replay_candidate(
                    iterator,
                    failover_started_at=failover_started_at,
                    exclude_event_ids=exclude_event_ids,
                    locked_ordering_scopes=locked_ordering_scopes,
                ),
            )

    async def _next_replay_candidate(
        self,
        items: AsyncIterator[Mapping[str, object]],
        *,
        failover_started_at: datetime,
        exclude_event_ids: Collection[str],
        locked_ordering_scopes: set[str],
    ) -> CosmosStoredEvent | None:
        async for item in items:
            record = decode_cosmos_item(item)
            self.partition_keys_by_event_id[record.event.event_id] = (
                record.partition_key
            )
            await self._remember_partition_key_value(
                record.partition_key,
                persist=False,
            )
            if not _replay_candidate(
                record,
                failover_started_at=failover_started_at,
                exclude_event_ids=exclude_event_ids,
            ):
                continue
            scoped_key = ordering_scope(record.event)
            if scoped_key is not None and scoped_key in locked_ordering_scopes:
                continue
            return record
        return None

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


def _fresh_ordering_blocker(
    record: CosmosStoredEvent,
    *,
    now: datetime,
    claim_timeout: timedelta,
) -> bool:
    return (
        ordering_scope(record.event) is not None
        and record.status is OutboxStatus.IN_FLIGHT
        and record.claimed_at is not None
        and record.claimed_at + claim_timeout > now
    )


def _replay_candidate(
    record: CosmosStoredEvent,
    *,
    failover_started_at: datetime,
    exclude_event_ids: Collection[str],
) -> bool:
    return (
        record.event.event_id not in exclude_event_ids
        and record.status
        in {
            OutboxStatus.PENDING,
            OutboxStatus.IN_FLIGHT,
            OutboxStatus.SENT,
        }
        and record.event.expires_at >= failover_started_at
    )


def _partition_key_from_registry_item(item: Mapping[str, object]) -> str | None:
    if item.get("kind") != _PARTITION_REGISTRY_KIND:
        return None
    partition_key = item.get("partition_key")
    if isinstance(partition_key, str):
        return partition_key
    raise RetryableStoreError("Cosmos partition registry item is missing partition_key")


def _partition_stream_key(
    stream: _PartitionReplayStream,
) -> tuple[datetime, str]:
    if stream.current is None:
        return (datetime.max.replace(tzinfo=UTC), "")
    return (stream.current.event.created_at, stream.current.event.event_id)


async def _iter_query_items(
    items: AsyncIterable[Mapping[str, object]],
) -> AsyncIterator[Mapping[str, object]]:
    if isinstance(items, CosmosPagedQueryLike):
        async for page in items.by_page():
            async for item in page:
                yield item
        return
    async for item in items:
        yield item


def _partition_key_from_event_index_item(
    item: Mapping[str, object],
    *,
    event_id: str,
) -> str:
    if item.get("kind") != _EVENT_INDEX_KIND or item.get("event_id") != event_id:
        raise RetryableStoreError("Cosmos event index item does not match event_id")
    state = item.get("state")
    if state not in {_EVENT_INDEX_RESERVED, _EVENT_INDEX_COMMITTED}:
        raise RetryableStoreError("Cosmos event index item has invalid state")
    partition_key = item.get("partition_key")
    if isinstance(partition_key, str):
        return partition_key
    raise RetryableStoreError("Cosmos event index item is missing partition_key")


def _event_index_id(event_id: str) -> str:
    digest = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
    return f"{_EVENT_INDEX_ID_PREFIX}{digest}"


def _event_index_fingerprint(event: OutboxEvent) -> str:
    headers = {
        name: base64.b64encode(value).decode("ascii")
        for name, value in sorted(event.headers.items())
    }
    value = {
        "event_id": event.event_id,
        "topic": event.topic,
        "payload": base64.b64encode(event.payload).decode("ascii"),
        "key": (
            base64.b64encode(event.key).decode("ascii")
            if event.key is not None
            else None
        ),
        "headers": headers,
        "ordering_key": event.ordering_key,
        "ordering_sequence": event.ordering_sequence,
        "publishing_mode": event.publishing_mode.value,
        "schema_id": event.schema_id,
        "schema_version": event.schema_version,
    }
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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
