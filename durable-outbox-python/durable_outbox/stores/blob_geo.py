import asyncio
import base64
import binascii
import hmac
import json
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from types import MappingProxyType
from typing import Any, Literal, Protocol
from uuid import uuid4

from durable_outbox.core.admin import AdminActionStatus
from durable_outbox.core.capabilities import OutboxCapabilities
from durable_outbox.core.claim import (
    InFlightOrderingIndex,
    claim_order_key,
    is_claimable_record,
)
from durable_outbox.core.claim_token import claim_token_matches
from durable_outbox.core.duplicates import raise_if_incompatible_duplicate
from durable_outbox.core.errors import (
    ClaimConflictError,
    ConfigurationError,
    RetryableStoreError,
)
from durable_outbox.core.model import (
    AcceptedReceipt,
    ClaimedEvent,
    OutboxEvent,
    OutboxStatus,
    PublishingMode,
    PublishResult,
)
from durable_outbox.core.ordering import (
    OrderingLockBackend,
    OrderingLockLease,
    ordering_scope,
)
from durable_outbox.core.time import Clock, SystemClock
from durable_outbox.core.validation import (
    enforce_metadata_safe,
    enforce_payload_size,
    require_positive_limit,
)
from durable_outbox.stores.memory import StoredEvent
from durable_outbox.telemetry.metrics import MetricsAdapter, NoopMetrics

type BlobRegionName = Literal["primary", "secondary"]
_LOGGER = logging.getLogger(__name__)
MAX_BLOB_PAYLOAD_BYTES = 10 * 1024 * 1024


def event_blob_name(event_id: str) -> str:
    safe_id = sha256(event_id.encode("utf-8")).hexdigest()
    return f"outbox/v1/events/{safe_id}.json"


def ordering_lock_blob_name(environment: str, topic: str, ordering_key: str) -> str:
    topic_hash = sha256(topic.encode("utf-8")).hexdigest()
    key_hash = sha256(ordering_key.encode("utf-8")).hexdigest()
    return f"outbox/v1/key-locks/{environment}/{topic_hash}/{key_hash}.lock"


def cleanup_freeze_blob_name(environment: str) -> str:
    environment_hash = sha256(environment.encode("utf-8")).hexdigest()
    return f"outbox/v1/control/{environment_hash}/cleanup-freeze.json"


def _resolve_ordering_lock_lease_duration(
    *,
    claim_timeout: timedelta,
    ordering_lock_lease_duration: timedelta | None,
) -> timedelta:
    if ordering_lock_lease_duration is None:
        return claim_timeout
    if ordering_lock_lease_duration != claim_timeout:
        raise ConfigurationError(
            "ordering_lock_lease_duration must match claim_timeout until lock "
            "renewal is supported"
        )
    return ordering_lock_lease_duration


@dataclass(frozen=True, slots=True)
class BlobObject:
    name: str
    content: bytes
    metadata: Mapping[str, str]
    etag: str


@dataclass(frozen=True, slots=True)
class _ClaimMutationSnapshot:
    status: OutboxStatus
    claim_token: str | None
    claimed_at: datetime | None
    attempt_count: int
    etag: str | None


class BlobClientProtocol(Protocol):
    async def get_blob(self, name: str) -> BlobObject | None: ...

    async def put_blob(
        self,
        name: str,
        content: bytes,
        metadata: Mapping[str, str],
        *,
        if_none_match: bool = False,
        if_match: str | None = None,
    ) -> BlobObject: ...

    async def delete_blob(self, name: str, *, if_match: str | None = None) -> bool: ...

    async def list_blobs(self, *, prefix: str) -> list[BlobObject]: ...


class BlobPreconditionFailedError(RetryableStoreError):
    """Raised when a conditional blob write loses an optimistic race."""


class InMemoryBlobClient:
    def __init__(self) -> None:
        self._blobs: dict[str, BlobObject] = {}
        self._versions: dict[str, int] = {}

    async def get_blob(self, name: str) -> BlobObject | None:
        blob = self._blobs.get(name)
        return _copy_blob(blob) if blob is not None else None

    async def put_blob(
        self,
        name: str,
        content: bytes,
        metadata: Mapping[str, str],
        *,
        if_none_match: bool = False,
        if_match: str | None = None,
    ) -> BlobObject:
        current = self._blobs.get(name)
        if if_none_match and current is not None:
            raise BlobPreconditionFailedError("blob already exists")
        if if_match is not None and (current is None or current.etag != if_match):
            raise BlobPreconditionFailedError("blob etag precondition failed")
        version = self._versions.get(name, 0) + 1
        self._versions[name] = version
        blob = BlobObject(
            name=name,
            content=bytes(content),
            metadata=dict(metadata),
            etag=f'"{version}"',
        )
        self._blobs[name] = blob
        return _copy_blob(blob)

    async def delete_blob(self, name: str, *, if_match: str | None = None) -> bool:
        current = self._blobs.get(name)
        if current is None:
            return False
        if if_match is not None and current.etag != if_match:
            raise BlobPreconditionFailedError("blob etag precondition failed")
        del self._blobs[name]
        return True

    async def list_blobs(self, *, prefix: str) -> list[BlobObject]:
        return [
            _copy_blob(blob)
            for name, blob in sorted(self._blobs.items())
            if name.startswith(prefix)
        ]


class BlobOrderingLockBackend:
    def __init__(self, client: BlobClientProtocol) -> None:
        self.client = client

    async def acquire(
        self,
        *,
        lock_name: str,
        owner_token: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> OrderingLockLease | None:
        expires_at = now + lease_duration
        metadata = _lock_metadata(owner_token=owner_token, expires_at=expires_at)
        try:
            await self.client.put_blob(
                lock_name,
                b"",
                metadata,
                if_none_match=True,
            )
        except BlobPreconditionFailedError:
            current = await self.client.get_blob(lock_name)
            if current is None or not _lock_is_expired(current, now=now):
                return None
            try:
                await self.client.put_blob(
                    lock_name,
                    b"",
                    metadata,
                    if_match=current.etag,
                )
            except BlobPreconditionFailedError:
                return None
        return OrderingLockLease(
            lock_name=lock_name,
            owner_token=owner_token,
            expires_at=expires_at,
        )

    async def release(self, lease: OrderingLockLease) -> None:
        current = await self.client.get_blob(lease.lock_name)
        if current is None or not claim_token_matches(
            current.metadata.get("owner_token"),
            lease.owner_token,
        ):
            return
        try:
            await self.client.delete_blob(lease.lock_name, if_match=current.etag)
        except BlobPreconditionFailedError:
            return


class BlobOutboxStore:
    capabilities = OutboxCapabilities(
        store_name="BlobOutboxStore",
        rpo_zero_for_accepted_events=False,
        supports_ordering=True,
        supports_failover_replay=True,
        supports_ttl_freeze=True,
        max_payload_bytes=MAX_BLOB_PAYLOAD_BYTES,
        notes=("GRS/RA-GRS alone is not sufficient for RPO=0.",),
    )

    def __init__(
        self,
        *,
        client: BlobClientProtocol | None = None,
        environment: str = "default",
        store_name: str = "BlobOutboxStore",
        ordering_lock_backend: OrderingLockBackend | None = None,
        ordering_lock_lease_duration: timedelta | None = None,
        claim_timeout: timedelta = timedelta(minutes=5),
        fingerprint_key: bytes | None = None,
        clock: Clock | None = None,
    ) -> None:
        if client is None:
            raise ConfigurationError(
                "BlobOutboxStore requires an explicit blob client; "
                "use BlobOutboxStore.for_testing() for in-memory tests"
            )
        self.client = client
        if fingerprint_key is not None and not isinstance(fingerprint_key, bytes):
            raise ConfigurationError("fingerprint_key must be bytes")
        self.fingerprint_key = fingerprint_key
        enforce_metadata_safe(environment, field_name="environment")
        self.environment = environment
        ordering_lock_lease_duration = _resolve_ordering_lock_lease_duration(
            claim_timeout=claim_timeout,
            ordering_lock_lease_duration=ordering_lock_lease_duration,
        )
        self.ordering_lock_backend = ordering_lock_backend or BlobOrderingLockBackend(
            client
        )
        self.ordering_lock_lease_duration = ordering_lock_lease_duration
        self.claim_timeout = claim_timeout
        self.clock = clock or SystemClock()
        self.cleanup_frozen = False
        self.cleanup_freeze_reason: str | None = None
        self.records: dict[str, StoredEvent] = {}
        self._record_etags: dict[str, str] = {}
        self._event_fingerprints: dict[str, tuple[OutboxEvent, str]] = {}
        self._in_flight_ordering_index = InFlightOrderingIndex()
        self._ordering_leases_by_event_id: dict[str, OrderingLockLease] = {}
        self.capabilities = OutboxCapabilities(
            store_name=store_name,
            rpo_zero_for_accepted_events=False,
            supports_ordering=True,
            supports_failover_replay=True,
            supports_ttl_freeze=True,
            max_payload_bytes=MAX_BLOB_PAYLOAD_BYTES,
            notes=("GRS/RA-GRS alone is not sufficient for RPO=0.",),
        )

    @classmethod
    def for_testing(
        cls,
        *,
        environment: str = "test",
        ordering_lock_backend: OrderingLockBackend | None = None,
        ordering_lock_lease_duration: timedelta | None = None,
        claim_timeout: timedelta = timedelta(minutes=5),
        fingerprint_key: bytes | None = None,
        clock: Clock | None = None,
    ) -> BlobOutboxStore:
        return cls(
            client=InMemoryBlobClient(),
            environment=environment,
            store_name="InMemoryBlobOutboxStore",
            ordering_lock_backend=ordering_lock_backend,
            ordering_lock_lease_duration=ordering_lock_lease_duration,
            claim_timeout=claim_timeout,
            fingerprint_key=fingerprint_key,
            clock=clock,
        )

    async def put(self, event: OutboxEvent) -> AcceptedReceipt:
        enforce_payload_size(event, self.capabilities)
        now = self.clock.utcnow()
        record = await self._load_record(event.event_id)
        if record is None:
            record = StoredEvent(event=event, accepted=True, accepted_at=now)
            await self._write_new_record(record)
        else:
            self._ensure_compatible_duplicate(record, event)
            if not record.accepted:
                record.accepted = True
                record.accepted_at = record.accepted_at or now
                await self._save_record(record)
        return AcceptedReceipt(
            event_id=event.event_id,
            accepted_at=record.accepted_at or now,
            rpo_zero=self.capabilities.rpo_zero_for_accepted_events,
            store=self.capabilities.store_name,
            durability_witness=(f"blob:{self.environment}",),
        )

    async def claim_batch(self, *, limit: int) -> list[ClaimedEvent]:
        require_positive_limit(limit)
        await self._refresh_records()
        now = self.clock.utcnow()
        claimed: list[ClaimedEvent] = []
        locked_keys = self._in_flight_ordering_keys(now)
        for record in self._ordered_records():
            if len(claimed) >= limit:
                break
            if not self._eligible_for_claim(record, now):
                continue
            ordering_key = record.event.effective_ordering_key
            scoped_key = _ordering_scope(record.event)
            if scoped_key is not None and scoped_key in locked_keys:
                continue
            lease = await self._acquire_ordering_lease(record.event, now=now)
            if ordering_key is not None and lease is None:
                continue
            event_id = record.event.event_id
            previous = _claim_mutation_snapshot(
                record,
                etag=self._record_etags.get(event_id),
            )
            token = str(uuid4())
            record.status = OutboxStatus.IN_FLIGHT
            record.claim_token = token
            record.claimed_at = now
            record.attempt_count += 1
            try:
                await self._save_record(record)
            except BlobPreconditionFailedError:
                _restore_claim_mutation(record, previous)
                if previous.etag is None:
                    self._record_etags.pop(event_id, None)
                else:
                    self._record_etags[event_id] = previous.etag
                if lease is not None:
                    await self.ordering_lock_backend.release(lease)
                continue
            if lease is not None:
                self._ordering_leases_by_event_id[record.event.event_id] = lease
            if scoped_key is not None:
                self._in_flight_ordering_index.record_claim(
                    record.event, claimed_at=now
                )
                locked_keys.add(scoped_key)
            claimed.append(
                ClaimedEvent(
                    event=record.event,
                    claim_token=token,
                    attempt_count=record.attempt_count,
                )
            )
        return claimed

    async def mark_sent(self, claimed: ClaimedEvent, result: PublishResult) -> None:
        record = await self._claimed_record(claimed)
        record.status = OutboxStatus.SENT
        record.sent_at = result.published_at
        record.publish_result = result
        record.claim_token = None
        record.claimed_at = None
        await self._save_record(record)
        self._in_flight_ordering_index.release(record.event)
        await self._release_ordering_lease(claimed.event.event_id)

    async def mark_pending_after_retryable_failure(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
        next_attempt_at: datetime,
    ) -> None:
        record = await self._claimed_record(claimed)
        record.status = OutboxStatus.PENDING
        record.next_attempt_at = next_attempt_at
        record.last_error_type = error_type
        record.last_error = error_message
        record.claim_token = None
        record.claimed_at = None
        await self._save_record(record)
        self._in_flight_ordering_index.release(record.event)
        await self._release_ordering_lease(claimed.event.event_id)

    async def mark_failed(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
    ) -> None:
        record = await self._claimed_record(claimed)
        record.status = OutboxStatus.FAILED
        record.failed_at = self.clock.utcnow()
        record.last_error_type = error_type
        record.last_error = error_message
        record.claim_token = None
        record.claimed_at = None
        await self._save_record(record)
        self._in_flight_ordering_index.release(record.event)
        await self._release_ordering_lease(claimed.event.event_id)

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
    ) -> list[ClaimedEvent]:
        require_positive_limit(limit)
        await self._refresh_records()
        candidates: list[ClaimedEvent] = []
        originals: dict[str, StoredEvent] = {}
        try:
            for record in sorted(
                self.records.values(), key=lambda item: item.event.created_at
            ):
                if len(candidates) >= limit:
                    break
                if not record.accepted:
                    continue
                if record.status not in {
                    OutboxStatus.PENDING,
                    OutboxStatus.IN_FLIGHT,
                    OutboxStatus.SENT,
                }:
                    continue
                if record.event.expires_at < failover_started_at:
                    continue
                event_id = record.event.event_id
                originals.setdefault(event_id, _clone_record(record))
                token = str(uuid4())
                source_status = record.status
                record.status = OutboxStatus.IN_FLIGHT
                record.claim_token = token
                record.claimed_at = self.clock.utcnow()
                record.attempt_count += 1
                try:
                    await self._save_record(record)
                except BlobPreconditionFailedError:
                    self.records[event_id] = originals.pop(event_id)
                    continue
                candidates.append(
                    ClaimedEvent(
                        event=record.event,
                        claim_token=token,
                        attempt_count=record.attempt_count,
                        source_status=source_status,
                    )
                )
        except BaseException:
            for original in reversed(tuple(originals.values())):
                self.records[original.event.event_id] = original
                await self._save_record(original)
            raise
        return candidates

    async def freeze_cleanup(self, *, reason: str) -> None:
        await self.client.put_blob(
            cleanup_freeze_blob_name(self.environment),
            json.dumps(
                {
                    "reason": reason,
                    "frozen_at": _encode_datetime(self.clock.utcnow()),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
            {"control": "cleanup-freeze"},
        )
        self.cleanup_frozen = True
        self.cleanup_freeze_reason = reason

    async def resume_cleanup(self) -> None:
        await self.client.delete_blob(cleanup_freeze_blob_name(self.environment))
        self.cleanup_frozen = False
        self.cleanup_freeze_reason = None

    async def cleanup_sent(self, *, now: datetime, safety_margin: timedelta) -> int:
        if await self._cleanup_is_frozen():
            return 0
        await self._refresh_records()
        deleted = 0
        for event_id, record in list(self.records.items()):
            if record.status is not OutboxStatus.SENT:
                continue
            if now <= record.event.expires_at + safety_margin:
                continue
            try:
                if await self.client.delete_blob(
                    event_blob_name(event_id), if_match=self._record_etags.get(event_id)
                ):
                    deleted += 1
            except BlobPreconditionFailedError:
                continue
            self.records.pop(event_id, None)
            self._record_etags.pop(event_id, None)
            self._event_fingerprints.pop(event_id, None)
            self._in_flight_ordering_index.release(record.event)
        return deleted

    async def repair_failed_to_pending(self, *, event_id: str) -> AdminActionStatus:
        record = await self._load_record(event_id)
        if record is None:
            return AdminActionStatus.NOT_FOUND
        if record.status is not OutboxStatus.FAILED:
            return AdminActionStatus.WRONG_STATE
        record.status = OutboxStatus.PENDING
        record.failed_at = None
        record.attempt_count = 0
        record.last_error_type = None
        record.last_error = None
        record.next_attempt_at = None
        record.claim_token = None
        record.claimed_at = None
        await self._save_record(record)
        self._in_flight_ordering_index.release(record.event)
        return AdminActionStatus.SUCCESS

    async def _cleanup_is_frozen(self) -> bool:
        marker = await self.client.get_blob(cleanup_freeze_blob_name(self.environment))
        if marker is None:
            self.cleanup_frozen = False
            self.cleanup_freeze_reason = None
            return False
        self.cleanup_frozen = True
        self.cleanup_freeze_reason = _cleanup_freeze_reason(marker)
        return True

    async def replay_event(self, *, event_id: str) -> AdminActionStatus:
        record = await self._load_record(event_id)
        if record is None:
            return AdminActionStatus.NOT_FOUND
        record.status = OutboxStatus.PENDING
        record.claim_token = None
        record.claimed_at = None
        record.next_attempt_at = None
        record.sent_at = None
        record.publish_result = None
        record.failed_at = None
        record.last_error_type = None
        record.last_error = None
        await self._save_record(record)
        self._in_flight_ordering_index.release(record.event)
        await self._release_ordering_lease(event_id)
        return AdminActionStatus.SUCCESS

    async def prepare_event(self, event: OutboxEvent) -> None:
        """Write or validate a PREPARED-only record for dual-region acceptance."""
        await self._put_prepared(event)

    async def accept_prepared_event(self, event: OutboxEvent) -> None:
        """Promote a prepared record to the regional accepted durability boundary."""
        await self._accept_prepared(event)

    async def load_region_record(self, event_id: str) -> StoredEvent | None:
        """Load one event record and refresh this region's local cache."""
        return await self._load_record(event_id)

    async def refresh_region_records(self) -> None:
        """Refresh this region's event cache from backing blob storage."""
        await self._refresh_records()

    async def write_region_record(self, record: StoredEvent) -> None:
        """Write a new cached record into this region with create-only semantics."""
        await self._write_new_record(record)

    async def save_region_record(self, record: StoredEvent) -> None:
        """Persist an existing cached record into this region with etag checking."""
        await self._save_record(record)

    async def _put_prepared(self, event: OutboxEvent) -> None:
        enforce_payload_size(event, self.capabilities)
        record = await self._load_record(event.event_id)
        if record is None:
            await self._write_new_record(
                StoredEvent(event=event, accepted=False, accepted_at=None)
            )
            return
        self._ensure_compatible_duplicate(record, event)

    async def _accept_prepared(self, event: OutboxEvent) -> None:
        record = await self._load_record(event.event_id)
        if record is None:
            raise RetryableStoreError("region missing prepared event")
        self._ensure_compatible_duplicate(record, event)
        if record.accepted:
            return
        accepted_record = _clone_record(record)
        accepted_record.accepted = True
        accepted_record.accepted_at = accepted_record.accepted_at or self.clock.utcnow()
        await self._save_record(accepted_record)

    async def _load_record(self, event_id: str) -> StoredEvent | None:
        blob = await self.client.get_blob(event_blob_name(event_id))
        if blob is None:
            existing = self.records.get(event_id)
            self.records.pop(event_id, None)
            self._record_etags.pop(event_id, None)
            self._event_fingerprints.pop(event_id, None)
            if existing is not None:
                self._in_flight_ordering_index.release(existing.event)
            return None
        record = _decode_record(blob.content)
        self._verify_and_cache_record_fingerprint(record, blob)
        self.records[event_id] = record
        self._record_etags[event_id] = blob.etag
        return record

    async def _refresh_records(self) -> None:
        blobs = await self.client.list_blobs(prefix="outbox/v1/events/")
        seen_ids: set[str] = set()
        for blob in blobs:
            record = _decode_record(blob.content)
            self._verify_and_cache_record_fingerprint(record, blob)
            event_id = record.event.event_id
            seen_ids.add(event_id)
            self.records[event_id] = record
            self._record_etags[event_id] = blob.etag
        for event_id in set(self.records) - seen_ids:
            self.records.pop(event_id, None)
            self._record_etags.pop(event_id, None)
            self._event_fingerprints.pop(event_id, None)
        self._in_flight_ordering_index.rebuild(
            self.records.values(),
            now=self.clock.utcnow(),
            claim_timeout=self.claim_timeout,
        )

    async def _write_new_record(self, record: StoredEvent) -> None:
        event_fingerprint = self._event_fingerprint_for_record(record)
        blob = await self.client.put_blob(
            event_blob_name(record.event.event_id),
            _encode_record(record),
            _record_metadata(
                record,
                environment=self.environment,
                event_fingerprint=event_fingerprint,
            ),
            if_none_match=True,
        )
        self.records[record.event.event_id] = record
        self._record_etags[record.event.event_id] = blob.etag
        self._event_fingerprints[record.event.event_id] = (
            record.event,
            event_fingerprint,
        )

    async def _save_record(self, record: StoredEvent) -> None:
        event_id = record.event.event_id
        event_fingerprint = self._event_fingerprint_for_record(record)
        blob = await self.client.put_blob(
            event_blob_name(event_id),
            _encode_record(record),
            _record_metadata(
                record,
                environment=self.environment,
                event_fingerprint=event_fingerprint,
            ),
            if_match=self._record_etags.get(event_id),
        )
        self.records[event_id] = record
        self._record_etags[event_id] = blob.etag
        self._event_fingerprints[event_id] = (record.event, event_fingerprint)

    async def _claimed_record(self, claimed: ClaimedEvent) -> StoredEvent:
        record = self.records.get(claimed.event.event_id)
        if record is None:
            loaded = await self._load_record(claimed.event.event_id)
            if loaded is None:
                raise ClaimConflictError("claimed event no longer exists")
            record = loaded
        if not claim_token_matches(record.claim_token, claimed.claim_token):
            raise ClaimConflictError("claim token does not match current owner")
        return record

    def _ensure_compatible_duplicate(
        self, record: StoredEvent, event: OutboxEvent
    ) -> None:
        raise_if_incompatible_duplicate(record.event, event)

    def _event_fingerprint_for_record(self, record: StoredEvent) -> str:
        event_id = record.event.event_id
        cached = self._event_fingerprints.get(event_id)
        if cached is not None:
            cached_event, cached_fingerprint = cached
            if cached_event == record.event:
                return cached_fingerprint
        fingerprint = _event_fingerprint(record.event, key=self.fingerprint_key)
        self._event_fingerprints[event_id] = (record.event, fingerprint)
        return fingerprint

    def _verify_and_cache_record_fingerprint(
        self,
        record: StoredEvent,
        blob: BlobObject,
    ) -> None:
        expected_fingerprint = blob.metadata.get("event_fingerprint")
        actual_fingerprint = _event_fingerprint(record.event, key=self.fingerprint_key)
        if expected_fingerprint != actual_fingerprint:
            raise RetryableStoreError("blob record fingerprint mismatch")
        self._event_fingerprints[record.event.event_id] = (
            record.event,
            actual_fingerprint,
        )

    def _ordered_records(self) -> Iterable[StoredEvent]:
        return sorted(self.records.values(), key=claim_order_key)

    def _eligible_for_claim(self, record: StoredEvent, now: datetime) -> bool:
        if not record.accepted:
            return False
        return is_claimable_record(
            record,
            now=now,
            claim_timeout=self.claim_timeout,
        )

    def _in_flight_ordering_keys(self, now: datetime) -> set[str]:
        return self._in_flight_ordering_index.active_keys(
            now=now,
            claim_timeout=self.claim_timeout,
        )

    async def _acquire_ordering_lease(
        self, event: OutboxEvent, *, now: datetime
    ) -> OrderingLockLease | None:
        ordering_key = event.effective_ordering_key
        if ordering_key is None:
            return None
        lock_name = ordering_lock_blob_name(self.environment, event.topic, ordering_key)
        return await self.ordering_lock_backend.acquire(
            lock_name=lock_name,
            owner_token=str(uuid4()),
            now=now,
            lease_duration=self.ordering_lock_lease_duration,
        )

    async def _release_ordering_lease(self, event_id: str) -> None:
        lease = self._ordering_leases_by_event_id.pop(event_id, None)
        if lease is None:
            return
        await self.ordering_lock_backend.release(lease)


@dataclass(frozen=True, slots=True)
class RegionWrite:
    prepared: bool
    accepted: bool


class DualRegionBlobOutboxStore:
    capabilities = OutboxCapabilities(
        store_name="DualRegionBlobOutboxStore",
        rpo_zero_for_accepted_events=True,
        supports_ordering=True,
        supports_failover_replay=True,
        supports_ttl_freeze=True,
        notes=(
            "RPO=0 is achieved by application-level dual writes.",
            "Azure GRS/RA-GRS alone is not sufficient for RPO=0.",
        ),
    )

    def __init__(
        self,
        *,
        primary_client: BlobClientProtocol | None = None,
        secondary_client: BlobClientProtocol | None = None,
        environment: str = "default",
        store_name: str = "DualRegionBlobOutboxStore",
        active_region: BlobRegionName = "primary",
        claim_timeout: timedelta = timedelta(minutes=5),
        fingerprint_key: bytes | None = None,
        metrics: MetricsAdapter | None = None,
        clock: Clock | None = None,
    ) -> None:
        if active_region not in ("primary", "secondary"):
            msg = "active_region must be 'primary' or 'secondary'"
            raise ValueError(msg)
        if primary_client is None or secondary_client is None:
            raise ConfigurationError(
                "DualRegionBlobOutboxStore requires explicit primary and secondary "
                "blob clients; use DualRegionBlobOutboxStore.for_testing() for "
                "in-memory tests"
            )
        self.clock = clock or SystemClock()
        self.primary = BlobOutboxStore(
            client=primary_client,
            environment=f"{environment}-primary",
            claim_timeout=claim_timeout,
            fingerprint_key=fingerprint_key,
            clock=self.clock,
        )
        self.secondary = BlobOutboxStore(
            client=secondary_client,
            environment=f"{environment}-secondary",
            claim_timeout=claim_timeout,
            fingerprint_key=fingerprint_key,
            clock=self.clock,
        )
        self.metrics = metrics or NoopMetrics()
        self.active_region: BlobRegionName = active_region
        self._pending_mirror_event_ids: set[str] = set()
        self.cleanup_frozen = False
        self.cleanup_freeze_reason: str | None = None
        self.capabilities = OutboxCapabilities(
            store_name=store_name,
            rpo_zero_for_accepted_events=True,
            supports_ordering=True,
            supports_failover_replay=True,
            supports_ttl_freeze=True,
            max_payload_bytes=MAX_BLOB_PAYLOAD_BYTES,
            notes=(
                "RPO=0 is achieved by application-level dual writes.",
                "Azure GRS/RA-GRS alone is not sufficient for RPO=0.",
            ),
        )

    @classmethod
    def for_testing(
        cls,
        *,
        environment: str = "test",
        active_region: BlobRegionName = "primary",
        claim_timeout: timedelta = timedelta(minutes=5),
        fingerprint_key: bytes | None = None,
        metrics: MetricsAdapter | None = None,
        clock: Clock | None = None,
    ) -> DualRegionBlobOutboxStore:
        return cls(
            primary_client=InMemoryBlobClient(),
            secondary_client=InMemoryBlobClient(),
            environment=environment,
            store_name="InMemoryDualRegionBlobOutboxStore",
            active_region=active_region,
            claim_timeout=claim_timeout,
            fingerprint_key=fingerprint_key,
            metrics=metrics,
            clock=clock,
        )

    @property
    def _active(self) -> BlobOutboxStore:
        return self.primary if self.active_region == "primary" else self.secondary

    @property
    def _standby(self) -> BlobOutboxStore:
        return self.secondary if self.active_region == "primary" else self.primary

    @property
    def records(self) -> Mapping[str, StoredEvent]:
        return MappingProxyType(
            {
                event_id: _clone_record(record)
                for event_id, record in self._active.records.items()
            }
        )

    def use_region(self, region: BlobRegionName) -> None:
        if region not in ("primary", "secondary"):
            msg = "region must be 'primary' or 'secondary'"
            raise ValueError(msg)
        self.active_region = region

    def promote_secondary(self) -> None:
        self.use_region("secondary")

    def promote_primary(self) -> None:
        self.use_region("primary")

    async def put(self, event: OutboxEvent) -> AcceptedReceipt:
        await asyncio.gather(
            self._prepare(self.primary, event),
            self._prepare(self.secondary, event),
        )
        await asyncio.gather(
            self._accept(self.primary, event),
            self._accept(self.secondary, event),
        )
        primary = self.primary.records[event.event_id]
        secondary = self.secondary.records[event.event_id]
        accepted_at_candidates = [
            accepted_at
            for accepted_at in (primary.accepted_at, secondary.accepted_at)
            if accepted_at is not None
        ]
        return AcceptedReceipt(
            event_id=event.event_id,
            accepted_at=max(accepted_at_candidates)
            if accepted_at_candidates
            else self.clock.utcnow(),
            rpo_zero=True,
            store=self.capabilities.store_name,
            durability_witness=(
                f"blob:{self.primary.environment}",
                f"blob:{self.secondary.environment}",
            ),
        )

    async def claim_batch(self, *, limit: int) -> list[ClaimedEvent]:
        return await self._active.claim_batch(limit=limit)

    async def mark_sent(self, claimed: ClaimedEvent, result: PublishResult) -> None:
        await self._active.mark_sent(claimed, result)
        await self._mirror_active_update(claimed.event.event_id)

    async def mark_pending_after_retryable_failure(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
        next_attempt_at: datetime,
    ) -> None:
        await self._active.mark_pending_after_retryable_failure(
            claimed,
            error_type=error_type,
            error_message=error_message,
            next_attempt_at=next_attempt_at,
        )
        await self._mirror_active_update(claimed.event.event_id)

    async def mark_failed(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
    ) -> None:
        await self._active.mark_failed(
            claimed,
            error_type=error_type,
            error_message=error_message,
        )
        await self._mirror_active_update(claimed.event.event_id)

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
    ) -> list[ClaimedEvent]:
        await self.reconcile_mirror_updates()
        await self.reconcile_prepared()
        return await self._active.failover_replay_candidates(
            failover_started_at=failover_started_at,
            limit=limit,
        )

    async def freeze_cleanup(self, *, reason: str) -> None:
        self.cleanup_frozen = True
        self.cleanup_freeze_reason = reason
        await self.primary.freeze_cleanup(reason=reason)
        await self.secondary.freeze_cleanup(reason=reason)

    async def resume_cleanup(self) -> None:
        self.cleanup_frozen = False
        self.cleanup_freeze_reason = None
        await self.primary.resume_cleanup()
        await self.secondary.resume_cleanup()

    async def cleanup_sent(self, *, now: datetime, safety_margin: timedelta) -> int:
        if self.cleanup_frozen:
            return 0
        await self._standby.cleanup_sent(now=now, safety_margin=safety_margin)
        deleted = await self._active.cleanup_sent(now=now, safety_margin=safety_margin)
        return deleted

    async def repair_failed_to_pending(self, *, event_id: str) -> AdminActionStatus:
        repaired = await self._active.repair_failed_to_pending(event_id=event_id)
        if repaired is AdminActionStatus.SUCCESS:
            await self._mirror_active_update(event_id)
        return repaired

    async def replay_event(self, *, event_id: str) -> AdminActionStatus:
        replayed = await self._active.replay_event(event_id=event_id)
        if replayed is AdminActionStatus.SUCCESS:
            await self._mirror_active_update(event_id)
        return replayed

    async def list_prepared_event_ids(self) -> tuple[str, ...]:
        await self.primary.refresh_region_records()
        await self.secondary.refresh_region_records()
        event_ids = set(self.primary.records) | set(self.secondary.records)
        return tuple(
            sorted(
                event_id
                for event_id in event_ids
                if _is_prepared(self.primary.records.get(event_id))
                or _is_prepared(self.secondary.records.get(event_id))
            )
        )

    async def reconcile_prepared(self) -> int:
        event_ids = await self.list_prepared_event_ids()
        for event_id in event_ids:
            await self.repair_prepared(event_id)
        return len(event_ids)

    async def pending_mirror_event_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._pending_mirror_event_ids))

    async def reconcile_mirror_updates(self) -> int:
        repaired = 0
        for event_id in tuple(sorted(self._pending_mirror_event_ids)):
            await self._mirror_active_update(event_id)
            repaired += 1
        return repaired

    async def repair_prepared(self, event_id: str) -> None:
        primary_record = await self.primary.load_region_record(event_id)
        secondary_record = await self.secondary.load_region_record(event_id)
        source = primary_record or secondary_record
        if source is None:
            return
        await self._prepare(self.primary, source.event)
        await self._prepare(self.secondary, source.event)
        await self._accept(self.primary, source.event)
        await self._accept(self.secondary, source.event)

    async def _prepare(self, region: BlobOutboxStore, event: OutboxEvent) -> None:
        await region.prepare_event(event)

    async def _accept(self, region: BlobOutboxStore, event: OutboxEvent) -> None:
        await region.accept_prepared_event(event)

    async def _mirror_active_update(self, event_id: str) -> None:
        last_error: Exception | None = None
        for _ in range(3):
            try:
                await self._mirror_active_update_once(event_id)
                self._pending_mirror_event_ids.discard(event_id)
                return
            except Exception as exc:
                last_error = exc
                self.metrics.increment(
                    "outbox_blob_mirror_update_failures_total",
                    active_region=self.active_region,
                    standby_region=self._standby_region_name(),
                    error_type=type(exc).__name__,
                )
        self._pending_mirror_event_ids.add(event_id)
        self.metrics.increment(
            "outbox_blob_mirror_updates_queued_total",
            active_region=self.active_region,
            standby_region=self._standby_region_name(),
        )
        _LOGGER.warning(
            "Queued dual-region blob mirror update for reconciliation",
            extra={
                "event_id": event_id,
                "active_region": self.active_region,
                "standby_region": self._standby_region_name(),
                "error_type": type(last_error).__name__ if last_error else "unknown",
            },
        )
        raise RetryableStoreError("standby mirror update failed") from last_error

    def _standby_region_name(self) -> BlobRegionName:
        return "secondary" if self.active_region == "primary" else "primary"

    async def _mirror_active_update_once(self, event_id: str) -> None:
        active_record = await self._active.load_region_record(event_id)
        if active_record is None:
            return
        standby_record = await self._standby.load_region_record(event_id)
        if standby_record is None:
            await self._standby.write_region_record(_clone_record(active_record))
            return
        standby_record.status = active_record.status
        standby_record.attempt_count = active_record.attempt_count
        standby_record.claim_token = None
        standby_record.claimed_at = None
        standby_record.next_attempt_at = active_record.next_attempt_at
        standby_record.sent_at = active_record.sent_at
        standby_record.publish_result = active_record.publish_result
        standby_record.failed_at = active_record.failed_at
        standby_record.last_error_type = active_record.last_error_type
        standby_record.last_error = active_record.last_error
        await self._standby.save_region_record(standby_record)


def blob_metadata(event: OutboxEvent, *, environment: str) -> Mapping[str, str]:
    enforce_metadata_safe(event.event_id, field_name="event_id")
    enforce_metadata_safe(event.topic, field_name="topic")
    enforce_metadata_safe(environment, field_name="environment")
    values = {
        "accepted": "true",
        "status": "PENDING",
        "event_id": event.event_id,
        "topic": event.topic,
        "environment": environment,
        "created_at_epoch_ms": str(_epoch_ms(event.created_at)),
        "expires_at_epoch_ms": str(_epoch_ms(event.expires_at)),
    }
    if event.ordering_key is not None:
        values["ordering_key_hash"] = sha256(event.ordering_key.encode()).hexdigest()
    if event.ordering_sequence is not None:
        values["ordering_sequence"] = str(event.ordering_sequence)
    return values


def _record_metadata(
    record: StoredEvent,
    *,
    environment: str,
    event_fingerprint: str,
) -> Mapping[str, str]:
    values = dict(blob_metadata(record.event, environment=environment))
    values["accepted"] = str(record.accepted).lower()
    values["status"] = record.status.value
    values["event_fingerprint"] = event_fingerprint
    return values


def _lock_metadata(*, owner_token: str, expires_at: datetime) -> Mapping[str, str]:
    return {
        "owner_token": owner_token,
        "expires_at": _encode_datetime(expires_at) or "",
    }


def _lock_is_expired(blob: BlobObject, *, now: datetime) -> bool:
    expires_at = _decode_datetime(blob.metadata.get("expires_at"))
    if expires_at is None:
        return False
    return expires_at <= now


def _cleanup_freeze_reason(blob: BlobObject) -> str:
    try:
        data = json.loads(blob.content.decode("utf-8"))
    except json.JSONDecodeError, UnicodeDecodeError:
        return "cleanup frozen"
    if not isinstance(data, dict):
        return "cleanup frozen"
    reason = data.get("reason")
    return reason if isinstance(reason, str) and reason else "cleanup frozen"


def _ordering_scope(event: OutboxEvent) -> str | None:
    return ordering_scope(event)


def _copy_blob(blob: BlobObject) -> BlobObject:
    return BlobObject(
        name=blob.name,
        content=bytes(blob.content),
        metadata=dict(blob.metadata),
        etag=blob.etag,
    )


def _claim_mutation_snapshot(
    record: StoredEvent,
    *,
    etag: str | None,
) -> _ClaimMutationSnapshot:
    return _ClaimMutationSnapshot(
        status=record.status,
        claim_token=record.claim_token,
        claimed_at=record.claimed_at,
        attempt_count=record.attempt_count,
        etag=etag,
    )


def _restore_claim_mutation(
    record: StoredEvent,
    snapshot: _ClaimMutationSnapshot,
) -> None:
    record.status = snapshot.status
    record.claim_token = snapshot.claim_token
    record.claimed_at = snapshot.claimed_at
    record.attempt_count = snapshot.attempt_count


def _is_prepared(record: StoredEvent | None) -> bool:
    return record is not None and not record.accepted


def _clone_record(record: StoredEvent) -> StoredEvent:
    return StoredEvent(
        event=record.event,
        status=record.status,
        accepted=record.accepted,
        accepted_at=record.accepted_at,
        attempt_count=record.attempt_count,
        claim_token=record.claim_token,
        claimed_at=record.claimed_at,
        next_attempt_at=record.next_attempt_at,
        sent_at=record.sent_at,
        publish_result=record.publish_result,
        failed_at=record.failed_at,
        last_error_type=record.last_error_type,
        last_error=record.last_error,
    )


def _encode_record(record: StoredEvent) -> bytes:
    return json.dumps(
        {
            "event": _encode_event(record.event),
            "status": record.status.value,
            "accepted": record.accepted,
            "accepted_at": _encode_datetime(record.accepted_at),
            "attempt_count": record.attempt_count,
            "claim_token": record.claim_token,
            "claimed_at": _encode_datetime(record.claimed_at),
            "next_attempt_at": _encode_datetime(record.next_attempt_at),
            "sent_at": _encode_datetime(record.sent_at),
            "publish_result": _encode_publish_result(record.publish_result),
            "failed_at": _encode_datetime(record.failed_at),
            "last_error_type": record.last_error_type,
            "last_error": record.last_error,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _decode_record(content: bytes) -> StoredEvent:
    data = json.loads(content.decode("utf-8"))
    if not isinstance(data, dict):
        raise RetryableStoreError("invalid blob record")
    record: Mapping[str, Any] = data
    return StoredEvent(
        event=_decode_event(_mapping_field(record, "event")),
        status=_status_field(record, "status"),
        accepted=_bool_field(record, "accepted"),
        accepted_at=_optional_datetime_field(record, "accepted_at"),
        attempt_count=_int_field(record, "attempt_count"),
        claim_token=_optional_str_field(record, "claim_token"),
        claimed_at=_optional_datetime_field(record, "claimed_at"),
        next_attempt_at=_optional_datetime_field(record, "next_attempt_at"),
        sent_at=_optional_datetime_field(record, "sent_at"),
        publish_result=_decode_publish_result(
            _optional_mapping_field(record, "publish_result")
        ),
        failed_at=_optional_datetime_field(record, "failed_at"),
        last_error_type=_optional_str_field(record, "last_error_type"),
        last_error=_optional_str_field(record, "last_error"),
    )


def _encode_event(event: OutboxEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "topic": event.topic,
        "payload": _encode_bytes(event.payload),
        "key": _encode_optional_bytes(event.key),
        "headers": {
            name: _encode_bytes(value) for name, value in sorted(event.headers.items())
        },
        "created_at": _encode_datetime(event.created_at),
        "expires_at": _encode_datetime(event.expires_at),
        "ordering_key": event.ordering_key,
        "ordering_sequence": event.ordering_sequence,
        "publishing_mode": event.publishing_mode.value,
        "schema_id": event.schema_id,
        "schema_version": event.schema_version,
    }


def _decode_event(data: Mapping[str, Any]) -> OutboxEvent:
    created_at = _required_datetime_field(data, "created_at")
    expires_at = _required_datetime_field(data, "expires_at")
    return OutboxEvent(
        event_id=_str_field(data, "event_id"),
        topic=_str_field(data, "topic"),
        payload=_decode_bytes_field(data, "payload"),
        key=_decode_optional_bytes_field(data, "key"),
        headers=_decode_headers(_mapping_field(data, "headers")),
        created_at=created_at,
        expires_at=expires_at,
        ordering_key=_optional_str_field(data, "ordering_key"),
        ordering_sequence=_optional_int_field(data, "ordering_sequence"),
        publishing_mode=_publishing_mode_field(data, "publishing_mode"),
        schema_id=_optional_str_field(data, "schema_id"),
        schema_version=_optional_str_field(data, "schema_version"),
    )


def _encode_publish_result(result: PublishResult | None) -> dict[str, Any] | None:
    if result is None:
        return None
    return {
        "partition": result.partition,
        "offset": result.offset,
        "published_at": _encode_datetime(result.published_at),
        "metadata": dict(result.metadata),
    }


def _decode_publish_result(data: Mapping[str, Any] | None) -> PublishResult | None:
    if data is None:
        return None
    published_at = _required_datetime_field(data, "published_at")
    return PublishResult(
        partition=_optional_int_field(data, "partition"),
        offset=_optional_int_field(data, "offset"),
        published_at=published_at,
        metadata=_str_mapping_field(data, "metadata"),
    )


def _event_fingerprint(event: OutboxEvent, *, key: bytes | None = None) -> str:
    encoded = json.dumps(
        _encode_event(event),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if key is not None:
        return hmac.new(key, encoded, sha256).hexdigest()
    return sha256(encoded).hexdigest()


def _encode_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _decode_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RetryableStoreError("blob record datetime value must be a string")
    try:
        decoded = datetime.fromisoformat(value)
    except ValueError as exc:
        raise RetryableStoreError("blob record datetime value is invalid") from exc
    if decoded.tzinfo is None:
        return decoded.replace(tzinfo=UTC)
    return decoded


def _required_datetime_field(data: Mapping[str, Any], field_name: str) -> datetime:
    decoded = _decode_datetime(_field(data, field_name))
    if decoded is None:
        raise RetryableStoreError(f"blob record missing required {field_name}")
    return decoded


def _optional_datetime_field(
    data: Mapping[str, Any],
    field_name: str,
) -> datetime | None:
    return _decode_datetime(_field(data, field_name))


def _encode_bytes(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _encode_optional_bytes(value: bytes | None) -> str | None:
    if value is None:
        return None
    return _encode_bytes(value)


def _decode_bytes_field(data: Mapping[str, Any], field_name: str) -> bytes:
    value = _str_field(data, field_name)
    try:
        return base64.b64decode(value.encode("ascii"), validate=True)
    except (binascii.Error, UnicodeEncodeError) as exc:
        raise RetryableStoreError(f"blob record invalid {field_name}") from exc


def _decode_optional_bytes_field(
    data: Mapping[str, Any],
    field_name: str,
) -> bytes | None:
    if _field(data, field_name) is None:
        return None
    return _decode_bytes_field(data, field_name)


def _decode_headers(data: Mapping[str, Any]) -> dict[str, bytes]:
    headers: dict[str, bytes] = {}
    for name, value in data.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise RetryableStoreError("blob record invalid headers")
        try:
            headers[name] = base64.b64decode(value.encode("ascii"), validate=True)
        except (binascii.Error, UnicodeEncodeError) as exc:
            raise RetryableStoreError("blob record invalid headers") from exc
    return headers


def _str_mapping_field(data: Mapping[str, Any], field_name: str) -> dict[str, str]:
    mapping = _mapping_field(data, field_name)
    values: dict[str, str] = {}
    for key, value in mapping.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise RetryableStoreError(f"blob record invalid {field_name}")
        values[key] = value
    return values


def _status_field(data: Mapping[str, Any], field_name: str) -> OutboxStatus:
    value = _str_field(data, field_name)
    try:
        return OutboxStatus(value)
    except ValueError as exc:
        raise RetryableStoreError(f"blob record invalid {field_name}") from exc


def _publishing_mode_field(
    data: Mapping[str, Any],
    field_name: str,
) -> PublishingMode:
    value = _str_field(data, field_name)
    try:
        return PublishingMode(value)
    except ValueError as exc:
        raise RetryableStoreError(f"blob record invalid {field_name}") from exc


def _mapping_field(data: Mapping[str, Any], field_name: str) -> Mapping[str, Any]:
    value = _field(data, field_name)
    if not isinstance(value, Mapping):
        raise RetryableStoreError(f"blob record invalid {field_name}")
    return value


def _optional_mapping_field(
    data: Mapping[str, Any],
    field_name: str,
) -> Mapping[str, Any] | None:
    value = _field(data, field_name)
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise RetryableStoreError(f"blob record invalid {field_name}")
    return value


def _str_field(data: Mapping[str, Any], field_name: str) -> str:
    value = _field(data, field_name)
    if not isinstance(value, str):
        raise RetryableStoreError(f"blob record invalid {field_name}")
    return value


def _optional_str_field(data: Mapping[str, Any], field_name: str) -> str | None:
    value = _field(data, field_name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RetryableStoreError(f"blob record invalid {field_name}")
    return value


def _bool_field(data: Mapping[str, Any], field_name: str) -> bool:
    value = _field(data, field_name)
    if not isinstance(value, bool):
        raise RetryableStoreError(f"blob record invalid {field_name}")
    return value


def _int_field(data: Mapping[str, Any], field_name: str) -> int:
    value = _field(data, field_name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise RetryableStoreError(f"blob record invalid {field_name}")
    return value


def _optional_int_field(data: Mapping[str, Any], field_name: str) -> int | None:
    value = _field(data, field_name)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise RetryableStoreError(f"blob record invalid {field_name}")
    return value


def _field(data: Mapping[str, Any], field_name: str) -> Any:
    try:
        return data[field_name]
    except KeyError as exc:
        raise RetryableStoreError(f"blob record missing required {field_name}") from exc


def _epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)
