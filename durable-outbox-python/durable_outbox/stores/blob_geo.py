import base64
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any, Literal, Protocol
from uuid import uuid4

from durable_outbox.core.capabilities import OutboxCapabilities
from durable_outbox.core.errors import (
    ClaimConflictError,
    ConfigurationError,
    DuplicateEventConflictError,
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
    InMemoryOrderingLockBackend,
    OrderingLockBackend,
    OrderingLockLease,
    ordering_scope,
)
from durable_outbox.core.time import Clock, SystemClock
from durable_outbox.core.validation import enforce_payload_size, require_positive_limit
from durable_outbox.stores.memory import StoredEvent

type BlobRegionName = Literal["primary", "secondary"]


def event_blob_name(event_id: str) -> str:
    safe_id = sha256(event_id.encode("utf-8")).hexdigest()
    return f"outbox/v1/events/{safe_id}.json"


def ordering_lock_blob_name(environment: str, topic: str, ordering_key: str) -> str:
    topic_hash = sha256(topic.encode("utf-8")).hexdigest()
    key_hash = sha256(ordering_key.encode("utf-8")).hexdigest()
    return f"outbox/v1/key-locks/{environment}/{topic_hash}/{key_hash}.lock"


@dataclass(frozen=True, slots=True)
class BlobObject:
    name: str
    content: bytes
    metadata: Mapping[str, str]
    etag: str


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


class BlobOutboxStore:
    capabilities = OutboxCapabilities(
        store_name="BlobOutboxStore",
        rpo_zero_for_accepted_events=False,
        supports_ordering=True,
        supports_failover_replay=True,
        supports_ttl_freeze=True,
        notes=("GRS/RA-GRS alone is not sufficient for RPO=0.",),
    )

    def __init__(
        self,
        *,
        client: BlobClientProtocol | None = None,
        environment: str = "default",
        store_name: str = "BlobOutboxStore",
        ordering_lock_backend: OrderingLockBackend | None = None,
        ordering_lock_lease_duration: timedelta = timedelta(minutes=5),
        claim_timeout: timedelta = timedelta(minutes=5),
        clock: Clock | None = None,
    ) -> None:
        if client is None:
            raise ConfigurationError(
                "BlobOutboxStore requires an explicit blob client; "
                "use BlobOutboxStore.for_testing() for in-memory tests"
            )
        self.client = client
        self.environment = environment
        self.ordering_lock_backend = (
            ordering_lock_backend or InMemoryOrderingLockBackend()
        )
        self.ordering_lock_lease_duration = ordering_lock_lease_duration
        self.claim_timeout = claim_timeout
        self.clock = clock or SystemClock()
        self.cleanup_frozen = False
        self.cleanup_freeze_reason: str | None = None
        self.records: dict[str, StoredEvent] = {}
        self._record_etags: dict[str, str] = {}
        self._ordering_leases_by_event_id: dict[str, OrderingLockLease] = {}
        self.capabilities = OutboxCapabilities(
            store_name=store_name,
            rpo_zero_for_accepted_events=False,
            supports_ordering=True,
            supports_failover_replay=True,
            supports_ttl_freeze=True,
            notes=("GRS/RA-GRS alone is not sufficient for RPO=0.",),
        )

    @classmethod
    def for_testing(
        cls,
        *,
        environment: str = "test",
        ordering_lock_backend: OrderingLockBackend | None = None,
        ordering_lock_lease_duration: timedelta = timedelta(minutes=5),
        claim_timeout: timedelta = timedelta(minutes=5),
        clock: Clock | None = None,
    ) -> BlobOutboxStore:
        return cls(
            client=InMemoryBlobClient(),
            environment=environment,
            store_name="InMemoryBlobOutboxStore",
            ordering_lock_backend=ordering_lock_backend,
            ordering_lock_lease_duration=ordering_lock_lease_duration,
            claim_timeout=claim_timeout,
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
            previous = _clone_record(record)
            token = str(uuid4())
            record.status = OutboxStatus.IN_FLIGHT
            record.claim_token = token
            record.claimed_at = now
            record.attempt_count += 1
            try:
                await self._save_record(record)
            except BlobPreconditionFailedError:
                self.records[record.event.event_id] = previous
                if lease is not None:
                    await self.ordering_lock_backend.release(lease)
                continue
            if lease is not None:
                self._ordering_leases_by_event_id[record.event.event_id] = lease
            if scoped_key is not None:
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
            token = str(uuid4())
            record.status = OutboxStatus.IN_FLIGHT
            record.claim_token = token
            record.claimed_at = self.clock.utcnow()
            record.attempt_count += 1
            try:
                await self._save_record(record)
            except BlobPreconditionFailedError:
                continue
            candidates.append(
                ClaimedEvent(
                    event=record.event,
                    claim_token=token,
                    attempt_count=record.attempt_count,
                )
            )
        return candidates

    async def freeze_cleanup(self, *, reason: str) -> None:
        self.cleanup_frozen = True
        self.cleanup_freeze_reason = reason

    async def resume_cleanup(self) -> None:
        self.cleanup_frozen = False
        self.cleanup_freeze_reason = None

    async def cleanup_sent(self, *, now: datetime, safety_margin: timedelta) -> int:
        if self.cleanup_frozen:
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
        return deleted

    async def repair_failed_to_pending(self, *, event_id: str) -> bool:
        record = await self._load_record(event_id)
        if record is None or record.status is not OutboxStatus.FAILED:
            return False
        record.status = OutboxStatus.PENDING
        record.failed_at = None
        record.attempt_count = 0
        record.last_error_type = None
        record.last_error = None
        record.next_attempt_at = None
        record.claim_token = None
        record.claimed_at = None
        await self._save_record(record)
        return True

    async def replay_event(self, *, event_id: str) -> bool:
        record = await self._load_record(event_id)
        if record is None:
            return False
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
        await self._release_ordering_lease(event_id)
        return True

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
        record.accepted = True
        record.accepted_at = self.clock.utcnow()
        await self._save_record(record)

    async def _load_record(self, event_id: str) -> StoredEvent | None:
        blob = await self.client.get_blob(event_blob_name(event_id))
        if blob is None:
            return None
        record = _decode_record(blob.content)
        expected_fingerprint = blob.metadata.get("event_fingerprint")
        if expected_fingerprint != _event_fingerprint(record.event):
            raise RetryableStoreError("blob record fingerprint mismatch")
        self.records[event_id] = record
        self._record_etags[event_id] = blob.etag
        return record

    async def _refresh_records(self) -> None:
        blobs = await self.client.list_blobs(prefix="outbox/v1/events/")
        for blob in blobs:
            record = _decode_record(blob.content)
            expected_fingerprint = blob.metadata.get("event_fingerprint")
            if expected_fingerprint != _event_fingerprint(record.event):
                raise RetryableStoreError("blob record fingerprint mismatch")
            event_id = record.event.event_id
            self.records[event_id] = record
            self._record_etags[event_id] = blob.etag

    async def _write_new_record(self, record: StoredEvent) -> None:
        blob = await self.client.put_blob(
            event_blob_name(record.event.event_id),
            _encode_record(record),
            _record_metadata(record, environment=self.environment),
            if_none_match=True,
        )
        self.records[record.event.event_id] = record
        self._record_etags[record.event.event_id] = blob.etag

    async def _save_record(self, record: StoredEvent) -> None:
        event_id = record.event.event_id
        blob = await self.client.put_blob(
            event_blob_name(event_id),
            _encode_record(record),
            _record_metadata(record, environment=self.environment),
            if_match=self._record_etags.get(event_id),
        )
        self.records[event_id] = record
        self._record_etags[event_id] = blob.etag

    async def _claimed_record(self, claimed: ClaimedEvent) -> StoredEvent:
        record = self.records.get(claimed.event.event_id)
        if record is None:
            loaded = await self._load_record(claimed.event.event_id)
            if loaded is None:
                raise ClaimConflictError("claimed event no longer exists")
            record = loaded
        if record.claim_token != claimed.claim_token:
            raise ClaimConflictError("claim token does not match current owner")
        return record

    def _ensure_compatible_duplicate(
        self, record: StoredEvent, event: OutboxEvent
    ) -> None:
        if _event_fingerprint(record.event) != _event_fingerprint(event):
            raise DuplicateEventConflictError(
                "event_id already exists with incompatible content"
            )

    def _ordered_records(self) -> Iterable[StoredEvent]:
        return sorted(
            self.records.values(),
            key=lambda item: (
                item.event.topic,
                item.event.ordering_key or "",
                item.event.ordering_sequence or 0,
                item.event.created_at,
            ),
        )

    def _eligible_for_claim(self, record: StoredEvent, now: datetime) -> bool:
        if not record.accepted:
            return False
        if record.status is OutboxStatus.PENDING:
            return record.next_attempt_at is None or record.next_attempt_at <= now
        if record.status is OutboxStatus.IN_FLIGHT and record.claimed_at is not None:
            return record.claimed_at + self.claim_timeout <= now
        return False

    def _in_flight_ordering_keys(self, now: datetime) -> set[str]:
        locked: set[str] = set()
        for record in self.records.values():
            key = record.event.effective_ordering_key
            if key is None or record.status is not OutboxStatus.IN_FLIGHT:
                continue
            if (
                record.claimed_at is None
                or record.claimed_at + self.claim_timeout <= now
            ):
                continue
            scoped_key = _ordering_scope(record.event)
            if scoped_key is not None:
                locked.add(scoped_key)
        return locked

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
            clock=self.clock,
        )
        self.secondary = BlobOutboxStore(
            client=secondary_client,
            environment=f"{environment}-secondary",
            claim_timeout=claim_timeout,
            clock=self.clock,
        )
        self.active_region: BlobRegionName = active_region
        self.records = self._active.records
        self.cleanup_frozen = False
        self.cleanup_freeze_reason: str | None = None
        self.capabilities = OutboxCapabilities(
            store_name=store_name,
            rpo_zero_for_accepted_events=True,
            supports_ordering=True,
            supports_failover_replay=True,
            supports_ttl_freeze=True,
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
        clock: Clock | None = None,
    ) -> DualRegionBlobOutboxStore:
        return cls(
            primary_client=InMemoryBlobClient(),
            secondary_client=InMemoryBlobClient(),
            environment=environment,
            store_name="InMemoryDualRegionBlobOutboxStore",
            active_region=active_region,
            claim_timeout=claim_timeout,
            clock=clock,
        )

    @property
    def _active(self) -> BlobOutboxStore:
        return self.primary if self.active_region == "primary" else self.secondary

    @property
    def _standby(self) -> BlobOutboxStore:
        return self.secondary if self.active_region == "primary" else self.primary

    def use_region(self, region: BlobRegionName) -> None:
        if region not in ("primary", "secondary"):
            msg = "region must be 'primary' or 'secondary'"
            raise ValueError(msg)
        self.active_region = region
        self.records = self._active.records

    def promote_secondary(self) -> None:
        self.use_region("secondary")

    def promote_primary(self) -> None:
        self.use_region("primary")

    async def put(self, event: OutboxEvent) -> AcceptedReceipt:
        await self._prepare(self.primary, event)
        await self._prepare(self.secondary, event)
        await self._accept(self.primary, event)
        await self._accept(self.secondary, event)
        self.records = self._active.records
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
        deleted = await self._active.cleanup_sent(now=now, safety_margin=safety_margin)
        await self._standby.cleanup_sent(now=now, safety_margin=safety_margin)
        return deleted

    async def repair_failed_to_pending(self, *, event_id: str) -> bool:
        repaired = await self._active.repair_failed_to_pending(event_id=event_id)
        if repaired:
            await self._mirror_active_update(event_id)
        return repaired

    async def replay_event(self, *, event_id: str) -> bool:
        replayed = await self._active.replay_event(event_id=event_id)
        if replayed:
            await self._mirror_active_update(event_id)
        return replayed

    async def list_prepared_event_ids(self) -> tuple[str, ...]:
        await self.primary._refresh_records()
        await self.secondary._refresh_records()
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

    async def repair_prepared(self, event_id: str) -> None:
        primary_record = await self.primary._load_record(event_id)
        secondary_record = await self.secondary._load_record(event_id)
        source = primary_record or secondary_record
        if source is None:
            return
        await self._prepare(self.primary, source.event)
        await self._prepare(self.secondary, source.event)
        await self._accept(self.primary, source.event)
        await self._accept(self.secondary, source.event)
        self.records = self._active.records

    async def _prepare(self, region: BlobOutboxStore, event: OutboxEvent) -> None:
        await region._put_prepared(event)

    async def _accept(self, region: BlobOutboxStore, event: OutboxEvent) -> None:
        await region._accept_prepared(event)

    async def _mirror_active_update(self, event_id: str) -> None:
        active_record = await self._active._load_record(event_id)
        if active_record is None:
            return
        standby_record = await self._standby._load_record(event_id)
        if standby_record is None:
            await self._standby._write_new_record(_clone_record(active_record))
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
        await self._standby._save_record(standby_record)


def blob_metadata(event: OutboxEvent, *, environment: str) -> Mapping[str, str]:
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


def _record_metadata(record: StoredEvent, *, environment: str) -> Mapping[str, str]:
    values = dict(blob_metadata(record.event, environment=environment))
    values["accepted"] = str(record.accepted).lower()
    values["status"] = record.status.value
    values["event_fingerprint"] = _event_fingerprint(record.event)
    return values


def _ordering_scope(event: OutboxEvent) -> str | None:
    return ordering_scope(event)


def _copy_blob(blob: BlobObject) -> BlobObject:
    return BlobObject(
        name=blob.name,
        content=bytes(blob.content),
        metadata=dict(blob.metadata),
        etag=blob.etag,
    )


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
    return StoredEvent(
        event=_decode_event(data["event"]),
        status=OutboxStatus(data["status"]),
        accepted=bool(data["accepted"]),
        accepted_at=_decode_datetime(data["accepted_at"]),
        attempt_count=int(data["attempt_count"]),
        claim_token=data["claim_token"],
        claimed_at=_decode_datetime(data["claimed_at"]),
        next_attempt_at=_decode_datetime(data["next_attempt_at"]),
        sent_at=_decode_datetime(data["sent_at"]),
        publish_result=_decode_publish_result(data["publish_result"]),
        failed_at=_decode_datetime(data["failed_at"]),
        last_error_type=data["last_error_type"],
        last_error=data["last_error"],
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
    created_at = _require_datetime(data["created_at"], field_name="created_at")
    expires_at = _require_datetime(data["expires_at"], field_name="expires_at")
    return OutboxEvent(
        event_id=str(data["event_id"]),
        topic=str(data["topic"]),
        payload=_decode_bytes(str(data["payload"])),
        key=_decode_optional_bytes(data["key"]),
        headers={
            str(name): _decode_bytes(str(value))
            for name, value in dict(data["headers"]).items()
        },
        created_at=created_at,
        expires_at=expires_at,
        ordering_key=data["ordering_key"],
        ordering_sequence=data["ordering_sequence"],
        publishing_mode=PublishingMode(data["publishing_mode"]),
        schema_id=data["schema_id"],
        schema_version=data["schema_version"],
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
    published_at = _require_datetime(data["published_at"], field_name="published_at")
    return PublishResult(
        partition=data["partition"],
        offset=data["offset"],
        published_at=published_at,
        metadata={
            str(key): str(value) for key, value in dict(data["metadata"]).items()
        },
    )


def _event_fingerprint(event: OutboxEvent) -> str:
    encoded = json.dumps(
        _encode_event(event),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _encode_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _decode_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    decoded = datetime.fromisoformat(str(value))
    if decoded.tzinfo is None:
        return decoded.replace(tzinfo=UTC)
    return decoded


def _require_datetime(value: Any, *, field_name: str) -> datetime:
    decoded = _decode_datetime(value)
    if decoded is None:
        raise RetryableStoreError(f"blob record missing required {field_name}")
    return decoded


def _encode_bytes(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _encode_optional_bytes(value: bytes | None) -> str | None:
    if value is None:
        return None
    return _encode_bytes(value)


def _decode_bytes(value: str) -> bytes:
    return base64.b64decode(value.encode("ascii"))


def _decode_optional_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    return _decode_bytes(str(value))


def _epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)
