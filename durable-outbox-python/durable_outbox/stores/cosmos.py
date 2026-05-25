from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Protocol
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
from durable_outbox.core.ordering import ordering_scope
from durable_outbox.core.time import Clock, SystemClock
from durable_outbox.core.validation import enforce_payload_size, require_positive_limit


@dataclass(frozen=True, slots=True)
class CosmosConfiguration:
    consistency: str
    regions: tuple[str, ...]
    multi_write: bool = False
    certified_mode: bool = True
    unordered_buckets: int = 16

    @property
    def is_rpo_zero(self) -> bool:
        return (
            self.consistency.lower() == "strong"
            and len(self.regions) > 1
            and not self.multi_write
        )


@dataclass(slots=True)
class CosmosStoredEvent:
    event: OutboxEvent
    partition_key: str
    version: int = 0
    status: OutboxStatus = OutboxStatus.PENDING
    accepted_at: datetime | None = None
    attempt_count: int = 0
    claim_token: str | None = None
    claimed_at: datetime | None = None
    next_attempt_at: datetime | None = None
    sent_at: datetime | None = None
    publish_result: PublishResult | None = None
    failed_at: datetime | None = None
    last_error_type: str | None = None
    last_error: str | None = None


class CosmosOutboxClient(Protocol):
    async def get(self, event_id: str) -> CosmosStoredEvent | None: ...

    async def insert(self, record: CosmosStoredEvent) -> CosmosStoredEvent: ...

    async def replace(
        self,
        record: CosmosStoredEvent,
        *,
        expected_version: int,
    ) -> CosmosStoredEvent: ...

    async def list_records(self) -> Sequence[CosmosStoredEvent]: ...

    async def delete(self, event_id: str) -> None: ...

    async def get_cleanup_freeze_reason(self) -> str | None: ...

    async def set_cleanup_freeze(self, reason: str) -> None: ...

    async def clear_cleanup_freeze(self) -> None: ...


class InMemoryCosmosOutboxClient:
    def __init__(self) -> None:
        self.records: dict[tuple[str, str], CosmosStoredEvent] = {}
        self.partition_keys_by_event_id: dict[str, str] = {}
        self.cleanup_freeze_reason: str | None = None

    async def get(self, event_id: str) -> CosmosStoredEvent | None:
        partition_key = self.partition_keys_by_event_id.get(event_id)
        if partition_key is None:
            return None
        record = self.records.get((partition_key, event_id))
        return _clone_record(record) if record is not None else None

    async def insert(self, record: CosmosStoredEvent) -> CosmosStoredEvent:
        if record.event.event_id in self.partition_keys_by_event_id:
            raise DuplicateEventConflictError("event_id already exists")
        stored = _clone_record(record, version=1)
        self.partition_keys_by_event_id[record.event.event_id] = record.partition_key
        self.records[(record.partition_key, record.event.event_id)] = stored
        return _clone_record(stored)

    async def replace(
        self,
        record: CosmosStoredEvent,
        *,
        expected_version: int,
    ) -> CosmosStoredEvent:
        current = self.records.get((record.partition_key, record.event.event_id))
        if current is None or current.version != expected_version:
            raise ClaimConflictError("record version precondition failed")
        stored = _clone_record(record, version=current.version + 1)
        self.partition_keys_by_event_id[stored.event.event_id] = stored.partition_key
        self.records[(stored.partition_key, stored.event.event_id)] = stored
        return _clone_record(stored)

    async def list_records(self) -> Sequence[CosmosStoredEvent]:
        return tuple(_clone_record(record) for record in self.records.values())

    async def delete(self, event_id: str) -> None:
        partition_key = self.partition_keys_by_event_id.pop(event_id, None)
        if partition_key is None:
            return
        self.records.pop((partition_key, event_id), None)

    async def get_cleanup_freeze_reason(self) -> str | None:
        return self.cleanup_freeze_reason

    async def set_cleanup_freeze(self, reason: str) -> None:
        self.cleanup_freeze_reason = reason

    async def clear_cleanup_freeze(self) -> None:
        self.cleanup_freeze_reason = None


class CosmosStrongOutboxStore:
    def __init__(
        self,
        config: CosmosConfiguration,
        *,
        client: CosmosOutboxClient | None = None,
        store_name: str = "CosmosStrongOutboxStore",
        claim_timeout: timedelta = timedelta(minutes=5),
        clock: Clock | None = None,
    ) -> None:
        if config.unordered_buckets < 1:
            raise ConfigurationError("Cosmos unordered_buckets must be at least 1")
        if config.certified_mode and not config.is_rpo_zero:
            raise ConfigurationError(
                "Cosmos certified RPO=0 mode requires strong consistency, "
                "more than one region, and single-write configuration"
            )
        if client is None:
            raise ConfigurationError(
                "CosmosStrongOutboxStore requires an explicit Cosmos client; "
                "use CosmosStrongOutboxStore.for_testing() for in-memory tests"
            )
        self.config = config
        self.client = client
        self.claim_timeout = claim_timeout
        self.clock = clock or SystemClock()
        self.cleanup_frozen = False
        self.cleanup_freeze_reason: str | None = None
        self.capabilities = OutboxCapabilities(
            store_name=store_name,
            rpo_zero_for_accepted_events=config.is_rpo_zero,
            supports_ordering=True,
            supports_failover_replay=True,
            supports_ttl_freeze=True,
            max_payload_bytes=2 * 1024 * 1024,
        )

    @classmethod
    def for_testing(
        cls,
        config: CosmosConfiguration,
        *,
        claim_timeout: timedelta = timedelta(minutes=5),
        clock: Clock | None = None,
    ) -> CosmosStrongOutboxStore:
        return cls(
            config,
            client=InMemoryCosmosOutboxClient(),
            store_name="InMemoryCosmosStrongOutboxStore",
            claim_timeout=claim_timeout,
            clock=clock,
        )

    async def put(self, event: OutboxEvent) -> AcceptedReceipt:
        enforce_payload_size(event, self.capabilities)
        partition_key = self.partition_key_for(event)
        now = self.clock.utcnow()
        record = await self.client.get(event.event_id)
        if record is None:
            record = CosmosStoredEvent(
                event=event,
                partition_key=partition_key,
                accepted_at=now,
            )
            record = await self.client.insert(record)
        elif record.partition_key != partition_key or record.event != event:
            raise DuplicateEventConflictError(
                "event_id already exists with incompatible content"
            )
        return AcceptedReceipt(
            event_id=event.event_id,
            accepted_at=record.accepted_at or now,
            rpo_zero=self.capabilities.rpo_zero_for_accepted_events,
            store=self.capabilities.store_name,
            durability_witness=tuple(
                f"cosmos:{region}" for region in self.config.regions
            ),
        )

    async def claim_batch(self, *, limit: int) -> list[ClaimedEvent]:
        return await self._claim_from_candidates(
            await self._claim_ordered_records(),
            limit=limit,
        )

    async def _claim_from_candidates(
        self,
        records: Sequence[CosmosStoredEvent],
        *,
        limit: int,
    ) -> list[ClaimedEvent]:
        require_positive_limit(limit)
        now = self.clock.utcnow()
        claimed: list[ClaimedEvent] = []
        locked_keys = await self._in_flight_ordering_keys(now)
        for record in records:
            if len(claimed) >= limit:
                break
            if not self._eligible_for_claim(record, now):
                continue
            scoped_key = ordering_scope(record.event)
            if scoped_key is not None and scoped_key in locked_keys:
                continue
            token = str(uuid4())
            record.status = OutboxStatus.IN_FLIGHT
            record.claim_token = token
            record.claimed_at = now
            record.attempt_count += 1
            try:
                record = await self.client.replace(
                    record,
                    expected_version=record.version,
                )
            except ClaimConflictError:
                continue
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
        updated = await self._cas_update(
            claimed.event.event_id,
            lambda current: _mark_sent_if_claimed(current, claimed, result),
        )
        if not updated:
            raise ClaimConflictError("claimed event no longer exists")

    async def mark_pending_after_retryable_failure(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
        next_attempt_at: datetime,
    ) -> None:
        updated = await self._cas_update(
            claimed.event.event_id,
            lambda current: _mark_pending_if_claimed(
                current,
                claimed,
                error_type=error_type,
                error_message=error_message,
                next_attempt_at=next_attempt_at,
            ),
        )
        if not updated:
            raise ClaimConflictError("claimed event no longer exists")

    async def mark_failed(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
    ) -> None:
        failed_at = self.clock.utcnow()
        updated = await self._cas_update(
            claimed.event.event_id,
            lambda current: _mark_failed_if_claimed(
                current,
                claimed,
                failed_at=failed_at,
                error_type=error_type,
                error_message=error_message,
            ),
        )
        if not updated:
            raise ClaimConflictError("claimed event no longer exists")

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
    ) -> list[ClaimedEvent]:
        require_positive_limit(limit)
        candidates: list[ClaimedEvent] = []
        records = sorted(
            await self.client.list_records(), key=lambda item: item.event.created_at
        )
        for record in records:
            if len(candidates) >= limit:
                break
            if record.status not in {
                OutboxStatus.PENDING,
                OutboxStatus.IN_FLIGHT,
                OutboxStatus.SENT,
            }:
                continue
            if record.event.expires_at < failover_started_at:
                continue
            token = str(uuid4())
            source_status = record.status
            record.status = OutboxStatus.IN_FLIGHT
            record.claim_token = token
            record.claimed_at = self.clock.utcnow()
            record.attempt_count += 1
            try:
                record = await self.client.replace(
                    record,
                    expected_version=record.version,
                )
            except ClaimConflictError:
                continue
            candidates.append(
                ClaimedEvent(
                    event=record.event,
                    claim_token=token,
                    attempt_count=record.attempt_count,
                    source_status=source_status,
                )
            )
        return candidates

    async def freeze_cleanup(self, *, reason: str) -> None:
        await self.client.set_cleanup_freeze(reason)
        self.cleanup_frozen = True
        self.cleanup_freeze_reason = reason

    async def resume_cleanup(self) -> None:
        await self.client.clear_cleanup_freeze()
        self.cleanup_frozen = False
        self.cleanup_freeze_reason = None

    async def cleanup_sent(self, *, now: datetime, safety_margin: timedelta) -> int:
        if await self._cleanup_is_frozen():
            return 0
        event_ids = [
            record.event.event_id
            for record in await self.client.list_records()
            if record.status is OutboxStatus.SENT
            and now > record.event.expires_at + safety_margin
        ]
        for event_id in event_ids:
            await self.client.delete(event_id)
        return len(event_ids)

    async def repair_failed_to_pending(self, *, event_id: str) -> bool:
        return await self._cas_update(event_id, _repair_failed_record)

    async def replay_event(self, *, event_id: str) -> bool:
        return await self._cas_update(event_id, _replay_record)

    async def _cleanup_is_frozen(self) -> bool:
        self.cleanup_freeze_reason = await self.client.get_cleanup_freeze_reason()
        self.cleanup_frozen = self.cleanup_freeze_reason is not None
        return self.cleanup_frozen

    def partition_key_for(self, event: OutboxEvent) -> str:
        if event.publishing_mode is PublishingMode.ORDERED and event.ordering_key:
            return f"{event.topic}#{_hash(event.ordering_key)}"
        bucket = int(_hash(event.event_id), 16) % self.config.unordered_buckets
        return f"{event.topic}#{bucket}"

    async def _claimed_record(self, claimed: ClaimedEvent) -> CosmosStoredEvent:
        record = await self.client.get(claimed.event.event_id)
        if record is None or record.claim_token != claimed.claim_token:
            raise ClaimConflictError("claim token does not match current owner")
        return record

    async def _cas_update(
        self,
        event_id: str,
        mutate: Callable[[CosmosStoredEvent], bool],
        *,
        attempts: int = 3,
    ) -> bool:
        for _ in range(attempts):
            record = await self.client.get(event_id)
            if record is None:
                return False
            if not mutate(record):
                return False
            try:
                await self.client.replace(record, expected_version=record.version)
                return True
            except ClaimConflictError:
                continue
        raise RetryableStoreError("record update lost too many version races")

    async def _claim_ordered_records(self) -> list[CosmosStoredEvent]:
        return sorted(
            await self.client.list_records(),
            key=lambda record: (
                record.event.topic,
                record.event.ordering_key or "",
                record.event.ordering_sequence or 0,
                record.event.created_at,
            ),
        )

    def _eligible_for_claim(self, record: CosmosStoredEvent, now: datetime) -> bool:
        if record.status is OutboxStatus.PENDING:
            return record.next_attempt_at is None or record.next_attempt_at <= now
        if record.status is OutboxStatus.IN_FLIGHT and record.claimed_at is not None:
            return record.claimed_at + self.claim_timeout <= now
        return False

    async def _in_flight_ordering_keys(self, now: datetime) -> set[str]:
        locked: set[str] = set()
        for record in await self.client.list_records():
            key = ordering_scope(record.event)
            if key is None:
                continue
            if record.status is not OutboxStatus.IN_FLIGHT:
                continue
            if (
                record.claimed_at is None
                or record.claimed_at + self.claim_timeout <= now
            ):
                continue
            locked.add(key)
        return locked


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _mark_sent_if_claimed(
    record: CosmosStoredEvent,
    claimed: ClaimedEvent,
    result: PublishResult,
) -> bool:
    _require_claim(record, claimed)
    record.status = OutboxStatus.SENT
    record.sent_at = result.published_at
    record.publish_result = result
    record.claim_token = None
    record.claimed_at = None
    return True


def _mark_pending_if_claimed(
    record: CosmosStoredEvent,
    claimed: ClaimedEvent,
    *,
    error_type: str,
    error_message: str,
    next_attempt_at: datetime,
) -> bool:
    _require_claim(record, claimed)
    record.status = OutboxStatus.PENDING
    record.next_attempt_at = next_attempt_at
    record.last_error_type = error_type
    record.last_error = error_message
    record.claim_token = None
    record.claimed_at = None
    return True


def _mark_failed_if_claimed(
    record: CosmosStoredEvent,
    claimed: ClaimedEvent,
    *,
    failed_at: datetime,
    error_type: str,
    error_message: str,
) -> bool:
    _require_claim(record, claimed)
    record.status = OutboxStatus.FAILED
    record.failed_at = failed_at
    record.last_error_type = error_type
    record.last_error = error_message
    record.claim_token = None
    record.claimed_at = None
    return True


def _repair_failed_record(record: CosmosStoredEvent) -> bool:
    if record.status is not OutboxStatus.FAILED:
        return False
    record.status = OutboxStatus.PENDING
    record.failed_at = None
    record.attempt_count = 0
    record.last_error_type = None
    record.last_error = None
    record.next_attempt_at = None
    record.claim_token = None
    record.claimed_at = None
    return True


def _replay_record(record: CosmosStoredEvent) -> bool:
    record.status = OutboxStatus.PENDING
    record.claim_token = None
    record.claimed_at = None
    record.next_attempt_at = None
    record.sent_at = None
    record.publish_result = None
    record.failed_at = None
    record.last_error_type = None
    record.last_error = None
    return True


def _require_claim(record: CosmosStoredEvent, claimed: ClaimedEvent) -> None:
    if record.claim_token != claimed.claim_token:
        raise ClaimConflictError("claim token does not match current owner")


def _clone_record(
    record: CosmosStoredEvent,
    *,
    version: int | None = None,
) -> CosmosStoredEvent:
    return CosmosStoredEvent(
        event=record.event,
        partition_key=record.partition_key,
        version=record.version if version is None else version,
        status=record.status,
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
