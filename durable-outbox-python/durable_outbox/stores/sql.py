from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Protocol
from uuid import uuid4

from durable_outbox.core.admin import AdminActionStatus
from durable_outbox.core.capabilities import OutboxCapabilities
from durable_outbox.core.claim import (
    claim_order_key,
    in_flight_ordering_keys,
    is_claimable_record,
)
from durable_outbox.core.claim_token import claim_token_matches
from durable_outbox.core.duplicates import raise_if_incompatible_duplicate
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
    PublishResult,
)
from durable_outbox.core.ordering import ordering_scope
from durable_outbox.core.time import Clock, SystemClock
from durable_outbox.core.validation import (
    enforce_payload_size,
    require_optional_positive_limit,
    require_positive_limit,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Sequence

SQL_TABLE_NAME = "durable_outbox_events"
SQL_PENDING_INDEX_NAME = "IX_outbox_pending"
SQL_REPLAY_INDEX_NAME = "IX_outbox_replay"
SQL_ORDERED_INDEX_NAME = "IX_outbox_ordered"

SQL_SCHEMA = f"""
CREATE TABLE {SQL_TABLE_NAME} (
    event_id            NVARCHAR(128) NOT NULL PRIMARY KEY,
    status              NVARCHAR(32)  NOT NULL,
    topic               NVARCHAR(256) NOT NULL,
    kafka_key           VARBINARY(900) NULL,
    headers_json        NVARCHAR(MAX) NULL,
    payload             VARBINARY(MAX) NOT NULL,
    schema_id           NVARCHAR(128) NULL,
    schema_version      NVARCHAR(64) NULL,
    publishing_mode      NVARCHAR(32)  NOT NULL DEFAULT 'UNORDERED',
    ordering_key         NVARCHAR(1024) NULL,
    ordering_key_hash   NVARCHAR(128) NULL,
    ordering_sequence   BIGINT NULL,
    created_at_utc      DATETIME2 NOT NULL,
    expires_at_utc      DATETIME2 NOT NULL,
    accepted_at_utc     DATETIME2 NULL,
    next_attempt_utc    DATETIME2 NULL,
    attempt_count       INT NOT NULL DEFAULT 0,
    claimed_by          NVARCHAR(256) NULL,
    claim_id            UNIQUEIDENTIFIER NULL,
    claimed_at_utc      DATETIME2 NULL,
    sent_at_utc         DATETIME2 NULL,
    published_at_utc     DATETIME2 NULL,
    publish_metadata_json NVARCHAR(MAX) NULL,
    kafka_partition     INT NULL,
    kafka_offset        BIGINT NULL,
    failed_at_utc       DATETIME2 NULL,
    last_error_type     NVARCHAR(256) NULL,
    last_error          NVARCHAR(2048) NULL,
    row_version         ROWVERSION NOT NULL
);

CREATE INDEX {SQL_PENDING_INDEX_NAME}
ON {SQL_TABLE_NAME}(status, next_attempt_utc, created_at_utc);

CREATE INDEX {SQL_REPLAY_INDEX_NAME}
ON {SQL_TABLE_NAME}(expires_at_utc, status);

CREATE INDEX {SQL_ORDERED_INDEX_NAME}
ON {SQL_TABLE_NAME}(topic, ordering_key_hash, ordering_sequence);
"""


@dataclass(frozen=True, slots=True)
class AzureSqlSyncConfiguration:
    sync_wait_succeeds: bool = True


@dataclass(slots=True)
class SqlStoredEvent:
    event: OutboxEvent
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


class SqlOutboxClient(Protocol):
    async def get(self, event_id: str) -> SqlStoredEvent | None: ...

    async def upsert_new(self, record: SqlStoredEvent) -> SqlStoredEvent: ...

    async def replace(
        self,
        record: SqlStoredEvent,
        *,
        expected_version: int,
    ) -> SqlStoredEvent: ...

    async def list_records(self) -> Sequence[SqlStoredEvent]: ...

    async def claim_batch_pending(
        self,
        *,
        limit: int,
        now: datetime,
        claim_timeout: timedelta,
    ) -> Sequence[SqlStoredEvent]: ...

    async def list_failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> Sequence[SqlStoredEvent]: ...

    async def list_cleanup_candidates(
        self,
        *,
        now: datetime,
        safety_margin: timedelta,
        limit: int | None = None,
    ) -> Sequence[SqlStoredEvent]: ...

    async def delete(self, event_id: str) -> None: ...

    async def wait_for_database_copy_sync(self) -> None: ...

    async def synchronized_secondary_count(self) -> int: ...

    async def get_cleanup_freeze_reason(self) -> str | None: ...

    async def set_cleanup_freeze(self, reason: str) -> None: ...

    async def clear_cleanup_freeze(self) -> None: ...


class InMemorySqlOutboxClient:
    def __init__(
        self,
        *,
        sync_wait_succeeds: bool = True,
        synchronized_secondaries: int = 1,
    ) -> None:
        self.records: dict[str, SqlStoredEvent] = {}
        self.sync_wait_succeeds = sync_wait_succeeds
        self.synchronized_secondaries = synchronized_secondaries
        self.sync_wait_count = 0
        self.cleanup_freeze_reason: str | None = None

    async def get(self, event_id: str) -> SqlStoredEvent | None:
        record = self.records.get(event_id)
        return _clone_record(record) if record is not None else None

    async def upsert_new(self, record: SqlStoredEvent) -> SqlStoredEvent:
        if record.event.event_id in self.records:
            raise DuplicateEventConflictError("event_id already exists")
        stored = _clone_record(record, version=1)
        self.records[record.event.event_id] = stored
        return _clone_record(stored)

    async def replace(
        self,
        record: SqlStoredEvent,
        *,
        expected_version: int,
    ) -> SqlStoredEvent:
        current = self.records.get(record.event.event_id)
        if current is None or current.version != expected_version:
            raise ClaimConflictError("record version precondition failed")
        stored = _clone_record(record, version=current.version + 1)
        self.records[record.event.event_id] = stored
        return _clone_record(stored)

    async def list_records(self) -> Sequence[SqlStoredEvent]:
        return tuple(_clone_record(record) for record in self.records.values())

    async def claim_batch_pending(
        self,
        *,
        limit: int,
        now: datetime,
        claim_timeout: timedelta,
    ) -> Sequence[SqlStoredEvent]:
        require_positive_limit(limit)
        records: list[SqlStoredEvent] = []
        claimable_seen = 0
        for record in sorted(self.records.values(), key=claim_order_key):
            clone = _clone_record(record)
            if is_claimable_record(clone, now=now, claim_timeout=claim_timeout):
                records.append(clone)
                claimable_seen += 1
            elif _fresh_ordering_blocker(clone, now=now, claim_timeout=claim_timeout):
                records.append(clone)
            if claimable_seen >= limit:
                break
        return tuple(records)

    async def list_failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> Sequence[SqlStoredEvent]:
        require_positive_limit(limit)
        records: list[SqlStoredEvent] = []
        locked_ordering_scopes: set[str] = set()
        for record in sorted(
            self.records.values(), key=lambda item: item.event.created_at
        ):
            if len(records) >= limit:
                break
            if record.event.event_id in exclude_event_ids:
                continue
            if record.status not in {
                OutboxStatus.PENDING,
                OutboxStatus.IN_FLIGHT,
                OutboxStatus.SENT,
            }:
                continue
            if record.event.expires_at < failover_started_at:
                continue
            scoped_key = ordering_scope(record.event)
            if scoped_key is not None and scoped_key in locked_ordering_scopes:
                continue
            records.append(_clone_record(record))
            if scoped_key is not None:
                locked_ordering_scopes.add(scoped_key)
        return tuple(records)

    async def list_cleanup_candidates(
        self,
        *,
        now: datetime,
        safety_margin: timedelta,
        limit: int | None = None,
    ) -> Sequence[SqlStoredEvent]:
        records = [
            _clone_record(record)
            for record in self.records.values()
            if record.status is OutboxStatus.SENT
            and now > record.event.expires_at + safety_margin
        ]
        if limit is not None:
            records = records[:limit]
        return tuple(records)

    async def delete(self, event_id: str) -> None:
        self.records.pop(event_id, None)

    async def wait_for_database_copy_sync(self) -> None:
        self.sync_wait_count += 1
        if not self.sync_wait_succeeds:
            raise RetryableStoreError("sp_wait_for_database_copy_sync timed out")

    async def synchronized_secondary_count(self) -> int:
        return self.synchronized_secondaries

    async def get_cleanup_freeze_reason(self) -> str | None:
        return self.cleanup_freeze_reason

    async def set_cleanup_freeze(self, reason: str) -> None:
        self.cleanup_freeze_reason = reason

    async def clear_cleanup_freeze(self) -> None:
        self.cleanup_freeze_reason = None


class _SqlOutboxStoreBase:
    capabilities: OutboxCapabilities
    _durability_witness: tuple[str, ...]

    def __init__(
        self,
        *,
        client: SqlOutboxClient | None = None,
        claim_timeout: timedelta = timedelta(minutes=5),
        clock: Clock | None = None,
    ) -> None:
        if client is None:
            raise ConfigurationError(
                "SQL outbox stores require an explicit SQL client; "
                "use the adapter for_testing() factory for in-memory tests"
            )
        self.client = client
        self.claim_timeout = claim_timeout
        self.clock = clock or SystemClock()
        self.cleanup_frozen = False
        self.cleanup_freeze_reason: str | None = None

    async def put(self, event: OutboxEvent) -> AcceptedReceipt:
        enforce_payload_size(event, self.capabilities)
        now = self.clock.utcnow()
        record = await self.client.get(event.event_id)
        if record is None:
            record = SqlStoredEvent(event=event, accepted_at=now)
            record = await self.client.upsert_new(record)
        else:
            raise_if_incompatible_duplicate(record.event, event)
        await self._after_put_acceptance_boundary()
        return AcceptedReceipt(
            event_id=event.event_id,
            accepted_at=record.accepted_at or now,
            rpo_zero=self.capabilities.rpo_zero_for_accepted_events,
            store=self.capabilities.store_name,
            durability_witness=self._durability_witness,
        )

    async def claim_batch(self, *, limit: int) -> list[ClaimedEvent]:
        return await self._claim_from_candidates(
            await self.client.claim_batch_pending(
                limit=limit,
                now=self.clock.utcnow(),
                claim_timeout=self.claim_timeout,
            ),
            limit=limit,
        )

    async def _claim_ordered_records(self) -> list[SqlStoredEvent]:
        return list(
            await self.client.claim_batch_pending(
                limit=1000,
                now=self.clock.utcnow(),
                claim_timeout=self.claim_timeout,
            )
        )

    async def _claim_from_candidates(
        self,
        records: Sequence[SqlStoredEvent],
        *,
        limit: int,
    ) -> list[ClaimedEvent]:
        require_positive_limit(limit)
        now = self.clock.utcnow()
        claimed: list[ClaimedEvent] = []
        locked_keys = in_flight_ordering_keys(
            records,
            now=now,
            claim_timeout=self.claim_timeout,
        )
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
        exclude_event_ids: Collection[str] = (),
    ) -> list[ClaimedEvent]:
        require_positive_limit(limit)
        candidates: list[ClaimedEvent] = []
        records = await self.client.list_failover_replay_candidates(
            failover_started_at=failover_started_at,
            limit=limit,
            exclude_event_ids=exclude_event_ids,
        )
        claimed_originals: list[tuple[SqlStoredEvent, SqlStoredEvent]] = []
        locked_ordering_scopes: set[str] = set()
        try:
            for record in records:
                if len(candidates) >= limit:
                    break
                if record.event.event_id in exclude_event_ids:
                    continue
                if record.status not in {
                    OutboxStatus.PENDING,
                    OutboxStatus.IN_FLIGHT,
                    OutboxStatus.SENT,
                }:
                    continue
                if record.event.expires_at < failover_started_at:
                    continue
                scoped_key = ordering_scope(record.event)
                if scoped_key is not None and scoped_key in locked_ordering_scopes:
                    continue
                original = _clone_record(record)
                token = str(uuid4())
                source_status = record.status
                record.status = OutboxStatus.IN_FLIGHT
                record.claim_token = token
                record.claimed_at = self.clock.utcnow()
                record.attempt_count += 1
                try:
                    claimed = await self.client.replace(
                        record,
                        expected_version=record.version,
                    )
                except ClaimConflictError:
                    continue
                claimed_originals.append((claimed, original))
                if scoped_key is not None:
                    locked_ordering_scopes.add(scoped_key)
                candidates.append(
                    ClaimedEvent(
                        event=claimed.event,
                        claim_token=token,
                        attempt_count=claimed.attempt_count,
                        source_status=source_status,
                    )
                )
        except BaseException:
            for claimed, original in reversed(claimed_originals):
                await self.client.replace(original, expected_version=claimed.version)
            raise
        return candidates

    async def freeze_cleanup(self, *, reason: str) -> None:
        await self.client.set_cleanup_freeze(reason)
        self.cleanup_frozen = True
        self.cleanup_freeze_reason = reason

    async def resume_cleanup(self) -> None:
        await self.client.clear_cleanup_freeze()
        self.cleanup_frozen = False
        self.cleanup_freeze_reason = None

    async def cleanup_sent(
        self,
        *,
        now: datetime,
        safety_margin: timedelta,
        batch_size: int | None = None,
        max_per_tick: int | None = None,
    ) -> int:
        require_optional_positive_limit(batch_size, field_name="batch_size")
        require_optional_positive_limit(max_per_tick, field_name="max_per_tick")
        if await self._cleanup_is_frozen():
            return 0
        event_ids = [
            record.event.event_id
            for record in await self.client.list_cleanup_candidates(
                now=now,
                safety_margin=safety_margin,
                limit=max_per_tick,
            )
        ]
        for event_id in event_ids:
            await self.client.delete(event_id)
        return len(event_ids)

    async def repair_failed_to_pending(self, *, event_id: str) -> AdminActionStatus:
        return await self._cas_admin_update(event_id, _repair_failed_record)

    async def replay_event(self, *, event_id: str) -> AdminActionStatus:
        return await self._cas_admin_update(event_id, _replay_record)

    async def _cleanup_is_frozen(self) -> bool:
        self.cleanup_freeze_reason = await self.client.get_cleanup_freeze_reason()
        self.cleanup_frozen = self.cleanup_freeze_reason is not None
        return self.cleanup_frozen

    async def _after_put_acceptance_boundary(self) -> None:
        return

    async def _claimed_record(self, claimed: ClaimedEvent) -> SqlStoredEvent:
        record = await self.client.get(claimed.event.event_id)
        if record is None or not claim_token_matches(
            record.claim_token,
            claimed.claim_token,
        ):
            raise ClaimConflictError("claim token does not match current owner")
        return record

    async def _cas_update(
        self,
        event_id: str,
        mutate: Callable[[SqlStoredEvent], bool],
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

    async def _cas_admin_update(
        self,
        event_id: str,
        mutate: Callable[[SqlStoredEvent], AdminActionStatus],
        *,
        attempts: int = 3,
    ) -> AdminActionStatus:
        for _ in range(attempts):
            record = await self.client.get(event_id)
            if record is None:
                return AdminActionStatus.NOT_FOUND
            status = mutate(record)
            if status is not AdminActionStatus.SUCCESS:
                return status
            try:
                await self.client.replace(record, expected_version=record.version)
                return AdminActionStatus.SUCCESS
            except ClaimConflictError:
                continue
        raise RetryableStoreError("record update lost too many version races")

    def _eligible_for_claim(self, record: SqlStoredEvent, now: datetime) -> bool:
        return is_claimable_record(
            record,
            now=now,
            claim_timeout=self.claim_timeout,
        )

    async def _in_flight_ordering_keys(self, now: datetime) -> set[str]:
        return in_flight_ordering_keys(
            await self.client.list_records(),
            now=now,
            claim_timeout=self.claim_timeout,
        )


def _mark_sent_if_claimed(
    record: SqlStoredEvent,
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


def _fresh_ordering_blocker(
    record: SqlStoredEvent,
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


def _mark_pending_if_claimed(
    record: SqlStoredEvent,
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
    record: SqlStoredEvent,
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


def _repair_failed_record(record: SqlStoredEvent) -> AdminActionStatus:
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
    return AdminActionStatus.SUCCESS


def _replay_record(record: SqlStoredEvent) -> AdminActionStatus:
    record.status = OutboxStatus.PENDING
    record.claim_token = None
    record.claimed_at = None
    record.next_attempt_at = None
    record.sent_at = None
    record.publish_result = None
    record.failed_at = None
    record.last_error_type = None
    record.last_error = None
    return AdminActionStatus.SUCCESS


def _require_claim(record: SqlStoredEvent, claimed: ClaimedEvent) -> None:
    if not claim_token_matches(record.claim_token, claimed.claim_token):
        raise ClaimConflictError("claim token does not match current owner")


class AzureSqlSyncOutboxStore(_SqlOutboxStoreBase):
    def __init__(
        self,
        config: AzureSqlSyncConfiguration | None = None,
        *,
        client: SqlOutboxClient | None = None,
        store_name: str = "AzureSqlSyncOutboxStore",
        claim_timeout: timedelta = timedelta(minutes=5),
        clock: Clock | None = None,
    ) -> None:
        self.config = config or AzureSqlSyncConfiguration()
        super().__init__(
            client=client,
            claim_timeout=claim_timeout,
            clock=clock,
        )
        self.capabilities = OutboxCapabilities(
            store_name=store_name,
            rpo_zero_for_accepted_events=True,
            supports_ordering=True,
            supports_failover_replay=True,
            supports_ttl_freeze=True,
        )
        self._durability_witness = ("azure-sql:primary", "azure-sql:sync-secondary")

    @classmethod
    def for_testing(
        cls,
        config: AzureSqlSyncConfiguration | None = None,
        *,
        claim_timeout: timedelta = timedelta(minutes=5),
        clock: Clock | None = None,
    ) -> AzureSqlSyncOutboxStore:
        config = config or AzureSqlSyncConfiguration()
        return cls(
            config,
            client=InMemorySqlOutboxClient(
                sync_wait_succeeds=config.sync_wait_succeeds
            ),
            store_name="InMemoryAzureSqlSyncOutboxStore",
            claim_timeout=claim_timeout,
            clock=clock,
        )

    async def wait_for_database_copy_sync(self) -> None:
        await self.client.wait_for_database_copy_sync()

    async def _after_put_acceptance_boundary(self) -> None:
        await self.wait_for_database_copy_sync()


class SqlAlwaysOnOutboxStore(_SqlOutboxStoreBase):
    def __init__(
        self,
        *,
        required_synchronized_secondaries: int = 1,
        client: SqlOutboxClient | None = None,
        store_name: str = "SqlAlwaysOnOutboxStore",
        claim_timeout: timedelta = timedelta(minutes=5),
        clock: Clock | None = None,
    ) -> None:
        if required_synchronized_secondaries < 1:
            msg = "Always On RPO=0 requires at least one synchronized secondary"
            raise ValueError(msg)
        super().__init__(client=client, claim_timeout=claim_timeout, clock=clock)
        self.required_synchronized_secondaries = required_synchronized_secondaries
        self.capabilities = OutboxCapabilities(
            store_name=store_name,
            rpo_zero_for_accepted_events=True,
            supports_ordering=True,
            supports_failover_replay=True,
            supports_ttl_freeze=True,
        )
        self._durability_witness = (
            "sql-always-on:primary",
            *(
                f"sql-always-on:sync-secondary-{index}"
                for index in range(1, required_synchronized_secondaries + 1)
            ),
        )

    @classmethod
    def for_testing(
        cls,
        *,
        required_synchronized_secondaries: int = 1,
        synchronized_secondaries: int = 1,
        claim_timeout: timedelta = timedelta(minutes=5),
        clock: Clock | None = None,
    ) -> SqlAlwaysOnOutboxStore:
        return cls(
            required_synchronized_secondaries=required_synchronized_secondaries,
            client=InMemorySqlOutboxClient(
                synchronized_secondaries=synchronized_secondaries
            ),
            store_name="InMemorySqlAlwaysOnOutboxStore",
            claim_timeout=claim_timeout,
            clock=clock,
        )

    async def _after_put_acceptance_boundary(self) -> None:
        synchronized = await self.client.synchronized_secondary_count()
        if synchronized < self.required_synchronized_secondaries:
            raise RetryableStoreError("Always On synchronized secondary count too low")


def _clone_record(
    record: SqlStoredEvent,
    *,
    version: int | None = None,
) -> SqlStoredEvent:
    return SqlStoredEvent(
        event=record.event,
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
