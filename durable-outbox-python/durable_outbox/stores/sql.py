from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol
from uuid import uuid4

from durable_outbox.core.capabilities import OutboxCapabilities
from durable_outbox.core.errors import (
    ClaimConflictError,
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
from durable_outbox.core.validation import enforce_payload_size, require_positive_limit

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
    ordering_key_hash   NVARCHAR(128) NULL,
    ordering_sequence   BIGINT NULL,
    created_at_utc      DATETIME2 NOT NULL,
    expires_at_utc      DATETIME2 NOT NULL,
    next_attempt_utc    DATETIME2 NULL,
    attempt_count       INT NOT NULL DEFAULT 0,
    claimed_by          NVARCHAR(256) NULL,
    claim_id            UNIQUEIDENTIFIER NULL,
    claimed_at_utc      DATETIME2 NULL,
    sent_at_utc         DATETIME2 NULL,
    kafka_partition     INT NULL,
    kafka_offset        BIGINT NULL,
    failed_at_utc       DATETIME2 NULL,
    last_error_type     NVARCHAR(256) NULL,
    last_error          NVARCHAR(1024) NULL,
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

    async def delete(self, event_id: str) -> None: ...

    async def wait_for_database_copy_sync(self) -> None: ...

    async def synchronized_secondary_count(self) -> int: ...


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

    async def delete(self, event_id: str) -> None:
        self.records.pop(event_id, None)

    async def wait_for_database_copy_sync(self) -> None:
        self.sync_wait_count += 1
        if not self.sync_wait_succeeds:
            raise RetryableStoreError("sp_wait_for_database_copy_sync timed out")

    async def synchronized_secondary_count(self) -> int:
        return self.synchronized_secondaries


class _SqlOutboxStoreBase:
    capabilities: OutboxCapabilities

    def __init__(
        self,
        *,
        client: SqlOutboxClient | None = None,
        claim_timeout: timedelta = timedelta(minutes=5),
        clock: Clock | None = None,
    ) -> None:
        self.client = client or InMemorySqlOutboxClient()
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
        elif record.event != event:
            raise DuplicateEventConflictError(
                "event_id already exists with incompatible content"
            )
        await self._after_put_acceptance_boundary()
        return AcceptedReceipt(
            event_id=event.event_id,
            accepted_at=record.accepted_at or now,
            rpo_zero=self.capabilities.rpo_zero_for_accepted_events,
            store=self.capabilities.store_name,
        )

    async def claim_batch(self, *, limit: int) -> list[ClaimedEvent]:
        return await self._claim_from_candidates(
            await self._claim_ordered_records(),
            limit=limit,
        )

    async def _claim_ordered_records(self) -> list[SqlStoredEvent]:
        return sorted(
            await self.client.list_records(),
            key=lambda record: (
                record.event.topic,
                record.event.ordering_key or "",
                record.event.ordering_sequence or 0,
                record.event.created_at,
            ),
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
        record = await self._claimed_record(claimed)
        record.status = OutboxStatus.SENT
        record.sent_at = result.published_at
        record.publish_result = result
        record.claim_token = None
        record.claimed_at = None
        await self.client.replace(record, expected_version=record.version)

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
        await self.client.replace(record, expected_version=record.version)

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
        await self.client.replace(record, expected_version=record.version)

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
        event_ids = [
            record.event.event_id
            for record in await self.client.list_records()
            if record.status is OutboxStatus.SENT
            and now > record.event.expires_at + safety_margin
        ]
        for event_id in event_ids:
            await self.client.delete(event_id)
        return len(event_ids)

    async def repair_failed_to_pending(self, *, event_id: str) -> None:
        record = await self.client.get(event_id)
        if record is None or record.status is not OutboxStatus.FAILED:
            return
        record.status = OutboxStatus.PENDING
        record.failed_at = None
        record.attempt_count = 0
        record.last_error_type = None
        record.last_error = None
        record.next_attempt_at = None
        record.claim_token = None
        record.claimed_at = None
        await self.client.replace(record, expected_version=record.version)

    async def _after_put_acceptance_boundary(self) -> None:
        return

    async def _claimed_record(self, claimed: ClaimedEvent) -> SqlStoredEvent:
        record = await self.client.get(claimed.event.event_id)
        if record is None or record.claim_token != claimed.claim_token:
            raise ClaimConflictError("claim token does not match current owner")
        return record

    def _eligible_for_claim(self, record: SqlStoredEvent, now: datetime) -> bool:
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


class AzureSqlSyncOutboxStore(_SqlOutboxStoreBase):
    def __init__(
        self,
        config: AzureSqlSyncConfiguration | None = None,
        *,
        client: SqlOutboxClient | None = None,
        claim_timeout: timedelta = timedelta(minutes=5),
        clock: Clock | None = None,
    ) -> None:
        self.config = config or AzureSqlSyncConfiguration()
        default_client = InMemorySqlOutboxClient(
            sync_wait_succeeds=self.config.sync_wait_succeeds
        )
        super().__init__(
            client=client or default_client,
            claim_timeout=claim_timeout,
            clock=clock,
        )
        self.capabilities = OutboxCapabilities(
            store_name="AzureSqlSyncOutboxStore",
            rpo_zero_for_accepted_events=True,
            supports_ordering=True,
            supports_failover_replay=True,
            supports_ttl_freeze=True,
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
        claim_timeout: timedelta = timedelta(minutes=5),
        clock: Clock | None = None,
    ) -> None:
        if required_synchronized_secondaries < 1:
            msg = "Always On RPO=0 requires at least one synchronized secondary"
            raise ValueError(msg)
        super().__init__(client=client, claim_timeout=claim_timeout, clock=clock)
        self.required_synchronized_secondaries = required_synchronized_secondaries
        self.capabilities = OutboxCapabilities(
            store_name="SqlAlwaysOnOutboxStore",
            rpo_zero_for_accepted_events=True,
            supports_ordering=True,
            supports_failover_replay=True,
            supports_ttl_freeze=True,
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
