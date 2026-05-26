from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
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
    ValidationError,
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
from durable_outbox.core.validation import (
    enforce_payload_size,
    require_optional_positive_limit,
    require_positive_limit,
)

if TYPE_CHECKING:
    from collections.abc import Collection


@dataclass(slots=True)
class StoredEvent:
    event: OutboxEvent
    status: OutboxStatus = OutboxStatus.PENDING
    accepted: bool = True
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


@dataclass(slots=True)
class CleanupFreezeState:
    reason: str | None = None


class MemoryOutboxStore:
    capabilities = OutboxCapabilities(
        store_name="MemoryOutboxStore",
        rpo_zero_for_accepted_events=False,
        supports_ordering=True,
        supports_failover_replay=True,
        supports_ttl_freeze=True,
    )

    def __init__(
        self,
        *,
        claim_timeout: timedelta = timedelta(minutes=5),
        cleanup_state: CleanupFreezeState | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.records: dict[str, StoredEvent] = {}
        self.claim_timeout = claim_timeout
        self.clock = clock or SystemClock()
        self._cleanup_state = cleanup_state or CleanupFreezeState()
        self.cleanup_frozen = False
        self.cleanup_freeze_reason: str | None = None
        self._in_flight_ordering_index = InFlightOrderingIndex()

    async def put(self, event: OutboxEvent) -> AcceptedReceipt:
        enforce_payload_size(event, self.capabilities)
        if event.publishing_mode is PublishingMode.ORDERED and not event.ordering_key:
            raise ValidationError("ordered events require ordering_key")
        now = self.clock.utcnow()
        record = self.records.get(event.event_id)
        if record is None:
            record = StoredEvent(event=event, accepted_at=now)
            self.records[event.event_id] = record
        else:
            raise_if_incompatible_duplicate(record.event, event)
        return AcceptedReceipt(
            event_id=event.event_id,
            accepted_at=record.accepted_at or now,
            rpo_zero=self.capabilities.rpo_zero_for_accepted_events,
            store=self.capabilities.store_name,
            durability_witness=("memory:process",),
        )

    async def claim_batch(self, *, limit: int) -> list[ClaimedEvent]:
        require_positive_limit(limit)
        now = self.clock.utcnow()
        claimed: list[ClaimedEvent] = []
        locked_keys = self._in_flight_ordering_keys(now)
        ordered_pending = sorted(self.records.values(), key=claim_order_key)
        for record in ordered_pending:
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
        record = self._claimed_record(claimed)
        record.status = OutboxStatus.SENT
        record.sent_at = result.published_at
        record.publish_result = result
        record.claim_token = None
        record.claimed_at = None
        self._in_flight_ordering_index.release(record.event)

    async def mark_pending_after_retryable_failure(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
        next_attempt_at: datetime,
    ) -> None:
        record = self._claimed_record(claimed)
        record.status = OutboxStatus.PENDING
        record.next_attempt_at = next_attempt_at
        record.last_error_type = error_type
        record.last_error = error_message
        record.claim_token = None
        record.claimed_at = None
        self._in_flight_ordering_index.release(record.event)

    async def mark_failed(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
    ) -> None:
        record = self._claimed_record(claimed)
        record.status = OutboxStatus.FAILED
        record.failed_at = self.clock.utcnow()
        record.last_error_type = error_type
        record.last_error = error_message
        record.claim_token = None
        record.claimed_at = None
        self._in_flight_ordering_index.release(record.event)

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> list[ClaimedEvent]:
        require_positive_limit(limit)
        candidates: list[ClaimedEvent] = []
        originals: dict[str, StoredEvent] = {}
        locked_ordering_scopes: set[str] = set()
        try:
            for record in sorted(
                self.records.values(), key=lambda item: item.event.created_at
            ):
                if len(candidates) >= limit:
                    break
                if record.event.event_id in exclude_event_ids:
                    continue
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
                scoped_key = ordering_scope(record.event)
                if scoped_key is not None and scoped_key in locked_ordering_scopes:
                    continue
                event_id = record.event.event_id
                originals.setdefault(event_id, _clone_record(record))
                token = str(uuid4())
                source_status = record.status
                record.status = OutboxStatus.IN_FLIGHT
                record.claim_token = token
                record.claimed_at = self.clock.utcnow()
                record.attempt_count += 1
                self._in_flight_ordering_index.record_claim(
                    record.event,
                    claimed_at=record.claimed_at,
                )
                if scoped_key is not None:
                    locked_ordering_scopes.add(scoped_key)
                candidates.append(
                    ClaimedEvent(
                        event=record.event,
                        claim_token=token,
                        attempt_count=record.attempt_count,
                        source_status=source_status,
                    )
                )
        except BaseException:
            for event_id, original in originals.items():
                self.records[event_id] = original
                self._in_flight_ordering_index.release(original.event)
            raise
        return candidates

    async def freeze_cleanup(self, *, reason: str) -> None:
        self._cleanup_state.reason = reason
        self.cleanup_frozen = True
        self.cleanup_freeze_reason = reason

    async def resume_cleanup(self) -> None:
        self._cleanup_state.reason = None
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
        if self._cleanup_is_frozen():
            return 0
        to_delete = [
            event_id
            for event_id, record in self.records.items()
            if record.status is OutboxStatus.SENT
            and now > record.event.expires_at + safety_margin
        ]
        if max_per_tick is not None:
            to_delete = to_delete[:max_per_tick]
        for event_id in to_delete:
            del self.records[event_id]
        return len(to_delete)

    async def repair_failed_to_pending(self, *, event_id: str) -> AdminActionStatus:
        record = self.records.get(event_id)
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
        self._in_flight_ordering_index.release(record.event)
        return AdminActionStatus.SUCCESS

    def _cleanup_is_frozen(self) -> bool:
        self.cleanup_freeze_reason = self._cleanup_state.reason
        self.cleanup_frozen = self.cleanup_freeze_reason is not None
        return self.cleanup_frozen

    async def replay_event(self, *, event_id: str) -> AdminActionStatus:
        record = self.records.get(event_id)
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
        self._in_flight_ordering_index.release(record.event)
        return AdminActionStatus.SUCCESS

    def _claimed_record(self, claimed: ClaimedEvent) -> StoredEvent:
        record = self.records[claimed.event.event_id]
        if not claim_token_matches(record.claim_token, claimed.claim_token):
            raise ClaimConflictError("claim token does not match current owner")
        return record

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
