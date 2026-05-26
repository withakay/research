from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from durable_outbox.core.model import OutboxStatus
from durable_outbox.core.ordering import ordering_scope

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import datetime, timedelta

    from durable_outbox.core.model import OutboxEvent


class ClaimableRecord(Protocol):
    """Record fields required for provider-independent claim decisions."""

    event: OutboxEvent
    status: OutboxStatus
    claimed_at: datetime | None
    next_attempt_at: datetime | None


class InFlightOrderingIndex:
    """Local index of fresh in-flight ordered claims keyed by event id."""

    def __init__(self) -> None:
        self._claims_by_event_id: dict[str, tuple[str, datetime]] = {}

    def record_claim(self, event: OutboxEvent, *, claimed_at: datetime) -> None:
        key = ordering_scope(event)
        if key is None:
            return
        self._claims_by_event_id[event.event_id] = (key, claimed_at)

    def release(self, event: OutboxEvent) -> None:
        self._claims_by_event_id.pop(event.event_id, None)

    def clear(self) -> None:
        self._claims_by_event_id.clear()

    def rebuild(
        self,
        records: Iterable[ClaimableRecord],
        *,
        now: datetime,
        claim_timeout: timedelta,
    ) -> None:
        self.clear()
        for record in records:
            if record.status is not OutboxStatus.IN_FLIGHT:
                continue
            if record.claimed_at is None:
                continue
            if record.claimed_at + claim_timeout <= now:
                continue
            self.record_claim(record.event, claimed_at=record.claimed_at)

    def active_keys(self, *, now: datetime, claim_timeout: timedelta) -> set[str]:
        locked: set[str] = set()
        stale: list[str] = []
        for event_id, (key, claimed_at) in self._claims_by_event_id.items():
            if claimed_at + claim_timeout <= now:
                stale.append(event_id)
                continue
            locked.add(key)
        for event_id in stale:
            self._claims_by_event_id.pop(event_id, None)
        return locked


def is_claimable_record(
    record: ClaimableRecord,
    *,
    now: datetime,
    claim_timeout: timedelta,
) -> bool:
    if record.status is OutboxStatus.PENDING:
        return record.next_attempt_at is None or record.next_attempt_at <= now
    if record.status is OutboxStatus.IN_FLIGHT and record.claimed_at is not None:
        return record.claimed_at + claim_timeout <= now
    return False


def in_flight_ordering_keys(
    records: Iterable[ClaimableRecord],
    *,
    now: datetime,
    claim_timeout: timedelta,
) -> set[str]:
    locked: set[str] = set()
    for record in records:
        key = ordering_scope(record.event)
        if key is None:
            continue
        if record.status is not OutboxStatus.IN_FLIGHT:
            continue
        if record.claimed_at is None or record.claimed_at + claim_timeout <= now:
            continue
        locked.add(key)
    return locked


def claim_order_key[T: ClaimableRecord](record: T) -> tuple[str, str, int, datetime]:
    event = record.event
    return (
        event.topic,
        event.ordering_key or "",
        event.ordering_sequence or 0,
        event.created_at,
    )
