from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Protocol

from durable_outbox.core.model import OutboxEvent, OutboxStatus
from durable_outbox.core.ordering import ordering_scope


class ClaimableRecord(Protocol):
    """Record fields required for provider-independent claim decisions."""

    event: OutboxEvent
    status: OutboxStatus
    claimed_at: datetime | None
    next_attempt_at: datetime | None


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
