from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from durable_outbox.core.errors import ValidationError
from durable_outbox.core.model import ClaimedEvent, OutboxEvent, PublishingMode


@dataclass(frozen=True, slots=True)
class OrderingLockLease:
    lock_name: str
    owner_token: str
    expires_at: datetime


class OrderingLockBackend(Protocol):
    async def acquire(
        self,
        *,
        lock_name: str,
        owner_token: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> OrderingLockLease | None: ...

    async def release(self, lease: OrderingLockLease) -> None: ...


class OrderingCoordinator(Protocol):
    async def claim_ordered_batch(self, *, limit: int) -> list[ClaimedEvent]: ...


class InMemoryOrderingLockBackend:
    def __init__(self) -> None:
        self._leases: dict[str, OrderingLockLease] = {}

    async def acquire(
        self,
        *,
        lock_name: str,
        owner_token: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> OrderingLockLease | None:
        current = self._leases.get(lock_name)
        if current is not None and current.expires_at > now:
            return None
        lease = OrderingLockLease(
            lock_name=lock_name,
            owner_token=owner_token,
            expires_at=now + lease_duration,
        )
        self._leases[lock_name] = lease
        return lease

    async def release(self, lease: OrderingLockLease) -> None:
        current = self._leases.get(lease.lock_name)
        if current is None or current.owner_token != lease.owner_token:
            return
        del self._leases[lease.lock_name]

    def active_lease(self, lock_name: str) -> OrderingLockLease | None:
        return self._leases.get(lock_name)


def validate_ordered_event(event: OutboxEvent) -> None:
    if event.publishing_mode is not PublishingMode.ORDERED:
        return
    if not event.ordering_key:
        raise ValidationError("ordered events require ordering_key")


def one_per_ordering_key(claims: Iterable[ClaimedEvent]) -> list[ClaimedEvent]:
    selected: list[ClaimedEvent] = []
    seen: set[str] = set()
    for claim in claims:
        key = claim.event.effective_ordering_key
        if key is None:
            selected.append(claim)
            continue
        if key in seen:
            continue
        seen.add(key)
        selected.append(claim)
    return selected
