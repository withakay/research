import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol

from durable_outbox.core.errors import ValidationError
from durable_outbox.core.time import Clock, SystemClock
from durable_outbox.core.validation import require_optional_positive_limit

if TYPE_CHECKING:
    from datetime import datetime


class CleanupStore(Protocol):
    async def cleanup_sent(
        self,
        *,
        now: datetime,
        safety_margin: timedelta,
        batch_size: int | None = None,
        max_per_tick: int | None = None,
    ) -> int: ...


@dataclass(frozen=True, slots=True)
class CleanupPolicy:
    """Retention windows used by host cleanup runners."""

    sent_safety_margin: timedelta = timedelta(minutes=5)
    failed_retention: timedelta = timedelta(days=30)
    interval: timedelta = timedelta(minutes=1)
    batch_size: int | None = 100
    max_per_tick: int | None = 1000

    def __post_init__(self) -> None:
        if self.interval <= timedelta(0):
            raise ValidationError("cleanup interval must be positive")
        require_optional_positive_limit(self.batch_size, field_name="batch_size")
        require_optional_positive_limit(self.max_per_tick, field_name="max_per_tick")


@dataclass(slots=True)
class CleanupScheduler:
    """Runs sent-event cleanup with a bounded per-tick policy."""

    store: CleanupStore
    clock: Clock
    policy: CleanupPolicy

    def __init__(
        self,
        *,
        store: CleanupStore,
        clock: Clock | None = None,
        policy: CleanupPolicy | None = None,
    ) -> None:
        self.store = store
        self.clock = clock or SystemClock()
        self.policy = policy or CleanupPolicy()

    async def run_once(self) -> int:
        return await self.store.cleanup_sent(
            now=self.clock.utcnow(),
            safety_margin=self.policy.sent_safety_margin,
            batch_size=self.policy.batch_size,
            max_per_tick=self.policy.max_per_tick,
        )

    async def run_until_stopped(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.policy.interval.total_seconds(),
                )
            except TimeoutError:
                continue
