from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable

from durable_outbox.core.model import OutboxStatus
from durable_outbox.core.ordering import ordering_scope
from durable_outbox.core.validation import require_positive_limit
from durable_outbox.telemetry.metrics import NoopMetrics

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from datetime import datetime

    from durable_outbox.core.model import ClaimedEvent
    from durable_outbox.core.sink import MessageSink
    from durable_outbox.core.store import DurableOutboxStore
    from durable_outbox.telemetry.metrics import MetricsAdapter

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    replayed: int
    errored: int = 0


@runtime_checkable
class FailoverReplayStreamStore(Protocol):
    def iter_failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
    ) -> AsyncIterator[ClaimedEvent]: ...


class FailoverReplayer:
    def __init__(
        self,
        store: DurableOutboxStore,
        sink: MessageSink,
        *,
        require_rpo_zero: bool = True,
        metrics: MetricsAdapter | None = None,
        replay_page_size: int = 100,
        max_concurrency: int = 1,
    ) -> None:
        if require_rpo_zero:
            store.capabilities.require_rpo_zero()
        require_positive_limit(replay_page_size, field_name="replay_page_size")
        require_positive_limit(max_concurrency, field_name="max_concurrency")
        self.store = store
        self.sink = sink
        self.metrics = metrics or NoopMetrics()
        self.replay_page_size = replay_page_size
        self.max_concurrency = max_concurrency

    async def replay_once(
        self, *, failover_started_at: datetime, limit: int
    ) -> ReplaySummary:
        require_positive_limit(limit)
        await self.store.freeze_cleanup(reason="failover replay")
        if isinstance(self.store, FailoverReplayStreamStore):
            return await self._replay_once_streaming(
                failover_started_at=failover_started_at,
                limit=limit,
            )
        return await self._replay_once_paged(
            failover_started_at=failover_started_at,
            limit=limit,
        )

    async def _replay_once_paged(
        self, *, failover_started_at: datetime, limit: int
    ) -> ReplaySummary:
        replayed = 0
        errored = 0
        seen_event_ids: set[str] = set()
        remaining = limit
        while remaining > 0:
            page_limit = min(remaining, self.replay_page_size)
            candidates = await self.store.failover_replay_candidates(
                failover_started_at=failover_started_at,
                limit=page_limit,
                exclude_event_ids=seen_event_ids,
            )
            if not candidates:
                break
            seen_event_ids.update(claimed.event.event_id for claimed in candidates)
            page_replayed = await self._replay_page(candidates)
            replayed += page_replayed
            errored += len(candidates) - page_replayed
            remaining -= len(candidates)
            if len(candidates) < page_limit:
                break
        return ReplaySummary(replayed=replayed, errored=errored)

    async def _replay_once_streaming(
        self, *, failover_started_at: datetime, limit: int
    ) -> ReplaySummary:
        replayed = 0
        errored = 0
        page: list[ClaimedEvent] = []
        stream_store = cast("FailoverReplayStreamStore", self.store)
        async for claimed in stream_store.iter_failover_replay_candidates(
            failover_started_at=failover_started_at,
            limit=limit,
        ):
            page.append(claimed)
            if len(page) < self.replay_page_size:
                continue
            page_replayed = await self._replay_page(page)
            replayed += page_replayed
            errored += len(page) - page_replayed
            page = []
        if page:
            page_replayed = await self._replay_page(page)
            replayed += page_replayed
            errored += len(page) - page_replayed
        return ReplaySummary(replayed=replayed, errored=errored)

    async def complete_replay(self) -> None:
        await self.store.resume_cleanup()

    async def _replay_page(self, candidates: list[ClaimedEvent]) -> int:
        semaphore = asyncio.Semaphore(self.max_concurrency)
        ordering_locks: dict[str, asyncio.Lock] = {}
        results = await asyncio.gather(
            *(
                self._replay_claimed(
                    claimed,
                    semaphore=semaphore,
                    ordering_locks=ordering_locks,
                )
                for claimed in candidates
            )
        )
        return sum(results)

    async def _replay_claimed(
        self,
        claimed: ClaimedEvent,
        *,
        semaphore: asyncio.Semaphore,
        ordering_locks: dict[str, asyncio.Lock],
    ) -> int:
        scoped_key = ordering_scope(claimed.event)
        if scoped_key is None:
            return await self._publish_claimed(claimed, semaphore=semaphore)
        lock = ordering_locks.setdefault(scoped_key, asyncio.Lock())
        async with lock:
            return await self._publish_claimed(claimed, semaphore=semaphore)

    async def _publish_claimed(
        self,
        claimed: ClaimedEvent,
        *,
        semaphore: asyncio.Semaphore,
    ) -> int:
        async with semaphore:
            if claimed.source_status is OutboxStatus.SENT:
                _LOGGER.warning(
                    "Replaying previously sent outbox event; consumers must dedupe "
                    "by event_id",
                    extra={
                        "event_id": claimed.event.event_id,
                        "topic": claimed.event.topic,
                    },
                )
                self.metrics.increment(
                    "outbox_failover_sent_replays_total",
                    topic=claimed.event.topic,
                )
            try:
                result = await self.sink.publish(claimed.event)
                await self.store.mark_sent(claimed, result)
            except Exception as exc:
                self.metrics.increment(
                    "outbox_failover_replay_failures_total",
                    topic=claimed.event.topic,
                    error_type=type(exc).__name__,
                )
                return 0
            return 1
