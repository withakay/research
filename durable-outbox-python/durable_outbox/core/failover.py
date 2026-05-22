from dataclasses import dataclass
from datetime import datetime

from durable_outbox.core.sink import MessageSink
from durable_outbox.core.store import DurableOutboxStore


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    replayed: int


class FailoverReplayer:
    def __init__(self, store: DurableOutboxStore, sink: MessageSink) -> None:
        self.store = store
        self.sink = sink

    async def replay_once(
        self, *, failover_started_at: datetime, limit: int
    ) -> ReplaySummary:
        await self.store.freeze_cleanup(reason="failover replay")
        candidates = await self.store.failover_replay_candidates(
            failover_started_at=failover_started_at,
            limit=limit,
        )
        replayed = 0
        for claimed in candidates:
            result = await self.sink.publish(claimed.event)
            await self.store.mark_sent(claimed, result)
            replayed += 1
        return ReplaySummary(replayed=replayed)

    async def complete_replay(self) -> None:
        await self.store.resume_cleanup()
