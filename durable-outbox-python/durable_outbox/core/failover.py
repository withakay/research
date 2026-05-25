from dataclasses import dataclass
from datetime import datetime

from durable_outbox.core.sink import MessageSink
from durable_outbox.core.store import DurableOutboxStore
from durable_outbox.telemetry.metrics import MetricsAdapter, NoopMetrics


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    replayed: int
    errored: int = 0


class FailoverReplayer:
    def __init__(
        self,
        store: DurableOutboxStore,
        sink: MessageSink,
        *,
        require_rpo_zero: bool = True,
        metrics: MetricsAdapter | None = None,
    ) -> None:
        if require_rpo_zero:
            store.capabilities.require_rpo_zero()
        self.store = store
        self.sink = sink
        self.metrics = metrics or NoopMetrics()

    async def replay_once(
        self, *, failover_started_at: datetime, limit: int
    ) -> ReplaySummary:
        await self.store.freeze_cleanup(reason="failover replay")
        candidates = await self.store.failover_replay_candidates(
            failover_started_at=failover_started_at,
            limit=limit,
        )
        replayed = 0
        errored = 0
        for claimed in candidates:
            try:
                result = await self.sink.publish(claimed.event)
                await self.store.mark_sent(claimed, result)
            except Exception as exc:
                self.metrics.increment(
                    "outbox_failover_replay_failures_total",
                    topic=claimed.event.topic,
                    error_type=type(exc).__name__,
                )
                errored += 1
                continue
            replayed += 1
        return ReplaySummary(replayed=replayed, errored=errored)

    async def complete_replay(self) -> None:
        await self.store.resume_cleanup()
