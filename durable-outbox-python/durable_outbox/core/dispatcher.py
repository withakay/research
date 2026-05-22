from dataclasses import dataclass

from durable_outbox.core.errors import NonRetryablePublishError
from durable_outbox.core.retry import RetryPolicy
from durable_outbox.core.sink import MessageSink
from durable_outbox.core.store import DurableOutboxStore
from durable_outbox.core.time import Clock, SystemClock
from durable_outbox.telemetry.metrics import MetricsAdapter, NoopMetrics


@dataclass(frozen=True, slots=True)
class DispatchSummary:
    claimed: int = 0
    sent: int = 0
    retried: int = 0
    failed: int = 0
    store_update_failed: int = 0


class OutboxDispatcher:
    def __init__(
        self,
        store: DurableOutboxStore,
        sink: MessageSink,
        *,
        clock: Clock | None = None,
        retry_policy: RetryPolicy | None = None,
        metrics: MetricsAdapter | None = None,
    ) -> None:
        self.store = store
        self.sink = sink
        self.clock = clock or SystemClock()
        self.retry_policy = retry_policy or RetryPolicy()
        self.metrics = metrics or NoopMetrics()

    async def run_once(self, *, limit: int = 100) -> DispatchSummary:
        claimed_events = await self.store.claim_batch(limit=limit)
        sent = retried = failed = store_update_failed = 0

        for claimed in claimed_events:
            event = claimed.event
            self.metrics.increment("outbox_publish_attempts_total", topic=event.topic)
            try:
                result = await self.sink.publish(event)
            except NonRetryablePublishError as exc:
                await self.store.mark_failed(
                    claimed,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                self.metrics.increment(
                    "outbox_events_failed_total",
                    topic=event.topic,
                    error_type=type(exc).__name__,
                )
                failed += 1
            except Exception as exc:
                next_attempt_at = self.retry_policy.next_attempt_at(
                    self.clock.utcnow(),
                    attempt_count=claimed.attempt_count,
                )
                await self.store.mark_pending_after_retryable_failure(
                    claimed,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    next_attempt_at=next_attempt_at,
                )
                self.metrics.increment(
                    "outbox_publish_failures_total",
                    topic=event.topic,
                    error_type=type(exc).__name__,
                )
                retried += 1
            else:
                try:
                    await self.store.mark_sent(claimed, result)
                except Exception as exc:
                    self.metrics.increment(
                        "outbox_mark_sent_failures_total",
                        topic=event.topic,
                        error_type=type(exc).__name__,
                    )
                    store_update_failed += 1
                    continue
                self.metrics.increment(
                    "outbox_publish_success_total", topic=event.topic
                )
                sent += 1

        return DispatchSummary(
            claimed=len(claimed_events),
            sent=sent,
            retried=retried,
            failed=failed,
            store_update_failed=store_update_failed,
        )
