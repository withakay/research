from dataclasses import dataclass

from durable_outbox.core.errors import NonRetryablePublishError
from durable_outbox.core.retry import RetryPolicy
from durable_outbox.core.sink import MessageSink
from durable_outbox.core.store import DurableOutboxStore
from durable_outbox.core.time import Clock, SystemClock
from durable_outbox.telemetry.metrics import MetricsAdapter, NoopMetrics

MAX_STORED_ERROR_MESSAGE_BYTES = 512
TRUNCATED_ERROR_SUFFIX = "...[truncated]"


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
                error_type = type(exc).__name__
                error_message = _stored_error_message(exc)
                try:
                    await self.store.mark_failed(
                        claimed,
                        error_type=error_type,
                        error_message=error_message.message,
                    )
                except Exception as store_exc:
                    self._record_store_update_failure(
                        topic=event.topic,
                        operation="mark_failed",
                        exc=store_exc,
                    )
                    store_update_failed += 1
                    continue
                if error_message.truncated:
                    self.metrics.increment(
                        "outbox_error_messages_truncated_total",
                        topic=event.topic,
                        error_type=error_type,
                    )
                self.metrics.increment(
                    "outbox_events_failed_total",
                    topic=event.topic,
                    error_type=error_type,
                )
                failed += 1
            except Exception as exc:
                error_type = type(exc).__name__
                error_message = _stored_error_message(exc)
                next_attempt_at = self.retry_policy.next_attempt_at(
                    self.clock.utcnow(),
                    attempt_count=claimed.attempt_count,
                )
                try:
                    await self.store.mark_pending_after_retryable_failure(
                        claimed,
                        error_type=error_type,
                        error_message=error_message.message,
                        next_attempt_at=next_attempt_at,
                    )
                except Exception as store_exc:
                    self._record_store_update_failure(
                        topic=event.topic,
                        operation="mark_pending_after_retryable_failure",
                        exc=store_exc,
                    )
                    store_update_failed += 1
                    continue
                if error_message.truncated:
                    self.metrics.increment(
                        "outbox_error_messages_truncated_total",
                        topic=event.topic,
                        error_type=error_type,
                    )
                self.metrics.increment(
                    "outbox_publish_failures_total",
                    topic=event.topic,
                    error_type=error_type,
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
                    self._record_store_update_failure(
                        topic=event.topic,
                        operation="mark_sent",
                        exc=exc,
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

    def _record_store_update_failure(
        self,
        *,
        topic: str,
        operation: str,
        exc: Exception,
    ) -> None:
        self.metrics.increment(
            "outbox_store_update_failures_total",
            topic=topic,
            operation=operation,
            error_type=type(exc).__name__,
        )


@dataclass(frozen=True, slots=True)
class StoredErrorMessage:
    message: str
    truncated: bool


def _stored_error_message(exc: BaseException) -> StoredErrorMessage:
    message = str(exc)
    encoded = message.encode("utf-8", errors="replace")
    if len(encoded) <= MAX_STORED_ERROR_MESSAGE_BYTES:
        return StoredErrorMessage(message=message, truncated=False)
    suffix = TRUNCATED_ERROR_SUFFIX.encode()
    prefix_budget = MAX_STORED_ERROR_MESSAGE_BYTES - len(suffix)
    truncated = (
        encoded[:prefix_budget].decode("utf-8", errors="ignore")
        + TRUNCATED_ERROR_SUFFIX
    )
    return StoredErrorMessage(message=truncated, truncated=True)
