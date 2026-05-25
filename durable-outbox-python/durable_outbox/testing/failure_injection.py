from collections.abc import Iterable
from datetime import datetime, timedelta

from durable_outbox.core.admin import AdminActionStatus
from durable_outbox.core.errors import DurableOutboxError, RetryablePublishError
from durable_outbox.core.model import (
    AcceptedReceipt,
    ClaimedEvent,
    OutboxEvent,
    PublishResult,
)
from durable_outbox.core.sink import MessageSink
from durable_outbox.core.store import DurableOutboxStore


class FailingSink:
    def __init__(
        self,
        *,
        errors: Iterable[Exception] = (),
        fallback: MessageSink | None = None,
    ) -> None:
        self._errors = list(errors)
        from durable_outbox.testing.fake_sink import FakeSink

        self._fallback = fallback or FakeSink()

    async def publish(self, event: OutboxEvent) -> PublishResult:
        if self._errors:
            error = self._errors.pop(0)
            raise error
        return await self._fallback.publish(event)


def retryable_failure(message: str = "transient failure") -> DurableOutboxError:
    return RetryablePublishError(message)


class FailingStore:
    def __init__(
        self,
        store: DurableOutboxStore,
        *,
        put_errors: Iterable[Exception] = (),
        claim_errors: Iterable[Exception] = (),
        mark_retry_errors: Iterable[Exception] = (),
        mark_failed_errors: Iterable[Exception] = (),
        mark_sent_errors: Iterable[Exception] = (),
    ) -> None:
        self._store = store
        self._put_errors = list(put_errors)
        self._claim_errors = list(claim_errors)
        self._mark_retry_errors = list(mark_retry_errors)
        self._mark_failed_errors = list(mark_failed_errors)
        self._mark_sent_errors = list(mark_sent_errors)
        self.capabilities = store.capabilities

    async def put(self, event: OutboxEvent) -> AcceptedReceipt:
        if self._put_errors:
            raise self._put_errors.pop(0)
        return await self._store.put(event)

    async def claim_batch(self, *, limit: int) -> list[ClaimedEvent]:
        if self._claim_errors:
            raise self._claim_errors.pop(0)
        return await self._store.claim_batch(limit=limit)

    async def mark_sent(self, claimed: ClaimedEvent, result: PublishResult) -> None:
        if self._mark_sent_errors:
            raise self._mark_sent_errors.pop(0)
        await self._store.mark_sent(claimed, result)

    async def mark_pending_after_retryable_failure(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
        next_attempt_at: datetime,
    ) -> None:
        if self._mark_retry_errors:
            raise self._mark_retry_errors.pop(0)
        await self._store.mark_pending_after_retryable_failure(
            claimed,
            error_type=error_type,
            error_message=error_message,
            next_attempt_at=next_attempt_at,
        )

    async def mark_failed(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
    ) -> None:
        if self._mark_failed_errors:
            raise self._mark_failed_errors.pop(0)
        await self._store.mark_failed(
            claimed,
            error_type=error_type,
            error_message=error_message,
        )

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
    ) -> list[ClaimedEvent]:
        return await self._store.failover_replay_candidates(
            failover_started_at=failover_started_at,
            limit=limit,
        )

    async def freeze_cleanup(self, *, reason: str) -> None:
        await self._store.freeze_cleanup(reason=reason)

    async def resume_cleanup(self) -> None:
        await self._store.resume_cleanup()

    async def cleanup_sent(self, *, now: datetime, safety_margin: timedelta) -> int:
        return await self._store.cleanup_sent(now=now, safety_margin=safety_margin)

    async def repair_failed_to_pending(self, *, event_id: str) -> AdminActionStatus:
        return await self._store.repair_failed_to_pending(event_id=event_id)

    async def replay_event(self, *, event_id: str) -> AdminActionStatus:
        return await self._store.replay_event(event_id=event_id)
