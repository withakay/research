from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Collection
    from datetime import datetime, timedelta

    from durable_outbox.core.admin import AdminActionStatus
    from durable_outbox.core.capabilities import OutboxCapabilities
    from durable_outbox.core.model import (
        AcceptedReceipt,
        ClaimedEvent,
        OutboxEvent,
        PublishResult,
    )


@runtime_checkable
class DurableOutboxStore(Protocol):
    """Persistence contract for accepted, claimed, replayed, and cleaned events."""

    capabilities: OutboxCapabilities

    async def put(self, event: OutboxEvent) -> AcceptedReceipt:
        """Persist `event` idempotently by `event_id` or reject incompatible duplicates."""
        ...

    async def claim_batch(self, *, limit: int) -> list[ClaimedEvent]:
        """Claim at most `limit` eligible events and move them to `IN_FLIGHT`."""
        ...

    async def mark_sent(self, claimed: ClaimedEvent, result: PublishResult) -> None:
        """Mark a currently claimed event as `SENT` after sink acknowledgement."""
        ...

    async def mark_pending_after_retryable_failure(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
        next_attempt_at: datetime,
    ) -> None:
        """Release a failed claim back to `PENDING` for a future retry attempt."""
        ...

    async def mark_failed(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
    ) -> None:
        """Move a claimed event to terminal `FAILED` after deterministic sink failure."""
        ...

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
        exclude_event_ids: Collection[str] = (),
    ) -> list[ClaimedEvent]:
        """Claim accepted replay candidates that remain TTL-valid for failover."""
        ...

    async def freeze_cleanup(self, *, reason: str) -> None:
        """Prevent sent-event cleanup while failover or operator review is active."""
        ...

    async def resume_cleanup(self) -> None:
        """Resume normal cleanup after a previous `freeze_cleanup` call."""
        ...

    async def cleanup_sent(
        self,
        *,
        now: datetime,
        safety_margin: timedelta,
        batch_size: int | None = None,
        max_per_tick: int | None = None,
    ) -> int:
        """Delete expired `SENT` events older than `safety_margin` and return the count."""
        ...

    async def repair_failed_to_pending(self, *, event_id: str) -> AdminActionStatus:
        """Reset an existing `FAILED` event to `PENDING` for operator-driven repair."""
        ...

    async def replay_event(self, *, event_id: str) -> AdminActionStatus:
        """Reset an existing event to `PENDING` for explicit manual replay."""
        ...
