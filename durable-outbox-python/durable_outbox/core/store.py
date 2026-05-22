from datetime import datetime
from typing import Protocol

from durable_outbox.core.capabilities import OutboxCapabilities
from durable_outbox.core.model import (
    AcceptedReceipt,
    ClaimedEvent,
    OutboxEvent,
    PublishResult,
)


class DurableOutboxStore(Protocol):
    capabilities: OutboxCapabilities

    async def put(self, event: OutboxEvent) -> AcceptedReceipt: ...

    async def claim_batch(self, *, limit: int) -> list[ClaimedEvent]: ...

    async def mark_sent(self, claimed: ClaimedEvent, result: PublishResult) -> None: ...

    async def mark_pending_after_retryable_failure(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
        next_attempt_at: datetime,
    ) -> None: ...

    async def mark_failed(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
    ) -> None: ...

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
    ) -> list[ClaimedEvent]: ...

    async def freeze_cleanup(self, *, reason: str) -> None: ...

    async def resume_cleanup(self) -> None: ...
