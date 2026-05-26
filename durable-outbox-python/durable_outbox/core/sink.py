from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from durable_outbox.core.model import OutboxEvent, PublishResult


@runtime_checkable
class MessageSink(Protocol):
    """Delivery contract for publishing one outbox event to an external sink."""

    async def publish(self, event: OutboxEvent) -> PublishResult:
        """Publish `event` once and return broker acknowledgement metadata."""
        ...
