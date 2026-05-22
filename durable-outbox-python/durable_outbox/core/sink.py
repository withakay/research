from typing import Protocol

from durable_outbox.core.model import OutboxEvent, PublishResult


class MessageSink(Protocol):
    async def publish(self, event: OutboxEvent) -> PublishResult: ...
