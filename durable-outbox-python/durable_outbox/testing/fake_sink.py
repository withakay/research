from datetime import UTC, datetime

from durable_outbox.core.model import OutboxEvent, PublishResult


class FakeSink:
    def __init__(self) -> None:
        self.published: list[OutboxEvent] = []

    async def publish(self, event: OutboxEvent) -> PublishResult:
        self.published.append(event)
        return PublishResult(
            partition=0,
            offset=len(self.published) - 1,
            published_at=datetime.now(UTC),
        )
