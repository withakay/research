from dataclasses import dataclass
from hashlib import sha256
from typing import Protocol

from durable_outbox.core.model import OutboxEvent


class DedupeStore(Protocol):
    async def add_if_absent(self, key: str) -> bool: ...


class InMemoryDedupeStore:
    def __init__(self) -> None:
        self.keys: set[str] = set()

    async def add_if_absent(self, key: str) -> bool:
        if key in self.keys:
            return False
        self.keys.add(key)
        return True


@dataclass(frozen=True, slots=True)
class DedupeDecision:
    event_id: str
    topic: str
    duplicate: bool

    @property
    def should_process(self) -> bool:
        return not self.duplicate


class EventDeduper:
    def __init__(self, store: DedupeStore | None = None) -> None:
        self.store = store or InMemoryDedupeStore()

    async def check(self, event: OutboxEvent) -> DedupeDecision:
        inserted = await self.store.add_if_absent(
            dedupe_key(event.topic, event.event_id)
        )
        return DedupeDecision(
            event_id=event.event_id,
            topic=event.topic,
            duplicate=not inserted,
        )


def dedupe_key(topic: str, event_id: str) -> str:
    return sha256(f"{topic}\0{event_id}".encode()).hexdigest()
