from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from durable_outbox.core.model import OutboxEvent, OutboxStatus
from durable_outbox.core.store import DurableOutboxStore
from durable_outbox.testing.fake_sink import FakeSink

type CleanupHook = Callable[[DurableOutboxStore, datetime], Awaitable[int]]
type StatusHook = Callable[[DurableOutboxStore, str], Awaitable[OutboxStatus]]


def make_event(
    event_id: str = "event-1", *, ordering_key: str | None = None
) -> OutboxEvent:
    now = datetime.now(UTC)
    return OutboxEvent(
        event_id=event_id,
        topic="durable.outbox.outputs",
        payload=b'{"ok":true}',
        key=b"model-run-1",
        headers={"content-type": b"application/json"},
        created_at=now,
        expires_at=now + timedelta(minutes=15),
        ordering_key=ordering_key,
    )


@dataclass(frozen=True, slots=True)
class ProviderContract:
    store_factory: Callable[[], DurableOutboxStore]
    status_of: StatusHook | None = None
    cleanup: CleanupHook | None = None


async def run_basic_provider_contract(
    store_factory: Callable[[], DurableOutboxStore],
) -> None:
    from durable_outbox.core.dispatcher import OutboxDispatcher

    store = store_factory()
    event = make_event()
    first = await store.put(event)
    second = await store.put(event)
    assert first.event_id == second.event_id

    claimed = await store.claim_batch(limit=10)
    assert len(claimed) == 1
    assert await store.claim_batch(limit=10) == []

    dispatcher = OutboxDispatcher(store, FakeSink())
    await store.mark_pending_after_retryable_failure(
        claimed[0],
        error_type="TimeoutError",
        error_message="retry",
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    summary = await dispatcher.run_once(limit=10)
    assert summary.sent == 1
    assert await store.claim_batch(limit=10) == []


async def run_provider_contract(contract: ProviderContract) -> None:
    await run_basic_provider_contract(contract.store_factory)
