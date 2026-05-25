from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

from durable_outbox.core.admin import AdminActionStatus
from durable_outbox.core.errors import DuplicateEventConflictError
from durable_outbox.core.model import (
    OutboxEvent,
    OutboxStatus,
    PublishingMode,
    PublishResult,
)
from durable_outbox.core.store import DurableOutboxStore
from durable_outbox.testing.fake_sink import FakeSink

type CleanupHook = Callable[[DurableOutboxStore, datetime], Awaitable[int]]
type StatusHook = Callable[[DurableOutboxStore, str], Awaitable[OutboxStatus]]
type StoreFactory = Callable[[], DurableOutboxStore]


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


def make_ordered_event(
    event_id: str,
    *,
    ordering_key: str = "customer-1",
    ordering_sequence: int = 0,
) -> OutboxEvent:
    return replace(
        make_event(event_id, ordering_key=ordering_key),
        publishing_mode=PublishingMode.ORDERED,
        ordering_sequence=ordering_sequence,
    )


@dataclass(frozen=True, slots=True)
class ProviderContract:
    """Reusable provider behavior matrix for downstream store adapters."""

    store_factory: StoreFactory
    status_of: StatusHook | None = None
    cleanup: CleanupHook | None = None


async def run_basic_provider_contract(
    store_factory: StoreFactory,
) -> None:
    """Run the historical smoke contract for compatibility with older tests."""
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

    assert (
        await store.replay_event(event_id=event.event_id) is AdminActionStatus.SUCCESS
    )
    replay_claim = await store.claim_batch(limit=10)
    assert [claim.event.event_id for claim in replay_claim] == [event.event_id]
    await store.mark_sent(
        replay_claim[0],
        PublishResult(partition=0, offset=1, published_at=datetime.now(UTC)),
    )
    assert await store.replay_event(event_id="missing") is AdminActionStatus.NOT_FOUND

    failed = make_event("failed-contract")
    await store.put(failed)
    failed_claim = (await store.claim_batch(limit=10))[0]
    await store.mark_failed(
        failed_claim,
        error_type="Fatal",
        error_message="stop",
    )
    assert (
        await store.repair_failed_to_pending(event_id=failed.event_id)
        is AdminActionStatus.SUCCESS
    )
    assert (
        await store.repair_failed_to_pending(event_id="missing")
        is AdminActionStatus.NOT_FOUND
    )
    assert (
        await store.repair_failed_to_pending(event_id=event.event_id)
        is AdminActionStatus.WRONG_STATE
    )
    repaired_claim = await store.claim_batch(limit=10)
    assert [claim.event.event_id for claim in repaired_claim] == [failed.event_id]


async def assert_provider_put_is_idempotent_for_compatible_duplicate(
    store_factory: StoreFactory,
) -> None:
    store = store_factory()
    event = make_event("contract-compatible-duplicate")

    first = await store.put(event)
    second = await store.put(event)

    assert second.event_id == first.event_id
    assert second.accepted_at == first.accepted_at


async def assert_provider_put_rejects_incompatible_duplicate(
    store_factory: StoreFactory,
) -> None:
    store = store_factory()
    event = make_event("contract-incompatible-duplicate")
    incompatible = replace(event, topic="other-topic")
    await store.put(event)

    try:
        await store.put(incompatible)
    except DuplicateEventConflictError:
        return
    raise AssertionError("provider accepted incompatible duplicate event_id")


async def assert_provider_claim_retry_sent_failed_and_failover_replay(
    store_factory: StoreFactory,
) -> None:
    store = store_factory()
    now = datetime.now(UTC)
    retryable = make_event("contract-retryable")
    failed = make_event("contract-failed")
    in_flight = make_event("contract-in-flight")
    sent = make_event("contract-sent")
    expired = replace(
        make_event("contract-expired"),
        created_at=now - timedelta(hours=2),
        expires_at=now - timedelta(hours=1),
    )
    await store.put(retryable)
    await store.put(failed)
    await store.put(in_flight)
    await store.put(sent)
    await store.put(expired)

    claimed = await store.claim_batch(limit=5)
    claims_by_id = {claim.event.event_id: claim for claim in claimed}
    assert set(claims_by_id) == {
        "contract-retryable",
        "contract-failed",
        "contract-in-flight",
        "contract-sent",
        "contract-expired",
    }
    await store.mark_pending_after_retryable_failure(
        claims_by_id[retryable.event_id],
        error_type="Retryable",
        error_message="try again",
        next_attempt_at=now - timedelta(seconds=1),
    )
    await store.mark_failed(
        claims_by_id[failed.event_id],
        error_type="Fatal",
        error_message="stop",
    )
    await store.mark_sent(
        claims_by_id[sent.event_id],
        PublishResult(partition=1, offset=2, published_at=now),
    )

    replay = await store.failover_replay_candidates(
        failover_started_at=now,
        limit=10,
    )
    assert {claim.event.event_id for claim in replay} == {
        retryable.event_id,
        in_flight.event_id,
        sent.event_id,
    }


async def assert_provider_ordered_claims_block_same_key(
    store_factory: StoreFactory,
) -> None:
    store = store_factory()
    if not store.capabilities.supports_ordering:
        return
    first = make_ordered_event(
        "contract-ordered-first",
        ordering_key="customer-1",
        ordering_sequence=1,
    )
    second = make_ordered_event(
        "contract-ordered-second",
        ordering_key="customer-1",
        ordering_sequence=2,
    )
    await store.put(second)
    await store.put(first)

    claimed = await store.claim_batch(limit=10)
    assert [claim.event.event_id for claim in claimed] == [first.event_id]
    assert await store.claim_batch(limit=10) == []

    await store.mark_sent(
        claimed[0],
        PublishResult(partition=1, offset=2, published_at=datetime.now(UTC)),
    )
    next_claim = await store.claim_batch(limit=10)
    assert [claim.event.event_id for claim in next_claim] == [second.event_id]


async def assert_provider_cleanup_freeze_blocks_cleanup(
    store_factory: StoreFactory,
) -> None:
    store = store_factory()
    event = make_event("contract-cleanup")
    await store.put(event)
    claimed = (await store.claim_batch(limit=1))[0]
    await store.mark_sent(
        claimed,
        PublishResult(partition=1, offset=2, published_at=datetime.now(UTC)),
    )

    cleanup_now = event.expires_at + timedelta(hours=1)
    await store.freeze_cleanup(reason="contract replay")
    deleted = await store.cleanup_sent(
        now=cleanup_now,
        safety_margin=timedelta(seconds=0),
    )
    assert deleted == 0

    await store.resume_cleanup()
    deleted = await store.cleanup_sent(
        now=cleanup_now,
        safety_margin=timedelta(seconds=0),
    )
    assert deleted == 1


async def assert_provider_admin_actions_report_statuses(
    store_factory: StoreFactory,
) -> None:
    store = store_factory()
    pending = make_event("contract-wrong-state-repair")
    await store.put(pending)
    assert (
        await store.repair_failed_to_pending(event_id="contract-missing")
        is AdminActionStatus.NOT_FOUND
    )
    assert (
        await store.repair_failed_to_pending(event_id=pending.event_id)
        is AdminActionStatus.WRONG_STATE
    )
    assert (
        await store.replay_event(event_id="contract-missing")
        is AdminActionStatus.NOT_FOUND
    )


async def assert_provider_repair_resets_failed_state(
    store_factory: StoreFactory,
) -> None:
    store = store_factory()
    event = make_event("contract-repair-reset")
    await store.put(event)
    claimed = (await store.claim_batch(limit=1))[0]
    await store.mark_pending_after_retryable_failure(
        claimed,
        error_type="Retryable",
        error_message="try again",
        next_attempt_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    retry_claim = (
        await store.failover_replay_candidates(
            failover_started_at=datetime.now(UTC),
            limit=1,
        )
    )[0]
    await store.mark_failed(
        retry_claim,
        error_type="Fatal",
        error_message="stop",
    )

    assert (
        await store.repair_failed_to_pending(event_id=event.event_id)
        is AdminActionStatus.SUCCESS
    )
    repaired = await store.claim_batch(limit=1)
    assert [claim.event.event_id for claim in repaired] == [event.event_id]
    assert repaired[0].attempt_count == 1


async def assert_provider_replay_requeues_sent_event(
    store_factory: StoreFactory,
) -> None:
    store = store_factory()
    event = make_event("contract-manual-replay")
    await store.put(event)
    claimed = (await store.claim_batch(limit=1))[0]
    await store.mark_sent(
        claimed,
        PublishResult(partition=1, offset=2, published_at=datetime.now(UTC)),
    )

    assert (
        await store.replay_event(event_id=event.event_id) is AdminActionStatus.SUCCESS
    )
    reclaimed = await store.claim_batch(limit=1)
    assert [claim.event.event_id for claim in reclaimed] == [event.event_id]
    assert reclaimed[0].attempt_count == 2


async def run_provider_contract(contract: ProviderContract) -> None:
    """Run the full reusable provider matrix on fresh store instances."""
    await assert_provider_put_is_idempotent_for_compatible_duplicate(
        contract.store_factory
    )
    await assert_provider_put_rejects_incompatible_duplicate(contract.store_factory)
    await assert_provider_claim_retry_sent_failed_and_failover_replay(
        contract.store_factory
    )
    await assert_provider_ordered_claims_block_same_key(contract.store_factory)
    await assert_provider_cleanup_freeze_blocks_cleanup(contract.store_factory)
    await assert_provider_admin_actions_report_statuses(contract.store_factory)
    await assert_provider_repair_resets_failed_state(contract.store_factory)
    await assert_provider_replay_requeues_sent_event(contract.store_factory)


__all__ = [
    "ProviderContract",
    "assert_provider_admin_actions_report_statuses",
    "assert_provider_claim_retry_sent_failed_and_failover_replay",
    "assert_provider_cleanup_freeze_blocks_cleanup",
    "assert_provider_ordered_claims_block_same_key",
    "assert_provider_put_is_idempotent_for_compatible_duplicate",
    "assert_provider_put_rejects_incompatible_duplicate",
    "assert_provider_repair_resets_failed_state",
    "assert_provider_replay_requeues_sent_event",
    "make_event",
    "make_ordered_event",
    "run_basic_provider_contract",
    "run_provider_contract",
]
