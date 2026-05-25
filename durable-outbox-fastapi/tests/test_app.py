import json
from datetime import datetime
from typing import Any

import pytest
from durable_outbox.core import OutboxDispatcher
from durable_outbox.core.capabilities import OutboxCapabilities
from durable_outbox.core.model import (
    AcceptedReceipt,
    ClaimedEvent,
    OutboxEvent,
    PublishResult,
)
from httpx import ASGITransport, AsyncClient

from durable_outbox_fastapi.app import create_app
from durable_outbox_fastapi.service import PublisherService


class Store:
    capabilities = OutboxCapabilities(
        store_name="test-store",
        rpo_zero_for_accepted_events=False,
        supports_ordering=False,
        supports_failover_replay=False,
        supports_ttl_freeze=False,
    )

    def __init__(self) -> None:
        self.events: list[OutboxEvent] = []

    async def put(self, event: OutboxEvent) -> AcceptedReceipt:
        self.events.append(event)
        return AcceptedReceipt(
            event_id=event.event_id,
            accepted_at=event.created_at,
            rpo_zero=False,
            store="test-store",
        )

    async def claim_batch(self, *, limit: int) -> list[ClaimedEvent]:
        _ = limit
        return [
            ClaimedEvent(event=event, claim_token=f"claim-{event.event_id}")
            for event in self.events
        ]

    async def mark_sent(self, claimed: ClaimedEvent, result: PublishResult) -> None:
        _ = claimed, result

    async def mark_pending_after_retryable_failure(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
        next_attempt_at: datetime,
    ) -> None:
        _ = claimed, error_type, error_message, next_attempt_at

    async def mark_failed(
        self,
        claimed: ClaimedEvent,
        *,
        error_type: str,
        error_message: str,
    ) -> None:
        _ = claimed, error_type, error_message

    async def failover_replay_candidates(
        self,
        *,
        failover_started_at: datetime,
        limit: int,
    ) -> list[ClaimedEvent]:
        _ = failover_started_at, limit
        return []

    async def freeze_cleanup(self, *, reason: str) -> None:
        _ = reason

    async def resume_cleanup(self) -> None:
        return None


class Sink:
    def __init__(self) -> None:
        self.events: list[OutboxEvent] = []

    async def publish(self, event: OutboxEvent) -> PublishResult:
        self.events.append(event)
        return PublishResult(
            partition=0,
            offset=len(self.events) - 1,
            published_at=event.created_at,
        )


def test_json_payload_is_canonicalized_for_stable_bytes() -> None:
    service = PublisherService(
        store=Store(),
        dispatcher=OutboxDispatcher(Store(), Sink()),
    )

    encoded = service.encode_payload({"b": 2, "a": 1})

    assert encoded == b'{"a":1,"b":2}'


@pytest.mark.asyncio
async def test_post_message_persists_and_dispatches_payload() -> None:
    store = Store()
    sink = Sink()
    service = PublisherService(
        store=store,
        dispatcher=OutboxDispatcher(store, sink),
    )

    app = create_app()
    app.state.publisher = service

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/topics/orders/messages",
            json={"order_id": "order-1"},
            headers={"x-message-key": "order-1"},
        )

    body: dict[str, Any] = response.json()
    assert response.status_code == 202
    assert body["topic"] == "orders"
    assert body["dispatch"]["sent"] == 1
    assert store.events[0].topic == "orders"
    assert (
        store.events[0].payload
        == json.dumps(
            {"order_id": "order-1"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )
    assert store.events[0].key == b"order-1"
    assert sink.events[0].event_id == body["event_id"]
