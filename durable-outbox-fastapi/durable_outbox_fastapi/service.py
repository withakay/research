import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from durable_outbox.core import DispatchSummary, OutboxDispatcher, OutboxEvent
from durable_outbox.core.model import AcceptedReceipt
from durable_outbox.core.store import DurableOutboxStore


@dataclass(frozen=True, slots=True)
class PublishResponse:
    event_id: str
    topic: str
    accepted_at: datetime
    store: str
    rpo_zero: bool
    dispatch: Mapping[str, int]


class PublisherService:
    def __init__(
        self,
        *,
        store: DurableOutboxStore,
        dispatcher: OutboxDispatcher,
        default_ttl: timedelta = timedelta(minutes=15),
    ) -> None:
        self.store = store
        self.dispatcher = dispatcher
        self.default_ttl = default_ttl

    async def publish(
        self,
        *,
        topic: str,
        payload: Any,
        key: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> PublishResponse:
        now = datetime.now(UTC)
        event = OutboxEvent(
            event_id=str(uuid4()),
            topic=topic,
            payload=self.encode_payload(payload),
            key=key.encode("utf-8") if key is not None else None,
            headers={
                name: value.encode("utf-8")
                for name, value in (
                    headers or {"content-type": "application/json"}
                ).items()
            },
            created_at=now,
            expires_at=now + self.default_ttl,
        )
        receipt = await self._put(event)
        summary = await self.dispatcher.run_once(limit=100)
        return _response(event, receipt, summary)

    def encode_payload(self, payload: Any) -> bytes:
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    async def _put(self, event: OutboxEvent) -> AcceptedReceipt:
        receipt = await self.store.put(event)
        if not isinstance(receipt, AcceptedReceipt):
            raise TypeError("store.put returned an invalid receipt")
        return receipt


def _response(
    event: OutboxEvent,
    receipt: AcceptedReceipt,
    summary: DispatchSummary,
) -> PublishResponse:
    return PublishResponse(
        event_id=event.event_id,
        topic=event.topic,
        accepted_at=receipt.accepted_at,
        store=receipt.store,
        rpo_zero=receipt.rpo_zero,
        dispatch={
            "claimed": summary.claimed,
            "sent": summary.sent,
            "retried": summary.retried,
            "failed": summary.failed,
            "store_update_failed": summary.store_update_failed,
        },
    )
