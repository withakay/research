import pytest

from durable_outbox.consumer import EventDeduper
from durable_outbox.consumer.dedupe import dedupe_key
from durable_outbox.testing.provider_contract import make_event


@pytest.mark.asyncio
async def test_event_deduper_skips_replayed_event_id_for_same_topic() -> None:
    deduper = EventDeduper()
    event = make_event("dedupe-me")

    first = await deduper.check(event)
    second = await deduper.check(event)

    assert first.should_process is True
    assert second.should_process is False
    assert second.duplicate is True


def test_dedupe_key_scopes_event_id_by_topic() -> None:
    assert dedupe_key("topic-a", "event-1") != dedupe_key("topic-b", "event-1")
