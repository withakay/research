from datetime import UTC, datetime, timedelta

import pytest

from durable_outbox.core import ClaimConflictError, RetryableStoreError, ValidationError
from durable_outbox.core.dispatcher import _stored_error_message
from durable_outbox.core.model import OutboxEvent
from durable_outbox.stores.blob_geo import (
    BlobOutboxStore,
    blob_metadata,
    cleanup_freeze_blob_name,
)
from durable_outbox.testing.provider_contract import make_event


@pytest.mark.parametrize(
    "error",
    [
        ClaimConflictError("claim token 11111111-2222-3333-4444-555555555555 leaked"),
        RetryableStoreError("owner=11111111-2222-3333-4444-555555555555"),
        ValidationError("bad token 11111111-2222-3333-4444-555555555555"),
    ],
)
def test_claim_token_never_in_stored_error_message(error: BaseException) -> None:
    stored = _stored_error_message(error)

    assert "11111111-2222-3333-4444-555555555555" not in stored.message
    assert "<uuid>" in stored.message


def test_event_rejects_metadata_unsafe_event_id() -> None:
    now = datetime.now(UTC)

    with pytest.raises(ValidationError, match="event_id"):
        OutboxEvent(
            event_id="event\rx-ms-meta-poison",
            topic="topic",
            payload=b"{}",
            key=None,
            headers={},
            created_at=now,
            expires_at=now + timedelta(minutes=1),
        )


def test_blob_store_rejects_metadata_unsafe_environment() -> None:
    with pytest.raises(ValidationError, match="environment"):
        BlobOutboxStore.for_testing(environment="prod\rx-ms-meta-poison")


def test_blob_metadata_rejects_metadata_unsafe_environment() -> None:
    with pytest.raises(ValidationError, match="environment"):
        blob_metadata(make_event("event-1"), environment="prod\rpoison")


@pytest.mark.asyncio
async def test_blob_cleanup_freeze_reason_is_not_written_to_metadata() -> None:
    store = BlobOutboxStore.for_testing()
    reason = "operator note with CR\rkept in content"

    await store.freeze_cleanup(reason=reason)
    frozen = await store._cleanup_is_frozen()
    marker = await store.client.get_blob(cleanup_freeze_blob_name("test"))

    assert frozen is True
    assert store.cleanup_freeze_reason == reason
    assert marker is not None
    assert reason not in marker.metadata.values()
