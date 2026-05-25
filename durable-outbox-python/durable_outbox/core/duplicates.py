from collections.abc import Mapping
from typing import Any

from durable_outbox.core.errors import DuplicateEventConflictError
from durable_outbox.core.model import OutboxEvent

_FIELDS = (
    "topic",
    "payload",
    "key",
    "headers",
    "ordering_key",
    "ordering_sequence",
    "publishing_mode",
    "schema_id",
    "schema_version",
)


def raise_if_incompatible_duplicate(
    existing: OutboxEvent, incoming: OutboxEvent
) -> None:
    field_name = first_event_difference(existing, incoming)
    if field_name is None:
        return
    raise DuplicateEventConflictError(
        f"event_id {incoming.event_id!r} already exists with incompatible "
        f"{field_name}: stored={_redact(getattr(existing, field_name))} "
        f"incoming={_redact(getattr(incoming, field_name))}"
    )


def first_event_difference(existing: OutboxEvent, incoming: OutboxEvent) -> str | None:
    for field_name in _FIELDS:
        existing_value = getattr(existing, field_name)
        incoming_value = getattr(incoming, field_name)
        if field_name == "headers":
            existing_value = dict(existing_value)
            incoming_value = dict(incoming_value)
        if existing_value != incoming_value:
            return field_name
    return None


def _redact(value: Any) -> str:
    if isinstance(value, bytes):
        return f"<bytes:{len(value)}>"
    if isinstance(value, Mapping):
        return f"<mapping:{len(value)}>"
    text = repr(value)
    if len(text) > 64:
        return text[:61] + "..."
    return text
