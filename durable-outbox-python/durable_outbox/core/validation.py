from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from durable_outbox.core.capabilities import OutboxCapabilities
from durable_outbox.core.errors import ValidationError

if TYPE_CHECKING:
    from durable_outbox.core.model import OutboxEvent

MAX_CLAIM_BATCH_LIMIT = 1000
MAX_METADATA_VALUE_CHARS = 1024


def enforce_metadata_safe(value: str, *, field_name: str) -> None:
    if len(value) > MAX_METADATA_VALUE_CHARS:
        raise ValidationError(
            f"{field_name} cannot exceed {MAX_METADATA_VALUE_CHARS} characters"
        )
    if not value.isascii() or any(
        ord(character) < 0x20 or ord(character) == 0x7F for character in value
    ):
        raise ValidationError(
            f"{field_name} must contain only printable ASCII metadata characters"
        )


def require_aware_datetime(value: datetime, *, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValidationError(f"{field_name} must be timezone-aware")


def require_positive_limit(limit: int, *, field_name: str = "limit") -> None:
    if limit < 1:
        raise ValidationError(f"{field_name} must be positive")
    if limit > MAX_CLAIM_BATCH_LIMIT:
        raise ValidationError(
            f"{field_name} must be less than or equal to {MAX_CLAIM_BATCH_LIMIT}"
        )


def enforce_payload_size(
    event: OutboxEvent,
    capabilities: OutboxCapabilities,
) -> None:
    maximum = capabilities.max_payload_bytes
    if maximum is not None and len(event.payload) > maximum:
        raise ValidationError(
            f"payload exceeds {capabilities.store_name} max_payload_bytes={maximum}"
        )
