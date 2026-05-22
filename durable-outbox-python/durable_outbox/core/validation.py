from datetime import datetime

from durable_outbox.core.capabilities import OutboxCapabilities
from durable_outbox.core.errors import ValidationError
from durable_outbox.core.model import OutboxEvent


def require_aware_datetime(value: datetime, *, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValidationError(f"{field_name} must be timezone-aware")


def require_positive_limit(limit: int, *, field_name: str = "limit") -> None:
    if limit < 1:
        raise ValidationError(f"{field_name} must be positive")


def enforce_payload_size(
    event: OutboxEvent,
    capabilities: OutboxCapabilities,
) -> None:
    maximum = capabilities.max_payload_bytes
    if maximum is not None and len(event.payload) > maximum:
        raise ValidationError(
            f"payload exceeds {capabilities.store_name} max_payload_bytes={maximum}"
        )
