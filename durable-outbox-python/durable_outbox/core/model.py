import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType

from durable_outbox.core.errors import ValidationError

MAX_HEADER_COUNT = 64
MAX_HEADER_VALUE_BYTES = 8192
TOPIC_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
BLOCKED_HEADER_PREFIXES = (
    "authorization",
    "cookie",
    "set-cookie",
    "proxy-authorization",
    "x-api-key",
    "x-auth-",
)


class OutboxStatus(StrEnum):
    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    SENT = "SENT"
    FAILED = "FAILED"


class PublishingMode(StrEnum):
    ORDERED = "ORDERED"
    UNORDERED = "UNORDERED"


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    event_id: str
    topic: str
    payload: bytes
    key: bytes | None
    headers: Mapping[str, bytes]
    created_at: datetime
    expires_at: datetime
    ordering_key: str | None = None
    ordering_sequence: int | None = None
    publishing_mode: PublishingMode = PublishingMode.UNORDERED
    schema_id: str | None = None
    schema_version: str | None = None

    def __post_init__(self) -> None:
        if not self.event_id:
            raise ValidationError("event_id is required")
        if not TOPIC_PATTERN.fullmatch(self.topic):
            raise ValidationError("topic must match ^[A-Za-z0-9._-]{1,249}$")
        if not isinstance(self.payload, bytes):
            raise ValidationError("payload must be bytes")
        _require_aware_datetime(self.created_at, field_name="created_at")
        _require_aware_datetime(self.expires_at, field_name="expires_at")
        if self.expires_at <= self.created_at:
            raise ValidationError("expires_at must be after created_at")
        if self.publishing_mode is PublishingMode.ORDERED and not self.ordering_key:
            raise ValidationError("ordered events require ordering_key")
        if self.ordering_sequence is not None and self.ordering_sequence < 0:
            raise ValidationError("ordering_sequence must be non-negative")
        object.__setattr__(self, "headers", _freeze_headers(self.headers))

    @property
    def effective_ordering_key(self) -> str | None:
        if self.publishing_mode is PublishingMode.ORDERED:
            return self.ordering_key
        return None


def _freeze_headers(headers: Mapping[str, bytes]) -> Mapping[str, bytes]:
    if len(headers) > MAX_HEADER_COUNT:
        raise ValidationError(
            f"headers cannot contain more than {MAX_HEADER_COUNT} entries"
        )
    frozen: dict[str, bytes] = {}
    for name, value in headers.items():
        if not name:
            raise ValidationError("header names cannot be empty")
        lowered = name.lower()
        if any(lowered.startswith(prefix) for prefix in BLOCKED_HEADER_PREFIXES):
            raise ValidationError(f"header name {name!r} is blocked")
        if not isinstance(value, bytes):
            raise ValidationError("header values must be bytes")
        if len(value) > MAX_HEADER_VALUE_BYTES:
            raise ValidationError(
                f"header {name!r} exceeds {MAX_HEADER_VALUE_BYTES} bytes"
            )
        frozen[name] = value
    return MappingProxyType(frozen)


def _require_aware_datetime(value: datetime, *, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValidationError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class AcceptedReceipt:
    event_id: str
    accepted_at: datetime
    rpo_zero: bool
    store: str
    durability_witness: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ClaimedEvent:
    event: OutboxEvent
    claim_token: str
    attempt_count: int = 1
    source_status: OutboxStatus | None = None


@dataclass(frozen=True, slots=True)
class PublishResult:
    partition: int | None
    offset: int | None
    published_at: datetime
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))
