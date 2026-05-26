from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, cast

from durable_outbox.core.errors import ConfigurationError
from durable_outbox_memory_store.store import (
    CleanupFreezeState,
    MemoryOutboxStore,
    StoredEvent,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from durable_outbox.core.time import Clock

__all__ = [
    "CleanupFreezeState",
    "MemoryOutboxStore",
    "StoredEvent",
    "build_memory_store",
]


def build_memory_store(config: Mapping[str, object] | None = None) -> MemoryOutboxStore:
    """Build an in-memory store from durable outbox plugin configuration."""

    values = config or {}
    return MemoryOutboxStore(
        claim_timeout=_optional_timedelta(values, "claim_timeout"),
        cleanup_state=cast("CleanupFreezeState | None", values.get("cleanup_state")),
        clock=cast("Clock | None", values.get("clock")),
    )


def _optional_timedelta(
    config: Mapping[str, object],
    name: str,
) -> timedelta:
    value = config.get(name)
    if value is None:
        return timedelta(minutes=5)
    if isinstance(value, timedelta):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return timedelta(seconds=value)
    raise ConfigurationError(
        f"memory store plugin config {name!r} must be a timedelta or seconds int"
    )
