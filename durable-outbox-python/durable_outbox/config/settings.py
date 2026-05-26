from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from os import environ
from typing import TYPE_CHECKING

from durable_outbox.core.cleanup import CleanupPolicy
from durable_outbox.core.errors import ConfigurationError
from durable_outbox.core.validation import require_positive_limit

if TYPE_CHECKING:
    from collections.abc import Mapping

_ENVIRONMENT = "DURABLE_OUTBOX_ENVIRONMENT"
_DISPATCHER_LIMIT = "DURABLE_OUTBOX_DISPATCHER_LIMIT"
_CLAIM_TIMEOUT_SECONDS = "DURABLE_OUTBOX_CLAIM_TIMEOUT_SECONDS"
_CLEANUP_SAFETY_MARGIN_SECONDS = "DURABLE_OUTBOX_CLEANUP_SAFETY_MARGIN_SECONDS"
_CLEANUP_INTERVAL_SECONDS = "DURABLE_OUTBOX_CLEANUP_INTERVAL_SECONDS"
_CLEANUP_BATCH_SIZE = "DURABLE_OUTBOX_CLEANUP_BATCH_SIZE"
_CLEANUP_MAX_PER_TICK = "DURABLE_OUTBOX_CLEANUP_MAX_PER_TICK"


@dataclass(frozen=True, slots=True)
class OutboxSettings:
    """Host-level defaults for dispatcher and cleanup wiring."""

    environment: str = "local"
    dispatcher_limit: int = 100
    claim_timeout: timedelta = timedelta(minutes=5)
    cleanup_safety_margin: timedelta = timedelta(minutes=5)
    cleanup_interval: timedelta = timedelta(minutes=1)
    cleanup_batch_size: int | None = 100
    cleanup_max_per_tick: int | None = 1000

    @classmethod
    def from_env(cls, values: Mapping[str, str] | None = None) -> OutboxSettings:
        source = environ if values is None else values
        defaults = cls()
        return cls(
            environment=source.get(_ENVIRONMENT, defaults.environment),
            dispatcher_limit=_env_positive_int(
                source,
                _DISPATCHER_LIMIT,
                defaults.dispatcher_limit,
            ),
            claim_timeout=timedelta(
                seconds=_env_positive_int(
                    source,
                    _CLAIM_TIMEOUT_SECONDS,
                    int(defaults.claim_timeout.total_seconds()),
                )
            ),
            cleanup_safety_margin=timedelta(
                seconds=_env_positive_int(
                    source,
                    _CLEANUP_SAFETY_MARGIN_SECONDS,
                    int(defaults.cleanup_safety_margin.total_seconds()),
                )
            ),
            cleanup_interval=timedelta(
                seconds=_env_positive_int(
                    source,
                    _CLEANUP_INTERVAL_SECONDS,
                    int(defaults.cleanup_interval.total_seconds()),
                )
            ),
            cleanup_batch_size=_env_optional_positive_int(
                source,
                _CLEANUP_BATCH_SIZE,
                defaults.cleanup_batch_size,
            ),
            cleanup_max_per_tick=_env_optional_positive_int(
                source,
                _CLEANUP_MAX_PER_TICK,
                defaults.cleanup_max_per_tick,
            ),
        )

    def __post_init__(self) -> None:
        require_positive_limit(self.dispatcher_limit, field_name="dispatcher_limit")
        if self.claim_timeout <= timedelta(0):
            raise ConfigurationError("claim_timeout must be positive")
        if self.cleanup_safety_margin < timedelta(0):
            raise ConfigurationError("cleanup_safety_margin cannot be negative")
        CleanupPolicy(
            sent_safety_margin=self.cleanup_safety_margin,
            interval=self.cleanup_interval,
            batch_size=self.cleanup_batch_size,
            max_per_tick=self.cleanup_max_per_tick,
        )

    def cleanup_policy(self) -> CleanupPolicy:
        return CleanupPolicy(
            sent_safety_margin=self.cleanup_safety_margin,
            interval=self.cleanup_interval,
            batch_size=self.cleanup_batch_size,
            max_per_tick=self.cleanup_max_per_tick,
        )


def _env_positive_int(
    source: Mapping[str, str],
    name: str,
    default: int,
) -> int:
    raw = source.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    require_positive_limit(value, field_name=name)
    return value


def _env_optional_positive_int(
    source: Mapping[str, str],
    name: str,
    default: int | None,
) -> int | None:
    raw = source.get(name)
    if raw is None:
        return default
    if raw == "":
        return None
    return _env_positive_int(source, name, default or 1)
