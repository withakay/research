from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

from durable_outbox.core.errors import ConfigurationError
from durable_outbox_file_sink.sink import FileSink

if TYPE_CHECKING:
    from collections.abc import Mapping

    from durable_outbox.core.time import Clock

__all__ = ["FileSink", "build_file_sink"]


def build_file_sink(config: Mapping[str, object]) -> FileSink:
    """Build a `FileSink` from durable outbox plugin configuration."""

    path = _required_path(config, "path")
    clock = config.get("clock")
    return FileSink(
        path,
        clock=cast("Clock | None", clock),
        fsync=_optional_bool(config, "fsync", default=False),
        fsync_interval_events=_optional_int(
            config,
            "fsync_interval_events",
            default=1,
        ),
        fsync_interval_ms=_optional_int_or_none(config, "fsync_interval_ms"),
    )


def _required_path(config: Mapping[str, object], name: str) -> Path:
    value = config.get(name)
    if isinstance(value, str | Path):
        return Path(value)
    raise ConfigurationError(f"file sink plugin requires string path config {name!r}")


def _optional_bool(
    config: Mapping[str, object],
    name: str,
    *,
    default: bool,
) -> bool:
    value = config.get(name, default)
    if isinstance(value, bool):
        return value
    raise ConfigurationError(f"file sink plugin config {name!r} must be a bool")


def _optional_int(
    config: Mapping[str, object],
    name: str,
    *,
    default: int,
) -> int:
    value = config.get(name, default)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise ConfigurationError(f"file sink plugin config {name!r} must be an int")


def _optional_int_or_none(config: Mapping[str, object], name: str) -> int | None:
    value = config.get(name)
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise ConfigurationError(f"file sink plugin config {name!r} must be an int")
