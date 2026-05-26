from __future__ import annotations

from importlib.metadata import EntryPoint, entry_points
from typing import TYPE_CHECKING, Protocol, cast

from durable_outbox.core.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from durable_outbox.core.sink import MessageSink
    from durable_outbox.core.store import DurableOutboxStore

STORE_ENTRY_POINT_GROUP = "durable_outbox.stores"
SINK_ENTRY_POINT_GROUP = "durable_outbox.sinks"

type PluginConfig = Mapping[str, object]


class StoreFactory(Protocol):
    """Factory loaded from a store plugin entry point."""

    def __call__(self, config: PluginConfig) -> DurableOutboxStore: ...


class SinkFactory(Protocol):
    """Factory loaded from a sink plugin entry point."""

    def __call__(self, config: PluginConfig) -> MessageSink: ...


def available_stores() -> tuple[str, ...]:
    """Return installed store plugin names without importing implementations."""

    return _available(STORE_ENTRY_POINT_GROUP)


def available_sinks() -> tuple[str, ...]:
    """Return installed sink plugin names without importing implementations."""

    return _available(SINK_ENTRY_POINT_GROUP)


def load_store(
    name: str,
    config: PluginConfig | None = None,
) -> DurableOutboxStore:
    """Load a named durable outbox store from package entry points."""

    factory = cast(
        "StoreFactory",
        _load_factory(STORE_ENTRY_POINT_GROUP, name, plugin_type="store"),
    )
    store = _call_factory(factory, name=name, config=config or {}, plugin_type="store")
    _require_methods(
        store,
        plugin_name=name,
        plugin_type="store",
        methods=(
            "put",
            "claim_batch",
            "mark_sent",
            "mark_pending_after_retryable_failure",
            "mark_failed",
            "failover_replay_candidates",
            "freeze_cleanup",
            "resume_cleanup",
            "cleanup_sent",
            "repair_failed_to_pending",
            "replay_event",
        ),
    )
    return store


def load_sink(
    name: str,
    config: PluginConfig | None = None,
) -> MessageSink:
    """Load a named message sink from package entry points."""

    factory = cast(
        "SinkFactory",
        _load_factory(SINK_ENTRY_POINT_GROUP, name, plugin_type="sink"),
    )
    sink = _call_factory(factory, name=name, config=config or {}, plugin_type="sink")
    _require_methods(
        sink,
        plugin_name=name,
        plugin_type="sink",
        methods=("publish",),
    )
    return sink


def _available(group: str) -> tuple[str, ...]:
    return tuple(sorted({entry_point.name for entry_point in _entry_points(group)}))


def _load_factory(
    group: str,
    name: str,
    *,
    plugin_type: str,
) -> object:
    matches = [
        entry_point for entry_point in _entry_points(group) if entry_point.name == name
    ]
    if not matches:
        msg = f"missing durable outbox {plugin_type} plugin {name!r}"
        raise ConfigurationError(msg)
    if len(matches) > 1:
        msg = f"duplicate durable outbox {plugin_type} plugin {name!r}"
        raise ConfigurationError(msg)
    try:
        return matches[0].load()
    except Exception as exc:
        msg = f"failed to load durable outbox {plugin_type} plugin {name!r}"
        raise ConfigurationError(msg) from exc


def _call_factory[PluginT](
    factory: object,
    *,
    name: str,
    config: PluginConfig,
    plugin_type: str,
) -> PluginT:
    if not callable(factory):
        msg = f"invalid durable outbox {plugin_type} plugin {name!r}: factory is not callable"
        raise ConfigurationError(msg)
    try:
        factory_func = cast("Callable[[dict[str, object]], PluginT]", factory)
        return factory_func(dict(config))
    except ConfigurationError:
        raise
    except TypeError as exc:
        msg = f"durable outbox {plugin_type} plugin {name!r} rejected configuration"
        raise ConfigurationError(msg) from exc


def _require_methods(
    value: object,
    *,
    plugin_name: str,
    plugin_type: str,
    methods: Sequence[str],
) -> None:
    missing = [
        method for method in methods if not callable(getattr(value, method, None))
    ]
    if missing:
        names = ", ".join(missing)
        msg = (
            f"invalid durable outbox {plugin_type} plugin {plugin_name!r}: "
            f"factory returned object missing {names}"
        )
        raise ConfigurationError(msg)


def _entry_points(group: str) -> tuple[EntryPoint, ...]:
    return tuple(entry_points().select(group=group))
