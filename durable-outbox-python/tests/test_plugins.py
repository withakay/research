from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import EntryPoint
from typing import Any

import pytest

from durable_outbox.core import ConfigurationError
from durable_outbox.core.sink import MessageSink
from durable_outbox.core.store import DurableOutboxStore
from durable_outbox.plugins import (
    available_sinks,
    available_stores,
    load_sink,
    load_store,
)
from durable_outbox.testing import FakeOutboxStore, FakeSink


@dataclass(slots=True)
class EntryPointGroups:
    stores: tuple[EntryPoint, ...] = ()
    sinks: tuple[EntryPoint, ...] = ()

    def select(self, *, group: str) -> tuple[EntryPoint, ...]:
        if group == "durable_outbox.stores":
            return self.stores
        if group == "durable_outbox.sinks":
            return self.sinks
        return ()


def create_sink(config: dict[str, Any]) -> MessageSink:
    assert config == {"path": "events.jsonl"}
    return FakeSink()


def create_store(config: dict[str, Any]) -> DurableOutboxStore:
    assert config == {"name": "memory"}
    return FakeOutboxStore()


def create_invalid_sink(config: dict[str, Any]) -> object:
    _ = config
    return object()


def duplicate_sink(config: dict[str, Any]) -> MessageSink:
    _ = config
    return FakeSink()


def _entry_point(name: str, value: str, *, group: str) -> EntryPoint:
    return EntryPoint(name=name, value=value, group=group)


def test_available_plugins_are_read_from_entry_point_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    groups = EntryPointGroups(
        stores=(
            _entry_point(
                "memory",
                "tests.test_plugins:create_store",
                group="durable_outbox.stores",
            ),
        ),
        sinks=(
            _entry_point(
                "file",
                "tests.test_plugins:create_sink",
                group="durable_outbox.sinks",
            ),
        ),
    )
    monkeypatch.setattr("durable_outbox.plugins.entry_points", lambda: groups)

    assert available_stores() == ("memory",)
    assert available_sinks() == ("file",)


def test_load_sink_and_store_from_named_plugins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    groups = EntryPointGroups(
        stores=(
            _entry_point(
                "memory",
                "tests.test_plugins:create_store",
                group="durable_outbox.stores",
            ),
        ),
        sinks=(
            _entry_point(
                "file",
                "tests.test_plugins:create_sink",
                group="durable_outbox.sinks",
            ),
        ),
    )
    monkeypatch.setattr("durable_outbox.plugins.entry_points", lambda: groups)

    assert isinstance(load_sink("file", {"path": "events.jsonl"}), MessageSink)
    assert isinstance(load_store("memory", {"name": "memory"}), DurableOutboxStore)


def test_load_plugin_errors_name_missing_and_invalid_plugins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    groups = EntryPointGroups(
        sinks=(
            _entry_point(
                "invalid",
                "tests.test_plugins:create_invalid_sink",
                group="durable_outbox.sinks",
            ),
            _entry_point(
                "duplicate",
                "tests.test_plugins:create_sink",
                group="durable_outbox.sinks",
            ),
            _entry_point(
                "duplicate",
                "tests.test_plugins:duplicate_sink",
                group="durable_outbox.sinks",
            ),
        ),
    )
    monkeypatch.setattr("durable_outbox.plugins.entry_points", lambda: groups)

    with pytest.raises(ConfigurationError, match="missing"):
        load_store("missing", {})
    with pytest.raises(ConfigurationError, match="invalid"):
        load_sink("invalid", {})
    with pytest.raises(ConfigurationError, match="duplicate"):
        load_sink("duplicate", {})
