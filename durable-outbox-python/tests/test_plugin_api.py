from __future__ import annotations

from importlib import import_module
from importlib.metadata import EntryPoint
from typing import TYPE_CHECKING

import pytest

from durable_outbox import available_sinks, available_stores, load_sink, load_store
from durable_outbox.core.errors import ConfigurationError
from durable_outbox.testing.provider_contract import make_event
from durable_outbox_blob_store import BlobOutboxStore
from durable_outbox_cosmos_store import CosmosStrongOutboxStore
from durable_outbox_file_sink import FileSink
from durable_outbox_memory_store import MemoryOutboxStore
from durable_outbox_sql_store import InMemorySqlOutboxClient

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path


def test_installed_provider_entry_points_are_discoverable() -> None:
    assert set(available_sinks()) >= {"file", "kafka"}
    assert set(available_stores()) >= {
        "azure-sql-sync",
        "blob",
        "cosmos",
        "dual-region-blob",
        "memory",
        "sql-always-on",
    }


@pytest.mark.asyncio
async def test_load_file_sink_from_plugin_entry_point(tmp_path: Path) -> None:
    sink = load_sink("file", {"path": tmp_path / "published.jsonl"})
    assert isinstance(sink, FileSink)
    event = make_event("plugin-file-sink")

    result = await sink.publish(event)
    await sink.aclose()

    assert result.partition == 0
    assert result.offset == 0
    assert (tmp_path / "published.jsonl").is_file()


def test_load_sql_stores_from_plugin_entry_points() -> None:
    azure = load_store("azure-sql-sync", {"client": InMemorySqlOutboxClient()})
    always_on = load_store(
        "sql-always-on",
        {"client": InMemorySqlOutboxClient(), "required_synchronized_secondaries": 1},
    )

    assert azure.capabilities.store_name == "AzureSqlSyncOutboxStore"
    assert always_on.capabilities.store_name == "SqlAlwaysOnOutboxStore"


def test_load_memory_store_from_plugin_entry_point() -> None:
    store = load_store("memory")

    assert isinstance(store, MemoryOutboxStore)


def test_load_blob_store_from_plugin_entry_point() -> None:
    store = load_store("blob", {"client": {}})

    assert isinstance(store, BlobOutboxStore)


def test_load_cosmos_store_from_plugin_entry_point() -> None:
    store = load_store("cosmos", {"client": {}})

    assert isinstance(store, CosmosStrongOutboxStore)


def test_missing_plugin_raises_configuration_error() -> None:
    with pytest.raises(ConfigurationError, match="missing durable outbox store plugin"):
        load_store("missing-store")


def test_duplicate_plugin_raises_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import durable_outbox.plugins as plugins

    entry_points = (
        EntryPoint(
            name="duplicate",
            value="durable_outbox_file_sink:build_file_sink",
            group=plugins.SINK_ENTRY_POINT_GROUP,
        ),
        EntryPoint(
            name="duplicate",
            value="durable_outbox_file_sink:build_file_sink",
            group=plugins.SINK_ENTRY_POINT_GROUP,
        ),
    )

    monkeypatch.setattr(
        plugins,
        "_entry_points",
        lambda group: entry_points if group == plugins.SINK_ENTRY_POINT_GROUP else (),
    )

    with pytest.raises(
        ConfigurationError, match="duplicate durable outbox sink plugin"
    ):
        load_sink("duplicate", {"path": "events.jsonl"})


def test_invalid_plugin_factory_result_raises_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import durable_outbox.plugins as plugins

    entry_points: Sequence[EntryPoint] = (
        EntryPoint(
            name="invalid",
            value="durable_outbox.testing.clock:FixedClock",
            group=plugins.SINK_ENTRY_POINT_GROUP,
        ),
    )
    monkeypatch.setattr(
        plugins,
        "_entry_points",
        lambda group: entry_points if group == plugins.SINK_ENTRY_POINT_GROUP else (),
    )

    with pytest.raises(ConfigurationError, match="object missing publish"):
        load_sink("invalid", {"path": "events.jsonl"})


def test_extracted_provider_modules_are_not_core_compatibility_modules() -> None:
    with pytest.raises(ModuleNotFoundError):
        import_module("durable_outbox.sinks.file")
    with pytest.raises(ModuleNotFoundError):
        import_module("durable_outbox.sinks.kafka")
    with pytest.raises(ModuleNotFoundError):
        import_module("durable_outbox.stores.azure_blob")
    with pytest.raises(ModuleNotFoundError):
        import_module("durable_outbox.stores.blob_geo")
    with pytest.raises(ModuleNotFoundError):
        import_module("durable_outbox.stores.cosmos")
    with pytest.raises(ModuleNotFoundError):
        import_module("durable_outbox.stores.cosmos_azure")
    with pytest.raises(ModuleNotFoundError):
        import_module("durable_outbox.stores.memory")
    with pytest.raises(ModuleNotFoundError):
        import_module("durable_outbox.stores.sql")
    with pytest.raises(ModuleNotFoundError):
        import_module("durable_outbox.stores.sql_pyodbc")
