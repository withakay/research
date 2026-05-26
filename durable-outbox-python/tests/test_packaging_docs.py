from __future__ import annotations

import inspect
import tomllib
from datetime import timedelta
from importlib import import_module
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PROJECT_ROOT.parent
OPTIONAL_DEPENDENCY_FLOORS = {}
EXPECTED_PROVIDER_PACKAGES = {
    "durable-outbox-blob-store": {
        "module": "durable_outbox_blob_store",
        "entry_points": {
            "durable_outbox.stores": {
                "blob": "durable_outbox_blob_store:build_blob_store",
                "dual-region-blob": "durable_outbox_blob_store:build_dual_region_blob_store",
            }
        },
    },
    "durable-outbox-cosmos-store": {
        "module": "durable_outbox_cosmos_store",
        "entry_points": {
            "durable_outbox.stores": {
                "cosmos": "durable_outbox_cosmos_store:build_cosmos_store"
            }
        },
    },
    "durable-outbox-file-sink": {
        "module": "durable_outbox_file_sink",
        "entry_points": {
            "durable_outbox.sinks": {"file": "durable_outbox_file_sink:build_file_sink"}
        },
    },
    "durable-outbox-kafka-sink": {
        "module": "durable_outbox_kafka_sink",
        "entry_points": {
            "durable_outbox.sinks": {
                "kafka": "durable_outbox_kafka_sink:build_kafka_sink"
            }
        },
    },
    "durable-outbox-memory-store": {
        "module": "durable_outbox_memory_store",
        "entry_points": {
            "durable_outbox.stores": {
                "memory": "durable_outbox_memory_store:build_memory_store"
            }
        },
    },
    "durable-outbox-sql-store": {
        "module": "durable_outbox_sql_store",
        "entry_points": {
            "durable_outbox.stores": {
                "azure-sql-sync": "durable_outbox_sql_store:build_azure_sql_sync_store",
                "sql-always-on": "durable_outbox_sql_store:build_sql_always_on_store",
            }
        },
    },
}
EXPECTED_WORKSPACE_MEMBERS = {
    "packages/*",
}
EXPECTED_PROJECT_URLS = {"Homepage", "Documentation", "Repository", "Issues"}
EXPECTED_KEYWORDS = {
    "azure",
    "durable-outbox",
    "kafka",
    "rpo-zero",
    "transactional-outbox",
}
EXPECTED_CLASSIFIERS = {
    "Framework :: AsyncIO",
    "Operating System :: OS Independent",
    "Topic :: Database",
    "Topic :: System :: Distributed Computing",
    "Typing :: Typed",
}


def test_readme_documents_configured_extras_and_verification_commands() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    readme = (PROJECT_ROOT / "README.md").read_text()

    assert not pyproject["project"].get("optional-dependencies")
    assert "durable-outbox[kafka]" not in readme
    assert "durable-outbox[azure]" not in readme
    assert "uv add durable-outbox-file-sink" in readme
    assert "uv add durable-outbox-kafka-sink" in readme
    assert "uv add durable-outbox-memory-store" in readme
    assert "uv add durable-outbox-blob-store" in readme
    assert "uv add durable-outbox-cosmos-store" in readme
    assert "uv add durable-outbox-sql-store" in readme
    assert "uv run pytest" in readme
    assert "aspire run --apphost DurableOutbox.Integration.AppHost" in readme
    assert "uv run ruff check ." in readme
    assert "uv run ty check" in readme
    assert "uv sync --all-packages --group dev" in readme
    assert "uv build --all-packages" in readme


def test_license_file_matches_project_metadata() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    license_text = (PROJECT_ROOT / "LICENSE").read_text()

    assert pyproject["project"]["license"] == "MIT"
    assert "MIT License" in license_text


def test_optional_dependency_floors_match_reviewed_provider_versions() -> None:
    assert OPTIONAL_DEPENDENCY_FLOORS == {}
    provider_dependencies = {
        package_name: set(
            tomllib.loads(
                (
                    PROJECT_ROOT / "packages" / package_name / "pyproject.toml"
                ).read_text()
            )["project"]["dependencies"]
        )
        for package_name in EXPECTED_PROVIDER_PACKAGES
    }

    assert (
        "confluent-kafka>=2.14.0" in provider_dependencies["durable-outbox-kafka-sink"]
    )
    assert {
        "aiohttp>=3.13.5",
        "azure-storage-blob>=12.29.0",
    } <= provider_dependencies["durable-outbox-blob-store"]
    assert (
        "azure-cosmos>=4.15.0" in provider_dependencies["durable-outbox-cosmos-store"]
    )


def test_core_package_uses_uv_build_workspace_metadata() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())

    assert pyproject["build-system"] == {
        "requires": ["uv_build>=0.11.8,<0.12.0"],
        "build-backend": "uv_build",
    }
    assert set(pyproject["tool"]["uv"]["workspace"]["members"]) == (
        EXPECTED_WORKSPACE_MEMBERS
    )
    assert pyproject["tool"]["uv"]["build-backend"]["module-name"] == "durable_outbox"
    assert pyproject["tool"]["uv"]["build-backend"]["module-root"] == ""


def test_provider_plugin_packages_use_uv_build_and_entry_points() -> None:
    for package_name, expected in EXPECTED_PROVIDER_PACKAGES.items():
        package_dir = PROJECT_ROOT / "packages" / package_name
        plugin = tomllib.loads((package_dir / "pyproject.toml").read_text())
        module_name = str(expected["module"])

        assert plugin["project"]["name"] == package_name
        assert plugin["project"]["dependencies"][0] == "durable-outbox"
        assert plugin["build-system"] == {
            "requires": ["uv_build>=0.11.8,<0.12.0"],
            "build-backend": "uv_build",
        }
        assert plugin["project"]["license"] == "MIT"
        assert plugin["project"]["entry-points"] == expected["entry_points"]
        assert plugin["tool"]["uv"]["build-backend"]["module-name"] == module_name
        assert (package_dir / module_name / "py.typed").is_file()


def test_core_package_has_no_concrete_provider_modules_or_extras() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())

    assert "optional-dependencies" not in pyproject["project"]
    for module_name in (
        "durable_outbox.sinks.kafka",
        "durable_outbox.stores.azure_blob",
        "durable_outbox.stores.blob_geo",
        "durable_outbox.stores.cosmos",
        "durable_outbox.stores.cosmos_azure",
        "durable_outbox.stores.memory",
    ):
        with pytest.raises(ModuleNotFoundError):
            import_module(module_name)


def test_project_metadata_describes_package_surface() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    project = pyproject["project"]

    assert set(project["urls"]) == EXPECTED_PROJECT_URLS
    assert set(project["keywords"]) == EXPECTED_KEYWORDS
    assert set(project["classifiers"]) >= EXPECTED_CLASSIFIERS
    assert (PROJECT_ROOT / "durable_outbox" / "py.typed").is_file()


def test_top_level_package_exports_obvious_public_api() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    package = import_module("durable_outbox")
    readme = (PROJECT_ROOT / "README.md").read_text()

    for name in (
        "MessageSink",
        "OutboxDispatcher",
        "OutboxEvent",
        "RetryPolicy",
        "__version__",
        "available_sinks",
        "available_stores",
        "load_sink",
        "load_store",
    ):
        assert name in package.__all__
        assert hasattr(package, name)
    assert package.__version__ == pyproject["project"]["version"]
    assert "from durable_outbox import OutboxDispatcher, OutboxEvent" in readme


def test_public_contracts_have_docstrings() -> None:
    from durable_outbox.config.settings import OutboxSettings
    from durable_outbox.core.capabilities import OutboxCapabilities
    from durable_outbox.core.cleanup import CleanupPolicy
    from durable_outbox.core.dispatcher import DispatchSummary
    from durable_outbox.core.model import (
        AcceptedReceipt,
        ClaimedEvent,
        OutboxEvent,
        PublishResult,
    )
    from durable_outbox.core.retry import RetryPolicy
    from durable_outbox.core.sink import MessageSink
    from durable_outbox.core.store import DurableOutboxStore

    for public_type in (
        AcceptedReceipt,
        ClaimedEvent,
        CleanupPolicy,
        DispatchSummary,
        DurableOutboxStore,
        MessageSink,
        OutboxCapabilities,
        OutboxEvent,
        OutboxSettings,
        PublishResult,
        RetryPolicy,
    ):
        assert inspect.getdoc(public_type)

    for method_name in (
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
    ):
        assert inspect.getdoc(getattr(DurableOutboxStore, method_name))
    assert inspect.getdoc(MessageSink.publish)


def test_outbox_settings_loads_environment_and_builds_cleanup_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from durable_outbox.config.settings import OutboxSettings

    monkeypatch.setenv("DURABLE_OUTBOX_ENVIRONMENT", "prod")
    monkeypatch.setenv("DURABLE_OUTBOX_DISPATCHER_LIMIT", "25")
    monkeypatch.setenv("DURABLE_OUTBOX_CLAIM_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("DURABLE_OUTBOX_CLEANUP_SAFETY_MARGIN_SECONDS", "30")
    monkeypatch.setenv("DURABLE_OUTBOX_CLEANUP_INTERVAL_SECONDS", "10")
    monkeypatch.setenv("DURABLE_OUTBOX_CLEANUP_BATCH_SIZE", "7")
    monkeypatch.setenv("DURABLE_OUTBOX_CLEANUP_MAX_PER_TICK", "11")

    settings = OutboxSettings.from_env()

    assert settings.environment == "prod"
    assert settings.dispatcher_limit == 25
    assert settings.claim_timeout == timedelta(seconds=45)
    assert settings.cleanup_policy().sent_safety_margin == timedelta(seconds=30)
    assert settings.cleanup_policy().interval == timedelta(seconds=10)
    assert settings.cleanup_policy().batch_size == 7
    assert settings.cleanup_policy().max_per_tick == 11


def test_dual_region_blob_store_uses_documented_region_methods() -> None:
    from durable_outbox_blob_store import (
        BlobOutboxStore,
        DualRegionBlobOutboxStore,
    )

    source = inspect.getsource(DualRegionBlobOutboxStore)

    for private_method in (
        "._accept_prepared",
        "._load_record",
        "._put_prepared",
        "._refresh_records",
        "._save_record",
        "._write_new_record",
    ):
        assert private_method not in source
    for method_name in (
        "accept_prepared_event",
        "load_region_record",
        "prepare_event",
        "refresh_region_records",
        "save_region_record",
        "write_region_record",
    ):
        assert inspect.getdoc(getattr(BlobOutboxStore, method_name))


def test_fixed_clock_testing_helper_is_centralized() -> None:
    for path in (PROJECT_ROOT / "tests").glob("test_*.py"):
        assert "\nclass FixedClock" not in path.read_text()
    assert (PROJECT_ROOT / "durable_outbox" / "testing" / "clock.py").is_file()


def test_readme_documents_provider_contract_matrix() -> None:
    readme = (PROJECT_ROOT / "README.md").read_text()

    assert "Provider Contract" in readme
    assert "run_provider_contract" in readme
    assert "ProviderContract" in readme
    assert "incompatible duplicate" in readme
    assert "ordered-key blocking" in readme


def test_dependabot_tracks_uv_lockfiles_for_durable_outbox_packages() -> None:
    dependabot = (REPO_ROOT / ".github" / "dependabot.yml").read_text()

    assert 'package-ecosystem: "uv"' in dependabot
    assert 'directory: "/durable-outbox-python"' in dependabot
    assert 'directory: "/durable-outbox-fastapi"' in dependabot


def test_provider_docs_cover_rpo_zero_modes() -> None:
    docs = "\n".join(path.read_text() for path in (PROJECT_ROOT / "docs").glob("*.md"))

    assert "RPO=0" in docs
    assert "Blob" in docs
    assert "Cosmos" in docs
    assert "SQL" in docs
    assert "adapter acceptance contract" in docs
    assert "fingerprint_key" in docs
    assert "HMAC-SHA256" in docs
    assert "max_payload_bytes" in docs
    assert "fsync_interval_events" in docs


def test_plugin_authoring_guide_documents_sink_and_store_contracts() -> None:
    guide = (PROJECT_ROOT / "docs" / "plugin-authoring.md").read_text()

    for required_text in (
        "durable_outbox.sinks",
        "durable_outbox.stores",
        "MessageSink",
        "DurableOutboxStore",
        "def build_sink(config: Mapping[str, object])",
        "def build_store(config: Mapping[str, object])",
        '[project.entry-points."durable_outbox.sinks"]',
        '[project.entry-points."durable_outbox.stores"]',
        "pyproject.toml",
    ):
        assert required_text in guide


def test_plugin_authoring_guide_documents_installation_modes() -> None:
    guide = (PROJECT_ROOT / "docs" / "plugin-authoring.md").read_text()

    for required_text in (
        "pip install durable-outbox-example-sink",
        "uv add durable-outbox-example-sink",
        "uv pip install -e ../durable-outbox-example-sink",
        "uv add ../durable-outbox-example-store",
        'load_sink("example-file"',
        'load_store("example-sql"',
    ):
        assert required_text in guide


def test_plugin_authoring_guide_documents_verification() -> None:
    guide = (PROJECT_ROOT / "docs" / "plugin-authoring.md").read_text()
    readme = (PROJECT_ROOT / "README.md").read_text()
    providers = (PROJECT_ROOT / "docs" / "providers.md").read_text()

    for required_text in (
        "available_sinks()",
        "available_stores()",
        "run_provider_contract",
        "ProviderContract",
        "uv run ty check",
        "uv build",
    ):
        assert required_text in guide
    assert "docs/plugin-authoring.md" in readme
    assert "plugin-authoring.md" in providers


def test_operations_docs_describe_error_and_outcome_policy() -> None:
    operations = (PROJECT_ROOT / "docs" / "operations.md").read_text()

    assert "Error And Outcome Semantics" in operations
    assert "AdminActionStatus" in operations
    assert "success" in operations
    assert "not_found" in operations
    assert "wrong_state" in operations
    assert "RetryablePublishError" in operations
    assert "NonRetryablePublishError" in operations
    assert "RetryableStoreError" in operations
    assert "ClaimConflictError" in operations


def test_operations_docs_describe_package_logging_contract() -> None:
    operations = (PROJECT_ROOT / "docs" / "operations.md").read_text()

    assert "## Logging" in operations
    assert "durable_outbox" in operations
    assert "event_id" in operations
    assert "operation" in operations
    assert "error_type" in operations
    assert "payload bytes" in operations


def test_data_model_docs_map_canonical_fields_to_adapter_renderings() -> None:
    data_model = (PROJECT_ROOT / "docs" / "data-model.md").read_text()
    proposal = (PROJECT_ROOT / "docs" / "durable-outbox-rpo0-proposal.md").read_text()

    assert "Canonical Field" in data_model
    assert "created_at" in data_model
    assert "created_at_epoch_ms" in data_model
    assert "created_at_utc" in data_model
    assert "publish_result" in data_model
    assert "Blob" in data_model
    assert "Cosmos" in data_model
    assert "SQL" in data_model
    assert "schema_id" in proposal
    assert "created_at_epoch_ms" in proposal
    assert "createdAtEpochMs" not in proposal
