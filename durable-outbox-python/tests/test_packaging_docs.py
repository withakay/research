import inspect
import tomllib
from importlib import import_module
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PROJECT_ROOT.parent
OPTIONAL_DEPENDENCY_FLOORS = {
    "azure": {
        "aiohttp>=3.13.5",
        "azure-storage-blob>=12.29.0",
        "azure-cosmos>=4.15.0",
    },
    "kafka": {"confluent-kafka>=2.14.0"},
    "sql": {"pyodbc>=5.3.0"},
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

    for extra in pyproject["project"]["optional-dependencies"]:
        assert f"durable-outbox[{extra}]" in readme
    assert "uv run pytest" in readme
    assert "aspire run --apphost DurableOutbox.Integration.AppHost" in readme
    assert "uv run ruff check ." in readme
    assert "uv run ty check" in readme
    assert "uv build" in readme


def test_license_file_matches_project_metadata() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    license_text = (PROJECT_ROOT / "LICENSE").read_text()

    assert pyproject["project"]["license"] == "MIT"
    assert "MIT License" in license_text


def test_optional_dependency_floors_match_reviewed_provider_versions() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())

    assert {
        extra: set(dependencies)
        for extra, dependencies in pyproject["project"]["optional-dependencies"].items()
    } == OPTIONAL_DEPENDENCY_FLOORS


def test_project_metadata_describes_package_surface() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text())
    project = pyproject["project"]

    assert set(project["urls"]) == EXPECTED_PROJECT_URLS
    assert set(project["keywords"]) == EXPECTED_KEYWORDS
    assert EXPECTED_CLASSIFIERS <= set(project["classifiers"])
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


def test_dual_region_blob_store_uses_documented_region_methods() -> None:
    from durable_outbox.stores.blob_geo import (
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
