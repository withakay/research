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
