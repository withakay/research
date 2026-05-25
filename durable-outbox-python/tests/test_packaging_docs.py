import tomllib
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
