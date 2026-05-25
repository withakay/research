import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


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


def test_provider_docs_cover_rpo_zero_modes() -> None:
    docs = "\n".join(path.read_text() for path in (PROJECT_ROOT / "docs").glob("*.md"))

    assert "RPO=0" in docs
    assert "Blob" in docs
    assert "Cosmos" in docs
    assert "SQL" in docs
    assert "adapter acceptance contract" in docs
