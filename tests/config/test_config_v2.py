"""Tests for kelp.config module."""

from pathlib import Path

from kelp.config import get_context, init
from kelp.meta.context import MetaContextStore


def test_init_loads_runtime_context(minimal_project_dir: Path) -> None:
    """init should load runtime context via the meta backend."""
    project_file = minimal_project_dir / "kelp_project.yml"

    ctx = init(project_file_path=str(project_file))

    assert ctx.project_settings.models_path == "./kelp_metadata/models"
    assert ctx.project_file_path == str(project_file)
    assert MetaContextStore.get("kelp") is ctx


def test_get_context_auto_resolves_when_not_initialized(
    simple_project_dir: Path,
    monkeypatch,
) -> None:
    """get_context should auto-resolve context when init=True by default."""
    monkeypatch.chdir(simple_project_dir)

    ctx = get_context()

    assert ctx is not None
    assert "catalog_name" in ctx.runtime_vars


def test_init_resolves_target_from_settings_env(
    multi_target_project_dir: Path,
    monkeypatch,
) -> None:
    """init should resolve target from settings env when target argument is missing."""
    project_file = multi_target_project_dir / "kelp_project.yml"
    monkeypatch.setenv("KELP_TARGET", "prod")

    ctx = init(project_file_path=str(project_file), refresh=True)

    assert ctx.target == "prod"


def test_init_resolves_project_file_from_settings_env(
    minimal_project_dir: Path,
    monkeypatch,
) -> None:
    """init should resolve project_file from settings env when argument is missing."""
    project_file = minimal_project_dir / "kelp_project.yml"
    monkeypatch.setenv("KELP_PROJECT_FILE", str(project_file))

    ctx = init(project_file_path=None, refresh=True)

    assert ctx.project_file_path == str(project_file)
