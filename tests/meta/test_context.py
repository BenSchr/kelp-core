"""Tests for isolated meta context storage."""

from pathlib import Path

from kelp.meta.context import MetaContextStore, MetaRuntimeContext


def _build_context(framework_id: str, marker: str, root_path: Path) -> MetaRuntimeContext:
    return MetaRuntimeContext(
        framework_id=framework_id,
        project_root=str(root_path),
        project_file_path=str(root_path / "project.yml"),
        target="dev",
        runtime_vars={"marker": marker},
        project_settings={"models_path": "./models"},
        catalog={"models": []},
    )


def test_contexts_are_isolated_by_framework_id(tmp_path: Path) -> None:
    """Different framework IDs should not overwrite each other's context."""
    MetaContextStore.clear_all()

    kelp_ctx = _build_context("kelp", "k", tmp_path / "kelp")
    xy_ctx = _build_context("xy", "x", tmp_path / "xy")

    MetaContextStore.set("kelp", kelp_ctx)
    MetaContextStore.set("xy", xy_ctx)

    stored_kelp = MetaContextStore.get("kelp")
    stored_xy = MetaContextStore.get("xy")

    assert stored_kelp is kelp_ctx
    assert stored_xy is xy_ctx
    assert stored_kelp is not None
    assert stored_xy is not None
    assert stored_kelp.runtime_vars["marker"] == "k"
    assert stored_xy.runtime_vars["marker"] == "x"


def test_get_or_create_reuses_existing_when_not_refreshing(tmp_path: Path) -> None:
    """get_or_create should return existing context unless refresh=True."""
    MetaContextStore.clear_all()

    first = _build_context("xy", "first", tmp_path / "xy")
    MetaContextStore.set("xy", first)

    created = MetaContextStore.get_or_create(
        "xy",
        factory=lambda: _build_context("xy", "second", tmp_path / "xy"),
        refresh=False,
    )

    assert created is first
    assert created.runtime_vars["marker"] == "first"
