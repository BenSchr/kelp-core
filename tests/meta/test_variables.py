"""Tests for shared variable resolution in kelp.meta.variables."""

from pathlib import Path

from kelp.meta.variables import resolve_vars_with_target


def test_resolve_vars_injects_target_for_root_vars(tmp_path: Path) -> None:
    """Built-in target var should be available while rendering root vars."""
    project_file = tmp_path / "xy_project.yml"
    project_file.write_text(
        """
xy_project:
  models_path: ./models
vars:
  env_schema: "schema_${ target }"
""",
        encoding="utf-8",
    )

    runtime_vars = resolve_vars_with_target(
        project_file,
        target="prod",
    )

    assert runtime_vars["target"] == "prod"
    assert runtime_vars["env_schema"] == "schema_prod"


def test_resolve_vars_merging_priority(tmp_path: Path) -> None:
    """Variable precedence should be init > overwrite > target > default."""
    project_file = tmp_path / "xy_project.yml"
    overwrite_file = tmp_path / "overwrite.yml"

    project_file.write_text(
        """
xy_project:
  models_path: ./models
vars:
  x: default
vars_overwrite: overwrite.yml
targets:
  dev:
    vars:
      x: target
""",
        encoding="utf-8",
    )
    overwrite_file.write_text(
        """
vars:
  x: overwrite
""",
        encoding="utf-8",
    )

    runtime_vars = resolve_vars_with_target(
        project_file,
        target_file_path=project_file,
        target="dev",
        init_vars={"x": "init"},
    )

    assert runtime_vars["x"] == "init"
    assert runtime_vars["target"] == "dev"
