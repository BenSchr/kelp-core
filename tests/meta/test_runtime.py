"""Integration tests for meta runtime orchestration."""

from pathlib import Path

from pydantic import BaseModel, Field

from kelp.meta.runtime import build_runtime_context, init_runtime
from kelp.meta.spec import MetaObjectSpec, MetaProjectSpec


class XYProjectSettings(BaseModel):
    """Framework-specific settings for test runtime."""

    models_path: str
    models: dict = Field(default_factory=dict)


class XYModel(BaseModel):
    """Test metadata model."""

    name: str
    catalog: str | None = None


def test_runtime_uses_framework_header_and_shared_vars(tmp_path: Path) -> None:
    """Runtime should read only framework header while sharing vars/targets logic."""
    project_file = tmp_path / "xy_project.yml"
    models_dir = tmp_path / "xy_metadata" / "models"
    models_dir.mkdir(parents=True)

    (models_dir / "customer.yml").write_text(
        """
xy_models:
  - name: customer
""",
        encoding="utf-8",
    )

    project_file.write_text(
        """
kelp_project:
  models_path: ./kelp_metadata/models
  models:
    +catalog: kelp_catalog
xy_project:
  models_path: ./xy_metadata/models
  models:
    +catalog: ${ target }_catalog
vars:
  project_env: ${ target }
targets:
  dev:
    vars:
      zone: eu
""",
        encoding="utf-8",
    )

    spec = MetaProjectSpec(
        framework_id="xy",
        project_header="xy_project",
        project_settings_model=XYProjectSettings,
        object_specs=(
            MetaObjectSpec(
                root_key="xy_models",
                project_config_key="models",
                path_attr="models_path",
                catalog_attr="models",
                model_class=XYModel,
                model_label="XYModel",
            ),
        ),
        project_filename="xy_project.yml",
    )

    ctx = build_runtime_context(
        spec,
        project_file_path=str(project_file),
        target="dev",
    )

    assert ctx.framework_id == "xy"
    assert ctx.runtime_vars["target"] == "dev"
    assert ctx.runtime_vars["project_env"] == "dev"
    assert ctx.runtime_vars["zone"] == "eu"

    assert len(ctx.catalog["models"]) == 1
    assert ctx.catalog["models"][0].name == "customer"
    assert ctx.catalog["models"][0].catalog == "dev_catalog"

    assert ctx.project_settings.models_path == "./xy_metadata/models"


def test_init_runtime_uses_meta_settings_when_enabled(tmp_path: Path, monkeypatch) -> None:
    """init_runtime should resolve target and project file from meta settings when enabled."""
    project_file = tmp_path / "xy_project.yml"
    models_dir = tmp_path / "xy_metadata" / "models"
    models_dir.mkdir(parents=True)

    (models_dir / "customer.yml").write_text(
        """
xy_models:
  - name: customer
""",
        encoding="utf-8",
    )

    project_file.write_text(
        """
xy_project:
  models_path: ./xy_metadata/models
  models:
    +catalog: ${ target }_catalog
targets:
  prod:
    vars:
      zone: eu
""",
        encoding="utf-8",
    )

    monkeypatch.setenv("XY_TARGET", "prod")
    monkeypatch.setenv("XY_PROJECT_FILE", str(project_file))

    spec = MetaProjectSpec(
        framework_id="xy_enabled",
        project_header="xy_project",
        project_settings_model=XYProjectSettings,
        object_specs=(
            MetaObjectSpec(
                root_key="xy_models",
                project_config_key="models",
                path_attr="models_path",
                catalog_attr="models",
                model_class=XYModel,
                model_label="XYModel",
            ),
        ),
        project_filename="xy_project.yml",
        resolve_runtime_settings=True,
        settings_env_prefix="XY_",
    )

    ctx = init_runtime(spec, project_file_path=None, target=None, refresh=True)

    assert ctx.target == "prod"
    assert ctx.project_file_path == str(project_file)
    assert ctx.runtime_vars["zone"] == "eu"


def test_init_runtime_ignores_meta_settings_when_disabled(tmp_path: Path, monkeypatch) -> None:
    """init_runtime should not use meta settings values unless explicitly enabled in spec."""
    project_file = tmp_path / "xy_project.yml"
    models_dir = tmp_path / "xy_metadata" / "models"
    models_dir.mkdir(parents=True)

    (models_dir / "customer.yml").write_text(
        """
xy_models:
  - name: customer
""",
        encoding="utf-8",
    )

    project_file.write_text(
        """
xy_project:
  models_path: ./xy_metadata/models
  models: {}
""",
        encoding="utf-8",
    )

    monkeypatch.setenv("XY_TARGET", "prod")

    spec = MetaProjectSpec(
        framework_id="xy_disabled",
        project_header="xy_project",
        project_settings_model=XYProjectSettings,
        object_specs=(
            MetaObjectSpec(
                root_key="xy_models",
                project_config_key="models",
                path_attr="models_path",
                catalog_attr="models",
                model_class=XYModel,
                model_label="XYModel",
            ),
        ),
        project_filename="xy_project.yml",
        resolve_runtime_settings=False,
        settings_env_prefix="XY_",
    )

    ctx = init_runtime(spec, project_file_path=str(project_file), target=None, refresh=True)

    assert ctx.target is None
