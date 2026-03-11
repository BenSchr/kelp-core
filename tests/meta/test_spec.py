"""Tests for kelp.meta.spec models."""

from pydantic import BaseModel

from kelp.meta.spec import MetaObjectSpec, MetaProjectSpec


class ExampleSettings(BaseModel):
    """Dummy framework settings model for tests."""

    models_path: str


class ExampleModel(BaseModel):
    """Dummy metadata model for tests."""

    name: str


def test_meta_object_spec_builds_with_required_fields() -> None:
    """MetaObjectSpec should accept required object loading fields."""
    spec = MetaObjectSpec(
        root_key="xy_models",
        project_config_key="models",
        path_attr="models_path",
        catalog_attr="models",
        model_class=ExampleModel,
        model_label="ExampleModel",
    )

    assert spec.root_key == "xy_models"
    assert spec.path_attr == "models_path"
    assert spec.model_class is ExampleModel


def test_meta_project_spec_enforces_framework_header_isolation() -> None:
    """MetaProjectSpec should carry framework-specific header/settings model."""
    object_spec = MetaObjectSpec(
        root_key="xy_models",
        project_config_key="models",
        path_attr="models_path",
        catalog_attr="models",
        model_class=ExampleModel,
        model_label="ExampleModel",
    )

    project_spec = MetaProjectSpec(
        framework_id="xy",
        project_header="xy_project",
        project_settings_model=ExampleSettings,
        object_specs=(object_spec,),
    )

    assert project_spec.framework_id == "xy"
    assert project_spec.project_header == "xy_project"
    assert project_spec.project_settings_model is ExampleSettings
    assert project_spec.object_specs[0].root_key == "xy_models"


def test_meta_project_spec_settings_resolution_flags_defaults() -> None:
    """MetaProjectSpec should provide explicit flags/keys for settings resolution."""
    project_spec = MetaProjectSpec(
        framework_id="xy",
        project_header="xy_project",
        project_settings_model=ExampleSettings,
    )

    assert project_spec.resolve_runtime_settings is False
    assert project_spec.settings_env_prefix == "KELP_"
    assert project_spec.settings_spark_prefix == "kelp"
    assert project_spec.target_setting_key == "target"
    assert project_spec.project_file_setting_key == "project_file"
