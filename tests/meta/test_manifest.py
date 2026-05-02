"""Tests for meta manifest serialization and deserialization."""

from pathlib import Path

import pytest
from pydantic import BaseModel, Field

from kelp.meta.context import MetaRuntimeContext
from kelp.meta.manifest import (
    MANIFEST_VERSION,
    ManifestPayload,
    _build_spec_signature,
    export_manifest,
    load_manifest,
)
from kelp.meta.runtime import build_runtime_context, init_runtime
from kelp.meta.spec import MetaObjectSpec, MetaProjectSpec


class SimpleSettings(BaseModel):
    """Test framework settings model."""

    models_path: str = Field(default="./models")
    models: dict = Field(default_factory=dict)


class SimpleModel(BaseModel):
    """Test metadata model."""

    name: str
    catalog: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    description: str | None = None


def _make_spec(framework_id: str = "test_fw") -> MetaProjectSpec:
    return MetaProjectSpec(
        framework_id=framework_id,
        project_header="test_project",
        project_settings_model=SimpleSettings,
        object_specs=(
            MetaObjectSpec(
                root_key="test_models",
                project_config_key="models",
                path_attr="models_path",
                catalog_attr="models",
                model_class=SimpleModel,
                model_label="SimpleModel",
            ),
        ),
        project_filename="test_project.yml",
    )


def _make_context(
    framework_id: str = "test_fw", root: str = "/fake/test_project"
) -> MetaRuntimeContext:
    return MetaRuntimeContext(
        framework_id=framework_id,
        project_root=root,
        project_file_path=f"{root}/test_project.yml",
        target="dev",
        runtime_vars={"catalog": "test_catalog", "env": "dev"},
        project_settings=SimpleSettings(
            models_path="./models",
            models={"+catalog": "test_catalog"},
        ),
        catalog={
            "models": [
                SimpleModel(name="customers", catalog="test_catalog"),
                SimpleModel(name="orders", catalog="test_catalog", description="Order table"),
            ],
        },
    )


class TestExportManifest:
    """Tests for manifest export."""

    def test_export_creates_json_file(self, tmp_path: Path) -> None:
        """export_manifest should create a valid JSON file."""
        spec = _make_spec()
        ctx = _make_context()
        output = tmp_path / "manifest.json"

        result = export_manifest(ctx, spec, str(output))

        assert Path(result).exists()
        content = output.read_text(encoding="utf-8")
        assert "test_fw" in content
        assert "customers" in content

    def test_export_includes_manifest_version(self, tmp_path: Path) -> None:
        """Exported manifest should include the current manifest version."""
        spec = _make_spec()
        ctx = _make_context()
        output = tmp_path / "manifest.json"

        export_manifest(ctx, spec, str(output))

        manifest = ManifestPayload.model_validate_json(output.read_text(encoding="utf-8"))
        assert manifest.manifest_version == MANIFEST_VERSION

    def test_export_includes_spec_signature(self, tmp_path: Path) -> None:
        """Exported manifest should include a valid spec signature."""
        spec = _make_spec()
        ctx = _make_context()
        output = tmp_path / "manifest.json"

        export_manifest(ctx, spec, str(output))

        manifest = ManifestPayload.model_validate_json(output.read_text(encoding="utf-8"))
        assert manifest.spec_signature.framework_id == "test_fw"
        assert manifest.spec_signature.project_header == "test_project"
        assert manifest.spec_signature.object_catalog_attrs == ["models"]

    def test_export_fails_on_non_json_serializable_vars(self, tmp_path: Path) -> None:
        """export_manifest should raise TypeError for non-JSON-safe runtime_vars."""
        spec = _make_spec()
        ctx = MetaRuntimeContext(
            framework_id="test_fw",
            project_root=str(tmp_path),
            project_file_path=str(tmp_path / "test_project.yml"),
            runtime_vars={"func": lambda x: x},  # Not JSON-serializable
            project_settings=SimpleSettings(),
            catalog={"models": []},
        )
        output = tmp_path / "manifest.json"

        with pytest.raises(TypeError, match="non-JSON-serializable"):
            export_manifest(ctx, spec, str(output))

    def test_export_creates_parent_directories(self, tmp_path: Path) -> None:
        """export_manifest should create parent dirs if they don't exist."""
        spec = _make_spec()
        ctx = _make_context()
        output = tmp_path / "nested" / "dir" / "manifest.json"

        result = export_manifest(ctx, spec, str(output))

        assert Path(result).exists()


class TestLoadManifest:
    """Tests for manifest loading."""

    def test_load_round_trip(self, tmp_path: Path) -> None:
        """Loading an exported manifest should produce equivalent context."""
        spec = _make_spec()
        ctx = _make_context()
        output = tmp_path / "manifest.json"

        export_manifest(ctx, spec, str(output))
        loaded = load_manifest(str(output), spec)

        assert loaded.framework_id == ctx.framework_id
        assert loaded.project_root == ctx.project_root
        assert loaded.project_file_path == ctx.project_file_path
        assert loaded.target == ctx.target
        assert loaded.runtime_vars == ctx.runtime_vars
        assert loaded.project_settings.models_path == ctx.project_settings.models_path
        assert len(loaded.catalog["models"]) == 2
        assert loaded.catalog["models"][0].name == "customers"
        assert loaded.catalog["models"][1].name == "orders"

    def test_load_file_not_found(self, tmp_path: Path) -> None:
        """load_manifest should raise FileNotFoundError for missing files."""
        spec = _make_spec()

        with pytest.raises(FileNotFoundError, match="not found"):
            load_manifest(str(tmp_path / "nonexistent.json"), spec)

    def test_load_invalid_json(self, tmp_path: Path) -> None:
        """load_manifest should raise ValueError for invalid JSON."""
        spec = _make_spec()
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not json at all", encoding="utf-8")

        with pytest.raises(ValueError, match="Failed to parse"):
            load_manifest(str(bad_file), spec)

    def test_load_version_mismatch(self, tmp_path: Path) -> None:
        """load_manifest should raise ValueError for version mismatch."""
        spec = _make_spec()
        ctx = _make_context()
        output = tmp_path / "manifest.json"
        export_manifest(ctx, spec, str(output))

        # Tamper with version
        import json

        data = json.loads(output.read_text(encoding="utf-8"))
        data["manifest_version"] = 999
        output.write_text(json.dumps(data), encoding="utf-8")

        with pytest.raises(ValueError, match="version mismatch"):
            load_manifest(str(output), spec)

    def test_load_framework_mismatch(self, tmp_path: Path) -> None:
        """load_manifest should raise ValueError for framework mismatch."""
        spec_a = _make_spec("framework_a")
        spec_b = _make_spec("framework_b")
        ctx = _make_context("framework_a")
        output = tmp_path / "manifest.json"

        export_manifest(ctx, spec_a, str(output))

        with pytest.raises(ValueError, match="framework mismatch"):
            load_manifest(str(output), spec_b)

    def test_load_object_specs_mismatch(self, tmp_path: Path) -> None:
        """load_manifest should raise ValueError for object spec mismatch."""
        spec = _make_spec()
        ctx = _make_context()
        output = tmp_path / "manifest.json"
        export_manifest(ctx, spec, str(output))

        # Load with a different spec that has different object_specs
        different_spec = MetaProjectSpec(
            framework_id="test_fw",
            project_header="test_project",
            project_settings_model=SimpleSettings,
            object_specs=(
                MetaObjectSpec(
                    root_key="test_models",
                    project_config_key="models",
                    path_attr="models_path",
                    catalog_attr="models",
                    model_class=SimpleModel,
                    model_label="SimpleModel",
                ),
                MetaObjectSpec(
                    root_key="test_functions",
                    project_config_key="functions",
                    path_attr="functions_path",
                    catalog_attr="functions",
                    model_class=SimpleModel,
                    model_label="SimpleFunction",
                ),
            ),
            project_filename="test_project.yml",
        )

        with pytest.raises(ValueError, match="object specs mismatch"):
            load_manifest(str(output), different_spec)

    def test_load_target_mismatch(self, tmp_path: Path) -> None:
        """load_manifest should raise if expected_target doesn't match."""
        spec = _make_spec()
        ctx = _make_context()  # target="dev"
        output = tmp_path / "manifest.json"

        export_manifest(ctx, spec, str(output))

        with pytest.raises(ValueError, match="target mismatch"):
            load_manifest(str(output), spec, expected_target="prod")

    def test_load_project_file_mismatch(self, tmp_path: Path) -> None:
        """load_manifest should raise if expected project file doesn't match."""
        spec = _make_spec()
        ctx = _make_context()
        output = tmp_path / "manifest.json"

        export_manifest(ctx, spec, str(output))

        with pytest.raises(ValueError, match="project file mismatch"):
            load_manifest(str(output), spec, expected_project_file_path="/other/path.yml")

    def test_load_no_target_check_when_not_specified(self, tmp_path: Path) -> None:
        """load_manifest should not check target when expected_target is None."""
        spec = _make_spec()
        ctx = _make_context()  # target="dev"
        output = tmp_path / "manifest.json"

        export_manifest(ctx, spec, str(output))

        # Should not raise - no expected_target validation
        loaded = load_manifest(str(output), spec)
        assert loaded.target == "dev"


class TestInitRuntimeWithManifest:
    """Tests for init_runtime with manifest_file_path."""

    def test_init_runtime_with_manifest_skips_source_loading(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        """init_runtime with manifest_file_path should not call source loading functions."""
        spec = _make_spec()
        ctx = _make_context()
        manifest_file = tmp_path / "manifest.json"
        export_manifest(ctx, spec, str(manifest_file))

        # Monkeypatch source-loading functions to fail
        monkeypatch.setattr(
            "kelp.meta.runtime.resolve_project_file_path",
            lambda *a, **kw: (_ for _ in ()).throw(AssertionError("Should not be called")),
        )
        monkeypatch.setattr(
            "kelp.meta.runtime.build_runtime_context",
            lambda *a, **kw: (_ for _ in ()).throw(AssertionError("Should not be called")),
        )

        loaded = init_runtime(
            spec,
            manifest_file_path=str(manifest_file),
            store_in_global=False,
        )

        assert loaded.framework_id == "test_fw"
        assert loaded.target == "dev"
        assert len(loaded.catalog["models"]) == 2

    def test_init_runtime_manifest_with_init_vars_raises(self, tmp_path: Path) -> None:
        """init_runtime should raise if both manifest_file_path and init_vars are provided."""
        spec = _make_spec()
        ctx = _make_context()
        manifest_file = tmp_path / "manifest.json"
        export_manifest(ctx, spec, str(manifest_file))

        with pytest.raises(ValueError, match="Cannot use 'manifest_file_path' together with"):
            init_runtime(
                spec,
                manifest_file_path=str(manifest_file),
                init_vars={"key": "value"},
                store_in_global=False,
            )


class TestBuildSpecSignature:
    """Tests for spec signature building."""

    def test_signature_is_deterministic(self) -> None:
        """Same spec should always produce same signature."""
        spec = _make_spec()
        sig1 = _build_spec_signature(spec)
        sig2 = _build_spec_signature(spec)
        assert sig1 == sig2

    def test_signature_sorts_catalog_attrs(self) -> None:
        """Catalog attrs should be sorted for deterministic comparison."""
        spec = MetaProjectSpec(
            framework_id="test",
            project_header="test_project",
            project_settings_model=SimpleSettings,
            object_specs=(
                MetaObjectSpec(
                    root_key="z_models",
                    project_config_key="z",
                    path_attr="z_path",
                    catalog_attr="zebras",
                    model_class=SimpleModel,
                    model_label="Z",
                ),
                MetaObjectSpec(
                    root_key="a_models",
                    project_config_key="a",
                    path_attr="a_path",
                    catalog_attr="alpacas",
                    model_class=SimpleModel,
                    model_label="A",
                ),
            ),
        )
        sig = _build_spec_signature(spec)
        assert sig.object_catalog_attrs == ["alpacas", "zebras"]


class TestManifestIntegration:
    """Integration tests using actual project fixture loading."""

    def test_full_round_trip_with_build_runtime_context(self, tmp_path: Path) -> None:
        """Build context from source, export, reload, and verify equivalence."""
        # Set up a minimal project
        project_file = tmp_path / "test_project.yml"
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        project_file.write_text(
            """
test_project:
  models_path: ./models
  models:
    +catalog: my_catalog

vars:
  env: dev
""",
            encoding="utf-8",
        )

        (models_dir / "customers.yml").write_text(
            """
test_models:
  - name: customers
    description: Customer table
""",
            encoding="utf-8",
        )

        spec = _make_spec()
        ctx = build_runtime_context(
            spec,
            project_file_path=str(project_file),
            target=None,
        )

        # Export manifest
        manifest_file = tmp_path / "output" / "manifest.json"
        export_manifest(ctx, spec, str(manifest_file))

        # Reload from manifest
        loaded = load_manifest(str(manifest_file), spec)

        # Verify equivalence
        assert loaded.framework_id == ctx.framework_id
        assert loaded.project_root == ctx.project_root
        assert loaded.project_file_path == ctx.project_file_path
        assert loaded.target == ctx.target
        assert loaded.runtime_vars == ctx.runtime_vars
        assert loaded.project_settings.models_path == ctx.project_settings.models_path
        assert len(loaded.catalog["models"]) == len(ctx.catalog["models"])
        assert loaded.catalog["models"][0].name == ctx.catalog["models"][0].name
        assert loaded.catalog["models"][0].catalog == ctx.catalog["models"][0].catalog
