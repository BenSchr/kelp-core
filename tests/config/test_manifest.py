"""Tests for kelp manifest support in config module."""

from pathlib import Path

import pytest

from kelp.config import export_manifest, init
from kelp.meta.context import MetaContextStore


class TestManifestExport:
    """Tests for kelp manifest export."""

    def test_export_manifest_from_context(self, simple_project_dir: Path, tmp_path: Path) -> None:
        """export_manifest should create a manifest JSON from context."""
        project_file = simple_project_dir / "kelp_project.yml"
        ctx = init(project_file_path=str(project_file))

        output = tmp_path / "manifest.json"
        result = export_manifest(str(output), ctx)

        assert Path(result).exists()
        content = Path(result).read_text(encoding="utf-8")
        assert "kelp" in content
        assert "dev_catalog" in content

    def test_export_manifest_auto_context(
        self, simple_project_dir: Path, tmp_path: Path, monkeypatch
    ) -> None:
        """export_manifest without explicit ctx should use current context."""
        monkeypatch.chdir(simple_project_dir)
        init()

        output = tmp_path / "manifest.json"
        result = export_manifest(str(output))

        assert Path(result).exists()


class TestManifestInit:
    """Tests for kelp init with manifest_file_path."""

    def test_init_with_manifest_file_path(self, simple_project_dir: Path, tmp_path: Path) -> None:
        """init with manifest_file_path should load from manifest."""
        project_file = simple_project_dir / "kelp_project.yml"
        ctx = init(project_file_path=str(project_file))
        manifest_file = tmp_path / "manifest.json"
        export_manifest(str(manifest_file), ctx)

        # Clear context and reinitialize from manifest
        MetaContextStore.clear_all()
        loaded = init(manifest_file_path=str(manifest_file), refresh=True)

        assert loaded.project_settings.models_path == ctx.project_settings.models_path
        assert loaded.runtime_vars["catalog_name"] == "dev_catalog"
        assert len(loaded.catalog_index.get_all("models")) == len(
            ctx.catalog_index.get_all("models")
        )

    def test_init_with_manifest_skips_source_loading(
        self, simple_project_dir: Path, tmp_path: Path, monkeypatch
    ) -> None:
        """init with manifest_file_path should not load source files."""
        project_file = simple_project_dir / "kelp_project.yml"
        ctx = init(project_file_path=str(project_file))
        manifest_file = tmp_path / "manifest.json"
        export_manifest(str(manifest_file), ctx)

        # Clear and monkeypatch to ensure no source loading
        MetaContextStore.clear_all()
        monkeypatch.setattr(
            "kelp.meta.runtime.resolve_project_file_path",
            lambda *a, **kw: (_ for _ in ()).throw(AssertionError("Should not be called")),
        )

        loaded = init(manifest_file_path=str(manifest_file), refresh=True)
        assert loaded.framework_id == "kelp"

    def test_init_manifest_with_init_vars_raises(
        self, simple_project_dir: Path, tmp_path: Path
    ) -> None:
        """init should raise if both manifest_file_path and init_vars are provided."""
        project_file = simple_project_dir / "kelp_project.yml"
        ctx = init(project_file_path=str(project_file))
        manifest_file = tmp_path / "manifest.json"
        export_manifest(str(manifest_file), ctx)

        MetaContextStore.clear_all()
        with pytest.raises(ValueError, match="Cannot use 'manifest_file_path' together with"):
            init(
                manifest_file_path=str(manifest_file),
                init_vars={"key": "value"},
                refresh=True,
            )


class TestManifestEnvVar:
    """Tests for KELP_MANIFEST_FILE environment variable."""

    def test_env_var_is_used_when_no_explicit_path(
        self, simple_project_dir: Path, tmp_path: Path, monkeypatch
    ) -> None:
        """KELP_MANIFEST_FILE should be used when no explicit manifest_file_path arg."""
        project_file = simple_project_dir / "kelp_project.yml"
        ctx = init(project_file_path=str(project_file))
        manifest_file = tmp_path / "manifest.json"
        export_manifest(str(manifest_file), ctx)

        MetaContextStore.clear_all()
        monkeypatch.setenv("KELP_MANIFEST_FILE", str(manifest_file))

        loaded = init(refresh=True)

        assert loaded.framework_id == "kelp"
        assert loaded.runtime_vars["catalog_name"] == "dev_catalog"

    def test_explicit_manifest_file_path_takes_precedence_over_env(
        self, simple_project_dir: Path, tmp_path: Path, monkeypatch
    ) -> None:
        """Explicit manifest_file_path argument should beat KELP_MANIFEST_FILE env var."""
        project_file = simple_project_dir / "kelp_project.yml"
        ctx = init(project_file_path=str(project_file))

        # Create two manifests
        manifest_a = tmp_path / "a.json"
        manifest_b = tmp_path / "b.json"
        export_manifest(str(manifest_a), ctx)
        export_manifest(str(manifest_b), ctx)

        MetaContextStore.clear_all()
        # Env points to a nonexistent file
        monkeypatch.setenv("KELP_MANIFEST_FILE", "/nonexistent/path.json")

        # Explicit path should work fine
        loaded = init(manifest_file_path=str(manifest_a), refresh=True)
        assert loaded.framework_id == "kelp"

    def test_env_var_manifest_not_found_raises(self, monkeypatch) -> None:
        """KELP_MANIFEST_FILE pointing to missing file should raise."""
        monkeypatch.setenv("KELP_MANIFEST_FILE", "/nonexistent/manifest.json")

        with pytest.raises(FileNotFoundError, match="not found"):
            init(refresh=True)
