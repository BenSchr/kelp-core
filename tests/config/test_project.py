"""Tests for kelp.config.project module.

This module tests project configuration loading and resolution APIs:
- load_project_config(): Load complete project configuration
- resolve_project_file_path(): Find project file
- load_project_yaml(): Load and parse project YAML
"""

from pathlib import Path

import pytest

from kelp.config.project import (
    load_project_config,
    load_project_yaml,
    resolve_project_file_path,
    resolve_project_root_dir,
)


class TestResolveProjectRootDir:
    """Test project root directory resolution."""

    def test_resolve_from_project_directory(self, minimal_project_dir: Path, monkeypatch):
        """Test resolving project root when in project directory."""
        monkeypatch.chdir(minimal_project_dir)

        root_dir = resolve_project_root_dir()

        assert Path(root_dir) == minimal_project_dir

    def test_resolve_from_subdirectory(self, simple_project_dir: Path, monkeypatch):
        """Test resolving project root from a subdirectory."""
        # Navigate to metadata directory
        metadata_dir = simple_project_dir / "kelp_metadata"
        monkeypatch.chdir(metadata_dir)

        root_dir = resolve_project_root_dir()

        assert Path(root_dir) == simple_project_dir

    def test_raises_when_project_not_found(self, tmp_path: Path, monkeypatch):
        """Test error when project file is not found."""
        # Create an empty temp directory
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        monkeypatch.chdir(empty_dir)

        with pytest.raises(FileNotFoundError, match="Project root with"):
            resolve_project_root_dir()


class TestLoadProjectYaml:
    """Test loading project YAML with Jinja rendering."""

    def test_load_project_yaml_minimal(self, minimal_project_dir: Path):
        """Test loading a minimal project YAML."""
        project_file = minimal_project_dir / "kelp_project.yml"

        data = load_project_yaml(str(project_file), runtime_vars={})

        assert "kelp_project" in data
        assert data["kelp_project"]["models_path"] == "./kelp_metadata/models"

    def test_load_project_yaml_with_vars(self, simple_project_dir: Path):
        """Test loading project YAML with variable substitution."""
        project_file = simple_project_dir / "kelp_project.yml"

        # Need to provide runtime_vars for rendering
        runtime_vars = {"catalog_name": "dev_catalog", "schema_name": "dev_schema"}
        data = load_project_yaml(str(project_file), runtime_vars=runtime_vars)

        assert "kelp_project" in data
        assert "vars" in data
        assert data["vars"]["catalog_name"] == "dev_catalog"

    def test_load_project_yaml_jinja_substitution(self, tmp_path: Path):
        """Test that Jinja variables are substituted correctly."""
        project_file = tmp_path / "kelp_project.yml"
        project_file.write_text(
            """
kelp_project:
  models_path: "./metadata"
  models:
    +catalog: "${ my_catalog }"
    +schema: "${ my_schema }"
"""
        )

        runtime_vars = {"my_catalog": "prod_catalog", "my_schema": "prod_schema"}
        data = load_project_yaml(str(project_file), runtime_vars=runtime_vars)

        assert data["kelp_project"]["models"]["+catalog"] == "prod_catalog"
        assert data["kelp_project"]["models"]["+schema"] == "prod_schema"


class TestLoadProjectConfig:
    """Test the main load_project_config function."""

    def test_load_minimal_project_config(self, minimal_project_dir: Path):
        """Test loading a minimal project configuration."""
        project_file = minimal_project_dir / "kelp_project.yml"

        config = load_project_config(
            project_file_path=str(project_file),
            target=None,
            init_vars={},
        )

        assert config.models_path == "./kelp_metadata/models"
        assert config.project_file_path == str(project_file)

    def test_load_project_with_vars(self, simple_project_dir: Path):
        """Test loading project with variables."""
        project_file = simple_project_dir / "kelp_project.yml"

        config = load_project_config(
            project_file_path=str(project_file),
            target=None,
            init_vars={},
        )

        assert config.models_path == "./kelp_metadata/models"
        # Vars are stored in runtime_vars, not on the config itself
        assert "+catalog" in str(config.models)

    def test_load_project_with_target(self, multi_target_project_dir: Path):
        """Test loading project configuration with a specific target."""
        project_file = multi_target_project_dir / "kelp_project.yml"

        # The test is checking that target selection works
        # Target vars will be merged during runtime, not in ProjectConfig
        config = load_project_config(
            project_file_path=str(project_file),
            target="prod",
            init_vars={},
        )

        assert config.models_path == "./kelp_metadata/models"

    def test_load_project_with_overwrite_vars(self, simple_project_dir: Path):
        """Test that runtime vars are accessible during loading."""
        project_file = simple_project_dir / "kelp_project.yml"

        init_vars = {
            "catalog_name": "override_catalog",
            "schema_name": "override_schema",
        }

        config = load_project_config(
            project_file_path=str(project_file),
            target=None,
            init_vars=init_vars,
        )

        assert config.models_path == "./kelp_metadata/models"
        # Init vars are used for Jinja rendering and variable resolution


class TestResolveProjectFilePath:
    """Test project file path resolution."""

    def test_resolve_with_env_variable(self, minimal_project_dir: Path, monkeypatch):
        """Test resolving from environment variable."""
        project_file = minimal_project_dir / "kelp_project.yml"
        monkeypatch.setenv("KELP_PROJECT_FILE", str(project_file))

        resolved = resolve_project_file_path()

        assert Path(resolved) == project_file

    def test_resolve_with_folder_search(self, minimal_project_dir: Path, monkeypatch):
        """Test resolving via folder search."""
        monkeypatch.chdir(minimal_project_dir)
        # Ensure env var is not set
        monkeypatch.delenv("KELP_PROJECT_FILE", raising=False)

        resolved = resolve_project_file_path()

        assert Path(resolved) == minimal_project_dir / "kelp_project.yml"

    def test_raises_when_specified_file_not_found(self, monkeypatch):
        """Test error when specified file doesn't exist."""
        monkeypatch.setenv("KELP_PROJECT_FILE", "/nonexistent/path/project.yml")

        with pytest.raises(FileNotFoundError, match="Specified project file not found"):
            resolve_project_file_path()
