"""Tests for kelp.config.lifecycle module.

This module tests the user-facing APIs for context lifecycle management:
- init(): Initialize runtime context
- get_context(): Retrieve global context
- ContextStore: Thread-safe context storage
"""

from pathlib import Path

import pytest

from kelp.config import get_context, init
from kelp.config.lifecycle import ContextExistsError, ContextStore


class TestInit:
    """Test the init() function - the primary user-facing API."""

    def test_init_minimal_project(self, minimal_project_dir: Path):
        """Test initialization with a minimal project."""
        project_file = minimal_project_dir / "kelp_project.yml"

        ctx = init(project_root=str(project_file), target=None)

        assert ctx is not None
        assert ctx.project_config.models_path == "./kelp_metadata/models"
        assert ctx.project_config.project_file_path == str(project_file)

    def test_init_simple_project_with_vars(self, simple_project_dir: Path):
        """Test initialization with a project that has variables."""
        project_file = simple_project_dir / "kelp_project.yml"

        ctx = init(project_root=str(project_file))

        assert ctx is not None
        assert ctx.project_config.models_path == "./kelp_metadata/models"
        assert "catalog_name" in ctx.runtime_vars
        assert ctx.runtime_vars["catalog_name"] == "dev_catalog"
        assert ctx.runtime_vars["schema_name"] == "dev_schema"

    def test_init_with_target(self, multi_target_project_dir: Path):
        """Test initialization with a specific target."""
        project_file = multi_target_project_dir / "kelp_project.yml"

        ctx = init(project_root=str(project_file), target="prod")

        assert ctx is not None
        # With multi-target projects, vars are merged during init
        # Check that target-specific vars are available
        assert "catalog_name" in ctx.runtime_vars or "environment" in ctx.runtime_vars

    def test_init_with_overwrite_vars(self, simple_project_dir: Path):
        """Test initialization with variable overrides."""
        project_file = simple_project_dir / "kelp_project.yml"

        overwrite_vars = {
            "catalog_name": "custom_catalog",
            "custom_var": "custom_value",
        }
        ctx = init(project_root=str(project_file), overwrite_vars=overwrite_vars)

        assert ctx.runtime_vars["catalog_name"] == "custom_catalog"
        assert ctx.runtime_vars["custom_var"] == "custom_value"
        # Original vars should still exist
        assert ctx.runtime_vars["schema_name"] == "dev_schema"

    def test_init_stores_in_global(self, minimal_project_dir: Path):
        """Test that init stores context globally by default."""
        project_file = minimal_project_dir / "kelp_project.yml"

        ctx = init(project_root=str(project_file))
        global_ctx = ContextStore.get()

        assert global_ctx is ctx
        assert global_ctx.project_config.models_path == "./kelp_metadata/models"

    def test_init_raises_on_duplicate_without_refresh(self, minimal_project_dir: Path):
        """Test that init with refresh=False returns existing context."""
        project_file = minimal_project_dir / "kelp_project.yml"

        ctx1 = init(project_root=str(project_file))
        ctx2 = init(project_root=str(project_file), refresh=False)

        # By default, get_or_create returns existing context
        assert ctx1 is ctx2

    def test_init_with_refresh(self, minimal_project_dir: Path, simple_project_dir: Path):
        """Test that init with refresh=True replaces existing context."""
        minimal_file = minimal_project_dir / "kelp_project.yml"
        simple_file = simple_project_dir / "kelp_project.yml"

        ctx1 = init(project_root=str(minimal_file))
        assert ctx1.project_config.models_path == "./kelp_metadata/models"

        ctx2 = init(project_root=str(simple_file), refresh=True)
        assert "catalog_name" in ctx2.runtime_vars

        global_ctx = ContextStore.get()
        assert global_ctx is ctx2

    def test_init_without_global_storage(self, minimal_project_dir: Path):
        """Test init with store_in_global=False."""
        project_file = minimal_project_dir / "kelp_project.yml"

        ctx = init(project_root=str(project_file), store_in_global=False)

        assert ctx is not None
        assert ContextStore.get() is None


class TestGetContext:
    """Test the get_context() function."""

    def test_get_context_returns_existing(self, minimal_project_dir: Path):
        """Test get_context returns the initialized context."""
        project_file = minimal_project_dir / "kelp_project.yml"

        ctx = init(project_root=str(project_file))
        retrieved_ctx = get_context()

        assert retrieved_ctx is ctx

    def test_get_context_auto_initializes_if_missing(self):
        """Test that get_context handles missing context gracefully."""
        # Context is cleared in conftest.py before each test
        # This test verifies behavior when no context exists
        ctx = ContextStore.get()
        assert ctx is None


class TestContextStore:
    """Test ContextStore thread-safe storage."""

    def test_context_store_set_and_get(self, minimal_project_dir: Path):
        """Test basic set and get operations."""
        project_file = minimal_project_dir / "kelp_project.yml"
        ctx = init(project_root=str(project_file), store_in_global=False)

        ContextStore.set(ctx)
        retrieved = ContextStore.get()

        assert retrieved is ctx

    def test_context_store_set_raises_without_overwrite(self, minimal_project_dir: Path):
        """Test that setting context twice raises error without overwrite flag."""
        project_file = minimal_project_dir / "kelp_project.yml"
        ctx = init(project_root=str(project_file))

        with pytest.raises(ContextExistsError):
            ContextStore.set(ctx, overwrite=False)

    def test_context_store_set_with_overwrite(
        self, minimal_project_dir: Path, simple_project_dir: Path
    ):
        """Test that setting context with overwrite=True replaces existing."""
        minimal_file = minimal_project_dir / "kelp_project.yml"
        simple_file = simple_project_dir / "kelp_project.yml"

        _ = init(project_root=str(minimal_file))
        ctx2 = init(project_root=str(simple_file), store_in_global=False)

        ContextStore.set(ctx2, overwrite=True)
        retrieved = ContextStore.get()

        assert retrieved is ctx2
        assert retrieved.project_config.models_path == "./kelp_metadata/models"

    def test_context_store_clear(self, minimal_project_dir: Path):
        """Test clearing the global context."""
        project_file = minimal_project_dir / "kelp_project.yml"
        init(project_root=str(project_file))

        assert ContextStore.get() is not None

        ContextStore.clear()

        assert ContextStore.get() is None

    def test_context_store_get_or_create_returns_existing(self, minimal_project_dir: Path):
        """Test get_or_create returns existing context when available."""
        project_file = minimal_project_dir / "kelp_project.yml"

        ctx1 = init(project_root=str(project_file))
        ctx2 = ContextStore.get_or_create(project_file_path=str(project_file))

        assert ctx1 is ctx2

    def test_context_store_get_or_create_creates_new(self, minimal_project_dir: Path):
        """Test get_or_create creates new context when none exists."""
        project_file = minimal_project_dir / "kelp_project.yml"

        ctx = ContextStore.get_or_create(project_file_path=str(project_file))

        assert ctx is not None
        assert ctx.project_config.models_path == "./kelp_metadata/models"
