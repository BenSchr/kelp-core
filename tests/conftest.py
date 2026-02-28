"""Global pytest fixtures and configuration."""

from pathlib import Path

import pytest

from kelp.config.lifecycle import ContextStore


@pytest.fixture(autouse=True)
def reset_context_and_env():
    """Reset the global context before each test.

    Ensures test isolation by clearing the context store.
    """
    # Clear context before test
    ContextStore.clear()

    yield

    # Clear context after test
    ContextStore.clear()


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def simple_project_dir(fixtures_dir: Path) -> Path:
    """Return the path to a simple test project."""
    return fixtures_dir / "simple_project"


@pytest.fixture
def multi_target_project_dir(fixtures_dir: Path) -> Path:
    """Return the path to a multi-target test project."""
    return fixtures_dir / "multi_target_project"


@pytest.fixture
def minimal_project_dir(fixtures_dir: Path) -> Path:
    """Return the path to a minimal test project."""
    return fixtures_dir / "minimal_project"


@pytest.fixture
def functions_abacs_project_dir(fixtures_dir: Path) -> Path:
    """Return the path to a test project with functions and ABAC policies."""
    return fixtures_dir / "functions_abacs_project"
