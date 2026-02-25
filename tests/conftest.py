"""Global pytest fixtures and configuration."""

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

from kelp.config.lifecycle import ContextStore


@pytest.fixture(scope="session", autouse=True)
def load_environment_variables():
    """Load environment variables from .env file at the start of the test session."""
    os.environ.pop("KELP_TARGET", None)  # Ensure KELP_ENV is not set to interfere with tests
    load_dotenv(".env.test", override=True)


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
