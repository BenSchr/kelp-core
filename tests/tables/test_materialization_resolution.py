"""Tests for how materialization resolves a target name against kelp metadata.

An unqualified name is a kelp model reference and must resolve; a qualified name is
a target table and is never resolved. A job without kelp metadata is not a special
mode — it names its targets in full.
"""

from pathlib import Path

import pytest

from kelp.config import init
from kelp.tables.materialization.resolve import (
    is_qualified_name,
    resolve_materialization_inputs,
)


@pytest.fixture
def materializations_project_dir(fixtures_dir: Path) -> Path:
    """Return fixture project root for materialization tests."""
    return fixtures_dir / "materializations_project"


@pytest.fixture
def initialized_materializations_context(
    monkeypatch: pytest.MonkeyPatch,
    materializations_project_dir: Path,
) -> None:
    """Initialize kelp context from the real fixture project."""
    monkeypatch.chdir(materializations_project_dir)
    init(project_file_path=str(materializations_project_dir / "kelp_project.yml"), refresh=True)


@pytest.mark.parametrize(
    ("table_name", "expected"),
    [
        ("orders", False),
        ("silver.orders", True),
        ("main.silver.orders", True),
        ("`orders.with.dots`", False),
        ("main.`orders.with.dots`", True),
    ],
)
def test_is_qualified_name(table_name: str, expected: bool) -> None:
    """Only a dot outside backticks makes a name qualified."""
    assert is_qualified_name(table_name) is expected


def test_model_backed_name_resolves_metadata(
    initialized_materializations_context: None,
) -> None:
    """An unqualified name matching a model carries DDL for both create and replace."""
    resolved = resolve_materialization_inputs(table_name="mat_append_orders", config=None)

    assert resolved.kelp_model is not None
    assert resolved.model_name == "mat_append_orders"
    assert resolved.create_table_ddl is not None
    assert "IF NOT EXISTS" in resolved.create_table_ddl
    assert resolved.replace_table_ddl is not None
    assert resolved.replace_table_ddl.startswith("CREATE OR REPLACE TABLE")


def test_unqualified_name_without_model_raises(
    initialized_materializations_context: None,
) -> None:
    """A typo'd model name fails instead of writing to an unqualified table."""
    with pytest.raises(LookupError, match="No kelp model named 'mat_append_ordres'"):
        resolve_materialization_inputs(table_name="mat_append_ordres", config=None)


def test_qualified_name_skips_model_resolution(
    initialized_materializations_context: None,
) -> None:
    """A qualified name is used as given, even when its last part names a model."""
    resolved = resolve_materialization_inputs(
        table_name="other_catalog.other_schema.mat_append_orders",
        config=None,
    )

    assert resolved.kelp_model is None
    assert resolved.dqx_quality is None
    assert resolved.create_table_ddl is None
    assert resolved.replace_table_ddl is None
    assert resolved.target_name == "other_catalog.other_schema.mat_append_orders"


def test_unqualified_name_without_project_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An unqualified name is never written as-is, not even without a kelp project."""
    monkeypatch.chdir(tmp_path)

    with pytest.raises(LookupError, match="No kelp project found to resolve model 'plain_orders'"):
        resolve_materialization_inputs(table_name="plain_orders", config=None)


def test_qualified_name_without_project_is_used_as_is(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Naming the target in full is how a job runs without kelp metadata."""
    monkeypatch.chdir(tmp_path)

    resolved = resolve_materialization_inputs(table_name="default.plain_orders", config=None)

    assert resolved.kelp_model is None
    assert resolved.target_name == "default.plain_orders"
    assert resolved.effective_config.mode == "append"
