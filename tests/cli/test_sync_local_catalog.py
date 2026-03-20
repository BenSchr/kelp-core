"""Tests for the sync-local-catalog CLI command."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from kelp.cli.sync_local_catalog import sync_local_catalog
from kelp.models.model import Model, TableType
from kelp.models.project_config import RemoteCatalogConfig
from kelp.service.yaml_manager import YamlUpdateReport


class _CatalogIndexStub:
    """Minimal catalog index stub for sync-local-catalog tests."""

    def __init__(self, table: Model) -> None:
        self._table = table

    def get_index(self, kind: str) -> dict[str, Model]:
        if kind == "models":
            return {self._table.name: self._table}
        if kind == "metric_views":
            return {}
        return {}

    def get_all(self, kind: str) -> list[Model]:
        if kind == "models":
            return [self._table]
        return []


def test_sync_local_catalog_passes_remote_catalog_config(mocker: MagicMock, tmp_path: Path) -> None:
    """sync-local-catalog should forward remote catalog config to YAML patching."""
    local_table = Model(
        name="test_table",
        catalog="cat",
        schema_="sch",
        table_type=TableType.MANAGED,
        columns=[],
        origin_file_path="models/test_table.yml",
    )
    remote_table = Model(
        name="test_table",
        catalog="cat",
        schema_="sch",
        table_type=TableType.MANAGED,
        table_properties={
            "delta.enableChangeDataFeed": "true",
            "system.internal": "ignore-me",
        },
        columns=[],
    )
    remote_config = RemoteCatalogConfig(
        table_property_mode="managed",
        managed_table_properties=["delta.enableChangeDataFeed"],
    )

    ctx = SimpleNamespace(
        project_settings=SimpleNamespace(
            metrics_path=None,
            remote_catalog_config=remote_config,
        ),
        catalog_index=_CatalogIndexStub(local_table),
    )

    patch_model_yaml = mocker.patch(
        "kelp.service.yaml_manager.YamlManager.patch_model_yaml",
        return_value=YamlUpdateReport(
            model_name="test_table",
            file_path=tmp_path / "models" / "test_table.yml",
            result_model={"name": "test_table"},
            changes_made=False,
            added_fields=[],
            updated_fields=[],
            removed_fields=[],
        ),
    )
    mocker.patch("kelp.config.init")
    mocker.patch("kelp.config.get_context", return_value=ctx)
    mocker.patch("kelp.utils.databricks.get_table_from_dbx_sdk", return_value=remote_table)
    path_config = SimpleNamespace(service_root_absolute=tmp_path / "models")
    mocker.patch(
        "kelp.service.yaml_manager.ServicePathConfig.from_context",
        return_value=path_config,
    )
    mocker.patch("kelp.cli.output.print_message")
    mocker.patch("kelp.cli.output.print_error")
    mocker.patch("kelp.cli.output.print_success")

    sync_local_catalog(name="test_table", dry_run=True)

    patch_model_yaml.assert_called_once_with(
        remote_table,
        path_config=path_config,
        dry_run=True,
        remote_catalog_config=remote_config,
    )
