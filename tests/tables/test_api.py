"""Unit tests for kelp.tables API — no Spark dependency, all mocked."""

from unittest.mock import MagicMock

import pytest

from kelp.models.model import Column, Model
from kelp.service.model_manager import KelpModel

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_kelp_table(
    name: str = "orders",
    fqn: str = "catalog.schema.orders",
    schema_val: str = "id BIGINT, name STRING",
    schema_lite_val: str = "id BIGINT, name STRING",
    columns: list[Column] | None = None,
) -> KelpModel:
    root = Model(
        name=name,
        catalog="catalog",
        schema_="schema",
        columns=columns
        or [
            Column(name="id", data_type="bigint"),
            Column(name="name", data_type="string"),
        ],
    )
    kt = KelpModel(name=name)
    kt.fqn = fqn
    kt.schema = schema_val
    kt.schema_lite = schema_lite_val
    kt.root_model = root
    return kt


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGetTable:
    def test_returns_kelp_table(self, mocker: MagicMock) -> None:
        kt = _make_kelp_table()
        mocker.patch("kelp.tables.api.ModelManager.build_model", return_value=kt)

        from kelp.tables import get_model

        result = get_model("orders")
        assert isinstance(result, KelpModel)
        assert result.name == "orders"

    def test_not_kelp_sdp_table(self, mocker: MagicMock) -> None:
        """Ensure we get KelpModel, not KelpSdpModel."""
        kt = _make_kelp_table()
        mocker.patch("kelp.tables.api.ModelManager.build_model", return_value=kt)

        from kelp.service.model_manager import KelpSdpModel
        from kelp.tables import get_model

        result = get_model("orders")
        assert not isinstance(result, KelpSdpModel)


class TestRef:
    def test_returns_fqn(self, mocker: MagicMock) -> None:
        kt = _make_kelp_table()
        mocker.patch("kelp.tables.api.ModelManager.build_model", return_value=kt)

        from kelp.tables import ref

        assert ref("orders") == "catalog.schema.orders"

    def test_returns_name_when_fqn_is_none(self, mocker: MagicMock) -> None:
        kt = _make_kelp_table()
        kt.fqn = None
        mocker.patch("kelp.tables.api.ModelManager.build_model", return_value=kt)

        from kelp.tables import ref

        assert ref("orders") == "orders"


class TestSchema:
    def test_returns_schema_ddl(self, mocker: MagicMock) -> None:
        kt = _make_kelp_table()
        mocker.patch("kelp.tables.api.ModelManager.build_model", return_value=kt)

        from kelp.tables import schema

        assert schema("orders") == "id BIGINT, name STRING"

    def test_returns_none_when_no_schema(self, mocker: MagicMock) -> None:
        kt = _make_kelp_table()
        kt.schema = None
        mocker.patch("kelp.tables.api.ModelManager.build_model", return_value=kt)

        from kelp.tables import schema

        assert schema("orders") is None


class TestSchemaLite:
    def test_returns_schema_lite(self, mocker: MagicMock) -> None:
        kt = _make_kelp_table()
        mocker.patch("kelp.tables.api.ModelManager.build_model", return_value=kt)

        from kelp.tables import schema_lite

        assert schema_lite("orders") == "id BIGINT, name STRING"


class TestDdl:
    def test_returns_ddl_string(self, mocker: MagicMock) -> None:
        kt = _make_kelp_table()
        mock_get_ddl = mocker.patch.object(
            kt,
            "get_ddl",
            return_value="CREATE TABLE IF NOT EXISTS catalog.schema.orders (...)",
        )
        mocker.patch("kelp.tables.api.ModelManager.build_model", return_value=kt)

        from kelp.tables import ddl

        result = ddl("orders")
        assert result is not None
        assert "CREATE TABLE" in result
        mock_get_ddl.assert_called_once_with(if_not_exists=True)

    def test_ddl_without_if_not_exists(self, mocker: MagicMock) -> None:
        kt = _make_kelp_table()
        mock_get_ddl = mocker.patch.object(
            kt, "get_ddl", return_value="CREATE TABLE catalog.schema.orders (...)"
        )
        mocker.patch("kelp.tables.api.ModelManager.build_model", return_value=kt)

        from kelp.tables import ddl

        ddl("orders", if_not_exists=False)
        mock_get_ddl.assert_called_once_with(if_not_exists=False)


class TestColumns:
    def test_returns_column_list(self, mocker: MagicMock) -> None:
        kt = _make_kelp_table()
        mocker.patch("kelp.tables.api.ModelManager.build_model", return_value=kt)

        from kelp.tables import columns

        result = columns("orders")
        assert len(result) == 2
        assert result[0].name == "id"
        assert result[1].name == "name"

    def test_returns_empty_when_no_root_model(self, mocker: MagicMock) -> None:
        kt = _make_kelp_table()
        kt.root_model = None
        mocker.patch("kelp.tables.api.ModelManager.build_model", return_value=kt)

        from kelp.tables import columns

        assert columns("orders") == []

    def test_returns_empty_when_no_columns(self, mocker: MagicMock) -> None:
        root = Model(name="orders", catalog="catalog", schema_="schema", columns=[])
        kt = KelpModel(name="orders")
        kt.fqn = "catalog.schema.orders"
        kt.root_model = root
        mocker.patch("kelp.tables.api.ModelManager.build_model", return_value=kt)

        from kelp.tables import columns

        assert columns("orders") == []


class TestFunc:
    def test_returns_qualified_function_name(self, mocker: MagicMock) -> None:
        mock_function = MagicMock()
        mock_function.get_qualified_name.return_value = "catalog.schema.my_function"

        mock_catalog_index = MagicMock()
        mock_catalog_index.get.return_value = mock_function

        mock_context = MagicMock()
        mock_context.catalog_index = mock_catalog_index

        mocker.patch("kelp.config.get_context", return_value=mock_context)

        from kelp.tables import func

        result = func("my_function")
        assert result == "catalog.schema.my_function"
        mock_catalog_index.get.assert_called_once_with("functions", "my_function")
        mock_function.get_qualified_name.assert_called_once()

    def test_function_not_found(self, mocker: MagicMock) -> None:
        mock_catalog_index = MagicMock()
        mock_catalog_index.get.side_effect = KeyError("my_function")

        mock_context = MagicMock()
        mock_context.catalog_index = mock_catalog_index

        mocker.patch("kelp.config.get_context", return_value=mock_context)

        from kelp.tables import func

        with pytest.raises(KeyError):
            func("nonexistent_function")
