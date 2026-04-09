"""Tests for kelp.pipelines.api module.

These tests verify the user-facing API functions that provide
convenient access to table metadata and parameters.
"""

from unittest.mock import MagicMock, patch

from kelp.pipelines import api
from kelp.service.model_manager import KelpSdpModel


class TestPipelinesApi:
    """Test the pipelines API functions."""

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_get_model(self, mock_build_sdp_model):
        """Test get_table returns KelpSdpModel object."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_build_sdp_model.return_value = mock_table

        result = api.get_model("test_table")

        assert result == mock_table
        mock_build_sdp_model.assert_called_once_with("test_table")

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_target_with_quarantine(self, mock_build_sdp_model):
        """Test target returns target_table when quarantine is enabled."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_table.target_table = "catalog.schema.table_validation"
        mock_build_sdp_model.return_value = mock_table

        result = api.target("test_table")

        assert result == "catalog.schema.table_validation"

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_target_without_quarantine(self, mock_build_sdp_model):
        """Test target returns table name when no quarantine."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_table.target_table = None
        mock_build_sdp_model.return_value = mock_table

        result = api.target("test_table")

        assert result == "test_table"

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_ref(self, mock_build_sdp_model):
        """Test ref returns FQN of the table."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_table.fqn = "catalog.schema.test_table"
        mock_build_sdp_model.return_value = mock_table

        result = api.ref("test_table")

        assert result == "catalog.schema.test_table"

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_ref_fallback(self, mock_build_sdp_model):
        """Test ref returns table name when FQN is None."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_table.fqn = None
        mock_build_sdp_model.return_value = mock_table

        result = api.ref("test_table")

        assert result == "test_table"

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_schema(self, mock_build_sdp_model):
        """Test schema returns the table schema."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_table.schema = "col1 string, col2 int"
        mock_build_sdp_model.return_value = mock_table

        result = api.schema("test_table")

        assert result == "col1 string, col2 int"

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_schema_with_exclude(self, mock_build_sdp_model):
        """Test schema passes exclude parameter to build_sdp_model."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_table.schema = "col1 string, col2 int"
        mock_build_sdp_model.return_value = mock_table

        result = api.schema("test_table", exclude=["col2"])

        assert result == "col1 string, col2 int"
        mock_build_sdp_model.assert_called_once_with("test_table", exclude=["col2"])

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_schema_lite(self, mock_build_sdp_model):
        """Test schema_lite returns the lite schema."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_table.schema_lite = "col1 string, col2 int"
        mock_build_sdp_model.return_value = mock_table

        result = api.schema_lite("test_table")

        assert result == "col1 string, col2 int"

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_schema_lite_with_exclude(self, mock_build_sdp_model):
        """Test schema_lite passes exclude parameter to build_sdp_model."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_table.schema_lite = "col1 string, col2 int"
        mock_build_sdp_model.return_value = mock_table

        result = api.schema_lite("test_table", exclude=["col2"])

        assert result == "col1 string, col2 int"
        mock_build_sdp_model.assert_called_once_with("test_table", exclude=["col2"])

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_params(self, mock_build_sdp_model):
        """Test params returns streaming table parameters."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_table.params.return_value = {
            "name": "catalog.schema.table",
            "comment": "Test table",
            "schema": "col1 string",
        }
        mock_build_sdp_model.return_value = mock_table

        result = api.params("test_table")

        assert result == {
            "name": "catalog.schema.table",
            "comment": "Test table",
            "schema": "col1 string",
        }
        mock_table.params.assert_called_once_with(exclude=[])

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_params_with_exclude(self, mock_build_sdp_model):
        """Test params with exclude parameter."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_table.params.return_value = {"name": "test"}
        mock_build_sdp_model.return_value = mock_table

        _ = api.params("test_table", exclude=["comment", "schema"])

        mock_table.params.assert_called_once_with(exclude=["comment", "schema"])

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_params_cst(self, mock_build_sdp_model):
        """Test params_cst returns create_streaming_table parameters."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_table.params_cst.return_value = {
            "name": "catalog.schema.table",
            "schema": "col1 string",
        }
        mock_build_sdp_model.return_value = mock_table

        result = api.params_cst("test_table")

        assert result == {
            "name": "catalog.schema.table",
            "schema": "col1 string",
        }
        mock_table.params_cst.assert_called_once_with(exclude=[])

    @patch("kelp.pipelines.api.ModelManager.build_sdp_model")
    def test_params_cst_with_exclude(self, mock_build_sdp_model):
        """Test params_cst with exclude parameter."""
        mock_table = MagicMock(spec=KelpSdpModel)
        mock_table.params_cst.return_value = {}
        mock_build_sdp_model.return_value = mock_table

        _ = api.params_cst("test_table", exclude=["path"])

        mock_table.params_cst.assert_called_once_with(exclude=["path"])

    @patch("kelp.tables.func")
    def test_func(self, mock_tables_func):
        """Test func returns qualified function name by delegating to tables.func."""
        mock_tables_func.return_value = "catalog.schema.my_function"

        result = api.func("my_function")

        assert result == "catalog.schema.my_function"
        mock_tables_func.assert_called_once_with("my_function")

    @patch("kelp.tables.func")
    def test_func_different_function(self, mock_tables_func):
        """Test func with different function name."""
        mock_tables_func.return_value = "prod.transforms.normalize_customer_id"

        result = api.func("normalize_customer_id")

        assert result == "prod.transforms.normalize_customer_id"
        mock_tables_func.assert_called_once_with("normalize_customer_id")
