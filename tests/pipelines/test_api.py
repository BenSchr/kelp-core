"""Tests for kelp.pipelines.api module.

These tests verify the user-facing API functions that provide
convenient access to table metadata and parameters.
"""

from unittest.mock import MagicMock, patch

from kelp.pipelines import api
from kelp.service.table_manager import KelpSdpTable


class TestPipelinesApi:
    """Test the pipelines API functions."""

    @patch("kelp.pipelines.api.TableManager.build_sdp_table")
    def test_get_table(self, mock_build_sdp_table):
        """Test get_table returns KelpSdpTable object."""
        mock_table = MagicMock(spec=KelpSdpTable)
        mock_build_sdp_table.return_value = mock_table

        result = api.get_table("test_table")

        assert result == mock_table
        mock_build_sdp_table.assert_called_once_with("test_table")

    @patch("kelp.pipelines.api.TableManager.build_sdp_table")
    def test_target_with_quarantine(self, mock_build_sdp_table):
        """Test target returns target_table when quarantine is enabled."""
        mock_table = MagicMock(spec=KelpSdpTable)
        mock_table.target_table = "catalog.schema.table_validation"
        mock_build_sdp_table.return_value = mock_table

        result = api.target("test_table")

        assert result == "catalog.schema.table_validation"

    @patch("kelp.pipelines.api.TableManager.build_sdp_table")
    def test_target_without_quarantine(self, mock_build_sdp_table):
        """Test target returns table name when no quarantine."""
        mock_table = MagicMock(spec=KelpSdpTable)
        mock_table.target_table = None
        mock_build_sdp_table.return_value = mock_table

        result = api.target("test_table")

        assert result == "test_table"

    @patch("kelp.pipelines.api.TableManager.build_sdp_table")
    def test_ref(self, mock_build_sdp_table):
        """Test ref returns FQN of the table."""
        mock_table = MagicMock(spec=KelpSdpTable)
        mock_table.fqn = "catalog.schema.test_table"
        mock_build_sdp_table.return_value = mock_table

        result = api.ref("test_table")

        assert result == "catalog.schema.test_table"

    @patch("kelp.pipelines.api.TableManager.build_sdp_table")
    def test_ref_fallback(self, mock_build_sdp_table):
        """Test ref returns table name when FQN is None."""
        mock_table = MagicMock(spec=KelpSdpTable)
        mock_table.fqn = None
        mock_build_sdp_table.return_value = mock_table

        result = api.ref("test_table")

        assert result == "test_table"

    @patch("kelp.pipelines.api.TableManager.build_sdp_table")
    def test_schema(self, mock_build_sdp_table):
        """Test schema returns the table schema."""
        mock_table = MagicMock(spec=KelpSdpTable)
        mock_table.schema = "col1 string, col2 int"
        mock_build_sdp_table.return_value = mock_table

        result = api.schema("test_table")

        assert result == "col1 string, col2 int"

    @patch("kelp.pipelines.api.TableManager.build_sdp_table")
    def test_schema_lite(self, mock_build_sdp_table):
        """Test schema_lite returns the lite schema."""
        mock_table = MagicMock(spec=KelpSdpTable)
        mock_table.schema_lite = "col1 string, col2 int"
        mock_build_sdp_table.return_value = mock_table

        result = api.schema_lite("test_table")

        assert result == "col1 string, col2 int"

    @patch("kelp.pipelines.api.TableManager.build_sdp_table")
    def test_params(self, mock_build_sdp_table):
        """Test params returns streaming table parameters."""
        mock_table = MagicMock(spec=KelpSdpTable)
        mock_table.params.return_value = {
            "name": "catalog.schema.table",
            "comment": "Test table",
            "schema": "col1 string",
        }
        mock_build_sdp_table.return_value = mock_table

        result = api.params("test_table")

        assert result == {
            "name": "catalog.schema.table",
            "comment": "Test table",
            "schema": "col1 string",
        }
        mock_table.params.assert_called_once_with(exclude=[])

    @patch("kelp.pipelines.api.TableManager.build_sdp_table")
    def test_params_with_exclude(self, mock_build_sdp_table):
        """Test params with exclude parameter."""
        mock_table = MagicMock(spec=KelpSdpTable)
        mock_table.params.return_value = {"name": "test"}
        mock_build_sdp_table.return_value = mock_table

        _ = api.params("test_table", exclude=["comment", "schema"])

        mock_table.params.assert_called_once_with(exclude=["comment", "schema"])

    @patch("kelp.pipelines.api.TableManager.build_sdp_table")
    def test_params_cst(self, mock_build_sdp_table):
        """Test params_cst returns create_streaming_table parameters."""
        mock_table = MagicMock(spec=KelpSdpTable)
        mock_table.params_cst.return_value = {
            "name": "catalog.schema.table",
            "schema": "col1 string",
        }
        mock_build_sdp_table.return_value = mock_table

        result = api.params_cst("test_table")

        assert result == {
            "name": "catalog.schema.table",
            "schema": "col1 string",
        }
        mock_table.params_cst.assert_called_once_with(exclude=[])

    @patch("kelp.pipelines.api.TableManager.build_sdp_table")
    def test_params_cst_with_exclude(self, mock_build_sdp_table):
        """Test params_cst with exclude parameter."""
        mock_table = MagicMock(spec=KelpSdpTable)
        mock_table.params_cst.return_value = {}
        mock_build_sdp_table.return_value = mock_table

        _ = api.params_cst("test_table", exclude=["path"])

        mock_table.params_cst.assert_called_once_with(exclude=["path"])
