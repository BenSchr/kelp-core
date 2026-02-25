"""Tests for kelp.pipelines.utils module."""

from kelp.pipelines.utils import merge_params


class TestMergeParams:
    """Test the merge_params utility function."""

    def test_merge_params_basic(self):
        """Test basic parameter merging."""
        params = {"key1": "value1", "key2": "value2"}
        meta_params = {"key3": "value3"}

        result = merge_params(params, meta_params)

        assert result == {"key1": "value1", "key2": "value2", "key3": "value3"}

    def test_merge_params_with_kwargs(self):
        """Test merging with additional kwargs."""
        params = {"key1": "value1"}
        meta_params = {"key2": "value2"}
        kwargs = {"key3": "value3"}

        result = merge_params(params, meta_params, kwargs)

        assert result == {"key1": "value1", "key2": "value2", "key3": "value3"}

    def test_merge_params_override_order(self):
        """Test that kwargs override params which override meta_params."""
        params = {"key": "from_params"}
        meta_params = {"key": "from_meta"}
        kwargs = {"key": "from_kwargs"}

        result = merge_params(params, meta_params, kwargs)

        assert result["key"] == "from_kwargs"

    def test_merge_params_removes_none_values(self):
        """Test that None values are filtered out."""
        params = {"key1": "value1", "key2": None, "key3": "value3"}
        meta_params = {"key4": None, "key5": "value5"}
        kwargs = {"key6": None}

        result = merge_params(params, meta_params, kwargs)

        assert "key2" not in result
        assert "key4" not in result
        assert "key6" not in result
        assert result == {"key1": "value1", "key3": "value3", "key5": "value5"}

    def test_merge_params_name_from_meta(self):
        """Test that name is taken from meta_params if present."""
        params = {"name": "table1", "key": "value"}
        meta_params = {"name": "table2"}

        result = merge_params(params, meta_params)

        assert result["name"] == "table2"

    def test_merge_params_no_name_in_meta(self):
        """Test that name from params is kept if not in meta_params."""
        params = {"name": "table1", "key": "value"}
        meta_params = {"other_key": "other_value"}

        result = merge_params(params, meta_params)

        assert result["name"] == "table1"

    def test_merge_params_empty_dicts(self):
        """Test merging with empty dictionaries."""
        params = {}
        meta_params = {}

        result = merge_params(params, meta_params)

        assert result == {}

    def test_merge_params_none_kwargs(self):
        """Test that None kwargs is handled gracefully."""
        params = {"key": "value"}
        meta_params = {"key2": "value2"}

        result = merge_params(params, meta_params, None)

        assert result == {"key": "value", "key2": "value2"}

    def test_merge_params_complex_scenario(self):
        """Test a complex real-world scenario."""
        params = {
            "name": "my_table",
            "schema": "col1 string, col2 int",
            "comment": None,
            "path": "/path/to/table",
        }
        meta_params = {
            "name": "override_table",
            "spark_conf": {"key": "value"},
            "table_properties": None,
        }
        kwargs = {
            "partition_cols": ["col1"],
        }

        result = merge_params(params, meta_params, kwargs)

        assert result == {
            "name": "override_table",
            "schema": "col1 string, col2 int",
            "path": "/path/to/table",
            "spark_conf": {"key": "value"},
            "partition_cols": ["col1"],
        }
        assert "comment" not in result
        assert "table_properties" not in result
