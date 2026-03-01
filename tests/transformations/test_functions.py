"""Tests for kelp.transformations.functions module.

Tests the apply_func() function that applies Unity Catalog functions
to DataFrames as chainable transformations.
"""

from unittest.mock import patch

import pytest


class TestApplyFunc:
    """Test the apply_func transformation function."""

    @patch("kelp.tables.func")
    def test_apply_func_returns_callable(self, mock_func_lookup):
        """Test apply_func returns a callable."""
        mock_func_lookup.return_value = "catalog.schema.normalize_id"

        from kelp.transformations.functions import apply_func

        transform = apply_func("normalize_id", "normalized_id", parameters={"id": "customer_id"})

        # Verify it's callable
        assert callable(transform)

    @patch("kelp.tables.func")
    def test_apply_func_with_parameter_mapping(self, mock_func_lookup):
        """Test apply_func accepts parameter mapping."""
        mock_func_lookup.return_value = "catalog.schema.format_name"

        from kelp.transformations.functions import apply_func

        transform = apply_func(
            "format_name",
            "full_name",
            parameters={"first_name": "first_name", "last_name": "last_name"},
        )

        assert callable(transform)
        mock_func_lookup.assert_called_once_with("format_name")

    @patch("kelp.tables.func")
    def test_apply_func_with_literal_column(self, mock_func_lookup):
        """Test apply_func with literal column name."""
        mock_func_lookup.return_value = "catalog.schema.normalize_email"

        from kelp.transformations.functions import apply_func

        transform = apply_func("normalize_email", "email_normalized", parameters="email")

        assert callable(transform)
        mock_func_lookup.assert_called_once_with("normalize_email")

    @patch("kelp.tables.func")
    def test_apply_func_without_parameters(self, mock_func_lookup):
        """Test apply_func with no explicit parameters (uses default None)."""
        mock_func_lookup.return_value = "catalog.schema.my_function"

        from kelp.transformations.functions import apply_func

        transform = apply_func("my_function", "output_col")

        assert callable(transform)

    @patch("kelp.tables.func")
    def test_apply_func_function_not_found(self, mock_func_lookup):
        """Test apply_func raises error when function is not found."""
        mock_func_lookup.side_effect = KeyError("nonexistent_function")

        from kelp.transformations.functions import apply_func

        with pytest.raises(KeyError):
            apply_func("nonexistent_function", "output")

    @patch("kelp.tables.func")
    def test_apply_func_docstring(self, mock_func_lookup):
        """Test apply_func has proper docstring."""
        mock_func_lookup.return_value = "catalog.schema.test"

        from kelp.transformations.functions import apply_func

        assert apply_func.__doc__ is not None
        assert "transformation" in apply_func.__doc__.lower()
        assert "unity" in apply_func.__doc__.lower()
        assert "function" in apply_func.__doc__.lower()

    @patch("kelp.tables.func")
    def test_apply_func_signature(self, mock_func_lookup):
        """Test apply_func has correct signature."""
        import inspect

        from kelp.transformations.functions import apply_func

        sig = inspect.signature(apply_func)
        params = list(sig.parameters.keys())

        # Should have func_name, new_column, and optional parameters
        assert "func_name" in params
        assert "new_column" in params
        assert "parameters" in params
