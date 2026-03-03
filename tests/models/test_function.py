"""Tests for kelp.models.function module."""

from kelp.models.function import FunctionParameter, KelpFunction


def test_create_minimal_function() -> None:
    """Create a function with minimal required fields."""
    function = KelpFunction(name="normalize_email", body="lower(email)")

    assert function.name == "normalize_email"
    assert function.language == "SQL"
    assert function.body == "lower(email)"


def test_create_function_with_parameters_and_qualified_name() -> None:
    """Create a function with parameters and verify qualified name."""
    function = KelpFunction(
        name="hash_value",
        catalog="main",
        schema_="security",
        language="PYTHON",
        parameters=[FunctionParameter(name="v", data_type="STRING")],
        returns_data_type="STRING",
        body="return v",
    )

    assert function.get_qualified_name() == "main.security.hash_value"
    assert function.parameters[0].name == "v"
