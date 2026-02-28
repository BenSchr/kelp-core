"""Tests for function DDL generation."""

from kelp.catalog.function_ddl import generate_create_function_ddl
from kelp.models.function import FunctionParameter, KelpFunction


def test_generate_create_sql_function_ddl() -> None:
    """Generate CREATE DDL for SQL function."""
    function = KelpFunction(
        name="area",
        catalog="main",
        schema_="default",
        language="SQL",
        parameters=[
            FunctionParameter(name="x", data_type="DOUBLE"),
            FunctionParameter(name="y", data_type="DOUBLE"),
        ],
        returns_data_type="DOUBLE",
        body="x * y",
    )

    ddl = generate_create_function_ddl(function)

    assert "CREATE OR REPLACE FUNCTION main.default.area(x DOUBLE, y DOUBLE)" in ddl
    assert "RETURNS DOUBLE" in ddl
    assert "LANGUAGE SQL" in ddl
    assert "RETURN $$" in ddl
    assert "x * y" in ddl


def test_generate_create_python_function_ddl() -> None:
    """Generate CREATE DDL for Python function."""
    function = KelpFunction(
        name="greet",
        language="PYTHON",
        parameters=[FunctionParameter(name="name", data_type="STRING")],
        returns_data_type="STRING",
        body='return "Hello " + name',
    )

    ddl = generate_create_function_ddl(function)

    assert "LANGUAGE PYTHON" in ddl
    assert "AS $$" in ddl
    assert 'return "Hello " + name' in ddl
