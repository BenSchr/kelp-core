"""DDL generation for Unity Catalog functions."""

from __future__ import annotations

from kelp.models.function import KelpFunction


def _quote_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _render_parameters(function: KelpFunction) -> str:
    params: list[str] = []
    for parameter in function.parameters:
        clause = f"{parameter.name} {parameter.data_type}"
        if parameter.default_expression is not None:
            clause += f" DEFAULT {parameter.default_expression}"
        if parameter.comment:
            clause += f" COMMENT '{_quote_sql_string(parameter.comment)}'"
        params.append(clause)
    return ", ".join(params)


def _render_returns(function: KelpFunction) -> str:
    if function.returns_table:
        cols = ", ".join(
            [
                (
                    f"{column.name} {column.data_type}"
                    + (f" COMMENT '{_quote_sql_string(column.comment)}'" if column.comment else "")
                )
                for column in function.returns_table
            ],
        )
        return f"RETURNS TABLE ({cols})"

    if function.returns_data_type:
        return f"RETURNS {function.returns_data_type}"

    return ""


def generate_create_function_ddl(function: KelpFunction) -> str:
    """Generate CREATE FUNCTION DDL for SQL/Python functions."""
    if not function.name:
        raise ValueError("Function name is required")
    if not function.body:
        raise ValueError("Function body is required")

    fqn = function.get_qualified_name()

    create_parts = ["CREATE"]
    if function.or_replace:
        create_parts.append("OR REPLACE")
    if function.temporary:
        create_parts.append("TEMPORARY")
    create_parts.append("FUNCTION")
    if function.if_not_exists:
        create_parts.append("IF NOT EXISTS")

    header = " ".join(create_parts)
    ddl_parts = [f"{header} {fqn}({_render_parameters(function)})"]

    returns = _render_returns(function)
    if returns:
        ddl_parts.append(returns)

    ddl_parts.append(f"LANGUAGE {function.language}")

    if function.deterministic is True:
        ddl_parts.append("DETERMINISTIC")
    elif function.deterministic is False:
        ddl_parts.append("NOT DETERMINISTIC")

    if function.description:
        ddl_parts.append(f"COMMENT '{_quote_sql_string(function.description)}'")

    if function.data_access:
        ddl_parts.append(function.data_access)

    if function.default_collation:
        ddl_parts.append(f"DEFAULT COLLATION {function.default_collation}")

    if function.environment is not None:
        environment_items: list[str] = []
        if function.environment.dependencies:
            dependencies = ", ".join(
                [f'"{dependency}"' for dependency in function.environment.dependencies]
            )
            environment_items.append(f"dependencies = '[{dependencies}]'")
        if function.environment.environment_version is not None:
            environment_items.append(
                f"environment_version = '{function.environment.environment_version}'",
            )
        if environment_items:
            ddl_parts.append(f"ENVIRONMENT ({', '.join(environment_items)})")

    body = function.body.strip()
    if function.language == "PYTHON":
        ddl_parts.append("AS $$")
        ddl_parts.append(body)
        ddl_parts.append("$$")
    else:
        if body.upper().startswith("RETURN "):
            ddl_parts.append(body)
        else:
            ddl_parts.append("RETURN")
            ddl_parts.append(body)

    return "\n".join(ddl_parts)


def generate_drop_function_ddl(function: KelpFunction) -> str:
    """Generate DROP FUNCTION DDL."""
    return f"DROP FUNCTION IF EXISTS {function.get_qualified_name()}"
