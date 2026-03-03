"""DDL generation for Unity Catalog ABAC policies."""

from __future__ import annotations

from kelp.models.abac import AbacPolicy


def _quote_sql_string(value: str) -> str:
    return value.replace("'", "''")


def _render_principals(principals: list[str]) -> str:
    return ", ".join([f"`{principal}`" for principal in principals])


def generate_create_abac_policy_ddl(policy: AbacPolicy) -> str:
    """Generate CREATE POLICY SQL statement."""
    if not policy.name:
        raise ValueError("ABAC policy name is required")

    create_stmt = "CREATE OR REPLACE POLICY"
    if policy.mode == "ROW_FILTER":
        create_stmt = "CREATE POLICY"

    ddl_parts = [
        f"{create_stmt} {policy.name}",
        f"ON {policy.securable_type} {policy.securable_name}",
    ]

    if policy.description:
        ddl_parts.append(f"COMMENT '{_quote_sql_string(policy.description)}'")

    if policy.mode == "ROW_FILTER":
        ddl_parts.append(f"ROW FILTER {policy.udf_name}")
    else:
        if not policy.target_column:
            raise ValueError("COLUMN_MASK policy requires target_column")
        ddl_parts.append(f"COLUMN MASK {policy.udf_name}")

    if policy.principals_to:
        ddl_parts.append(f"TO {_render_principals(policy.principals_to)}")
    if policy.principals_except:
        ddl_parts.append(f"EXCEPT {_render_principals(policy.principals_except)}")

    ddl_parts.append("FOR TABLES")
    if policy.for_tables_when:
        ddl_parts.append(f"WHEN {policy.for_tables_when}")

    if policy.match_columns:
        match_expr = ", ".join(
            [f"{item.condition} AS {item.alias}" for item in policy.match_columns],
        )
        ddl_parts.append(f"MATCH COLUMNS {match_expr}")

    if policy.mode == "ROW_FILTER" and policy.using_columns:
        ddl_parts.append(f"USING COLUMNS ({', '.join(policy.using_columns)})")

    if policy.mode == "COLUMN_MASK":
        ddl_parts.append(f"ON COLUMN {policy.target_column}")

    return "\n".join(ddl_parts) + ";"


def generate_drop_abac_policy_ddl(policy: AbacPolicy) -> str:
    """Generate DROP POLICY SQL statement."""
    return f"DROP POLICY {policy.name} ON {policy.securable_type} {policy.securable_name}"
