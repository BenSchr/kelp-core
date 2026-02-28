"""Schema enforcement transformation for PySpark DataFrames.

Provides :func:`apply_schema` — a composable transformation that casts,
reorders, adds, and drops columns to match a target schema defined in Kelp
metadata or supplied explicitly.

Usage with ``DataFrame.transform()``::

    from kelp.transformations import apply_schema

    # Look up schema from Kelp catalog
    df = raw_df.transform(apply_schema("my_table"))

    # Explicit DDL string
    df = raw_df.transform(apply_schema(schema="id INT, name STRING"))

    # With options
    df = raw_df.transform(
        apply_schema(
            "my_table",
            safe_cast=True,
            drop_extra_columns=True,
            add_missing_columns=True,
            missing_column_default=None,
        )
    )
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from pyspark.sql import Column as SparkColumn
from pyspark.sql import DataFrame, functions
from pyspark.sql.types import (
    ArrayType,
    DataType,
    MapType,
    StructField,
    StructType,
    VariantType,
)

logger = logging.getLogger(__name__)


def _get_kelp_columns(name: str) -> list:
    """Lazy import wrapper so callers that only use *schema=* never import kelp.tables."""
    from kelp.tables import columns as kelp_columns

    return kelp_columns(name)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def apply_schema(
    name: str | None = None,
    *,
    schema: str | StructType | None = None,
    safe_cast: bool = False,
    drop_extra_columns: bool = True,
    add_missing_columns: bool = True,
    missing_column_default: Any = None,
) -> Callable[[DataFrame], DataFrame]:
    """Return a transformation that enforces *schema* on a DataFrame.

    The returned callable is designed for :pyspark:`DataFrame.transform
    <pyspark.sql.DataFrame.transform>`.

    Exactly **one** of *name* or *schema* must be provided.

    * If *name* is given the target schema is resolved from the Kelp metadata
      catalog via :func:`kelp.tables.columns`.  When no columns are defined
      the DataFrame is returned unchanged (pass-through).
    * If *schema* is given as a DDL string (e.g. ``"id INT, name STRING"``) or
      a :class:`~pyspark.sql.types.StructType` it is used directly.

    Args:
        name: Kelp table name — resolves schema from the metadata catalog.
        schema: Explicit target schema as DDL string or ``StructType``.
        safe_cast: When ``True``, uses ``try_cast`` so incompatible values
            become ``NULL`` instead of raising.  Defaults to ``False``.
        drop_extra_columns: When ``True`` (default), columns present in the
            DataFrame but absent from the target schema are removed.
        add_missing_columns: When ``True`` (default), columns defined in the
            target schema but absent from the DataFrame are added with
            *missing_column_default* as their value.
        missing_column_default: The literal value used for newly added columns.
            Defaults to ``None`` (SQL ``NULL``).

    Returns:
        A ``Callable[[DataFrame], DataFrame]`` suitable for
        ``df.transform(…)``.

    Raises:
        ValueError: If both or neither of *name* / *schema* are provided.
    """
    if name is not None and schema is not None:
        msg = "Provide either 'name' or 'schema', not both."
        raise ValueError(msg)
    if name is None and schema is None:
        msg = "Provide either 'name' (Kelp table name) or 'schema' (DDL / StructType)."
        raise ValueError(msg)

    target_schema = _resolve_schema(name=name, schema=schema)

    if target_schema is None:
        logger.debug("No schema found for '%s' — returning pass-through.", name)
        return lambda df: df

    logger.debug(
        "apply_schema target: %d columns (%s)",
        len(target_schema.fields),
        ", ".join(f.name for f in target_schema.fields),
    )

    def _enforce(df: DataFrame) -> DataFrame:
        return _enforce_schema(
            df,
            target_schema=target_schema,
            safe_cast=safe_cast,
            drop_extra_columns=drop_extra_columns,
            add_missing_columns=add_missing_columns,
            missing_column_default=missing_column_default,
        )

    return _enforce


# ---------------------------------------------------------------------------
# Schema resolution
# ---------------------------------------------------------------------------


def _resolve_schema(
    *,
    name: str | None = None,
    schema: str | StructType | None = None,
) -> StructType | None:
    """Convert the caller-supplied target into a ``StructType`` (or ``None``).

    Args:
        name: Kelp table name to look up.
        schema: Explicit DDL string or StructType.

    Returns:
        Resolved StructType, or None if the table has no columns defined.
    """
    if schema is not None:
        if isinstance(schema, StructType):
            return schema
        # DDL string → StructType via PySpark's built-in parser
        from pyspark.sql.types import _parse_datatype_string

        parsed = _parse_datatype_string(schema)
        if isinstance(parsed, StructType):
            return parsed
        # Single type string like "INT" — wrap it so callers always get a StructType
        return StructType([StructField("value", parsed, nullable=True)])

    # Resolve from Kelp metadata — uses module-level import for testability
    cols = _get_kelp_columns(name)  # type: ignore[arg-type]
    if not cols:
        return None

    # Build a DDL string from Column definitions and parse
    ddl_parts: list[str] = []
    for col in cols:
        if col.data_type is None:
            logger.warning("Column '%s' has no data_type — skipping.", col.name)
            continue
        nullable_str = "" if col.nullable else " NOT NULL"
        ddl_parts.append(f"{col.name} {col.data_type}{nullable_str}")

    if not ddl_parts:
        return None

    from pyspark.sql.types import _parse_datatype_string

    ddl_string = ", ".join(ddl_parts)
    parsed = _parse_datatype_string(ddl_string)
    if isinstance(parsed, StructType):
        return parsed

    return None


# ---------------------------------------------------------------------------
# Inner enforcement logic
# ---------------------------------------------------------------------------


def _enforce_schema(
    df: DataFrame,
    *,
    target_schema: StructType,
    safe_cast: bool,
    drop_extra_columns: bool,
    add_missing_columns: bool,
    missing_column_default: Any,
) -> DataFrame:
    """Apply the four-phase schema enforcement to *df*.

    1. Cast existing columns to their target types (recursive for complex types).
    2. Drop columns not in the target schema (when *drop_extra_columns*).
    3. Add missing columns with *missing_column_default* (when *add_missing_columns*).
    4. Reorder columns to match the target schema order.
    """
    source_fields = {f.name.lower(): f for f in df.schema.fields}
    target_fields = {f.name.lower(): f for f in target_schema.fields}

    select_exprs: list[SparkColumn] = []

    for target_field in target_schema.fields:
        key = target_field.name.lower()

        if key in source_fields:
            source_field = source_fields[key]
            source_col_name = source_field.name  # preserve original casing

            casted = _cast_column(
                col_expr=functions.col(source_col_name),
                source_type=source_field.dataType,
                target_type=target_field.dataType,
                safe_cast=safe_cast,
                drop_extra_columns=drop_extra_columns,
                add_missing_columns=add_missing_columns,
                missing_column_default=missing_column_default,
            )
            select_exprs.append(casted.alias(target_field.name))
            logger.debug(
                "Cast column '%s': %s → %s",
                target_field.name,
                source_field.dataType.simpleString(),
                target_field.dataType.simpleString(),
            )
        elif add_missing_columns:
            default_col = _make_default_column(
                target_field=target_field,
                default_value=missing_column_default,
            )
            select_exprs.append(default_col)
            logger.debug(
                "Added missing column '%s' (%s) with default=%r",
                target_field.name,
                target_field.dataType.simpleString(),
                missing_column_default,
            )

    # Optionally keep extra columns (appended after target columns)
    if not drop_extra_columns:
        for source_field in df.schema.fields:
            if source_field.name.lower() not in target_fields:
                select_exprs.append(functions.col(source_field.name))
                logger.debug("Kept extra column '%s'", source_field.name)

    return df.select(*select_exprs)


# ---------------------------------------------------------------------------
# Recursive column casting
# ---------------------------------------------------------------------------


def _cast_column(
    col_expr: SparkColumn,
    source_type: DataType,
    target_type: DataType,
    *,
    safe_cast: bool,
    drop_extra_columns: bool,
    add_missing_columns: bool,
    missing_column_default: Any,
) -> SparkColumn:
    """Recursively cast *col_expr* from *source_type* to *target_type*.

    Dispatches to specialised helpers for ``StructType``, ``ArrayType``,
    ``MapType``, and ``VariantType``.  Simple / atomic types use a plain
    ``cast`` (or ``try_cast`` when *safe_cast* is ``True``).
    """
    # Same type — no-op
    if source_type == target_type:
        return col_expr

    # VARIANT ----------------------------------------------------------------
    if isinstance(target_type, VariantType):
        return _cast_to_variant(col_expr, source_type)

    # STRUCT -----------------------------------------------------------------
    if isinstance(target_type, StructType):
        return _cast_struct(
            col_expr=col_expr,
            source_type=source_type if isinstance(source_type, StructType) else None,
            target_type=target_type,
            safe_cast=safe_cast,
            drop_extra_columns=drop_extra_columns,
            add_missing_columns=add_missing_columns,
            missing_column_default=missing_column_default,
        )

    # ARRAY ------------------------------------------------------------------
    if isinstance(target_type, ArrayType):
        return _cast_array(
            col_expr=col_expr,
            source_type=source_type if isinstance(source_type, ArrayType) else None,
            target_type=target_type,
            safe_cast=safe_cast,
            drop_extra_columns=drop_extra_columns,
            add_missing_columns=add_missing_columns,
            missing_column_default=missing_column_default,
        )

    # MAP --------------------------------------------------------------------
    if isinstance(target_type, MapType):
        return _cast_map(
            col_expr=col_expr,
            source_type=source_type if isinstance(source_type, MapType) else None,
            target_type=target_type,
            safe_cast=safe_cast,
            drop_extra_columns=drop_extra_columns,
            add_missing_columns=add_missing_columns,
            missing_column_default=missing_column_default,
        )

    # SIMPLE / ATOMIC types --------------------------------------------------
    return _simple_cast(col_expr, target_type, safe_cast=safe_cast)


def _simple_cast(
    col_expr: SparkColumn,
    target_type: DataType,
    *,
    safe_cast: bool,
) -> SparkColumn:
    """Cast an atomic column, optionally using ``try_cast``."""
    if safe_cast:
        return col_expr.try_cast(target_type)
    return col_expr.cast(target_type)


# ---------------------------------------------------------------------------
# Struct casting
# ---------------------------------------------------------------------------


def _cast_struct(
    col_expr: SparkColumn,
    source_type: StructType | None,
    target_type: StructType,
    *,
    safe_cast: bool,
    drop_extra_columns: bool,
    add_missing_columns: bool,
    missing_column_default: Any,
) -> SparkColumn:
    """Recursively cast a struct column to *target_type*."""
    source_fields = {f.name.lower(): f for f in source_type.fields} if source_type else {}
    target_fields = {f.name.lower(): f for f in target_type.fields}

    fields: list[SparkColumn] = []

    for target_field in target_type.fields:
        key = target_field.name.lower()
        if key in source_fields:
            src = source_fields[key]
            inner = _cast_column(
                col_expr=col_expr[src.name],
                source_type=src.dataType,
                target_type=target_field.dataType,
                safe_cast=safe_cast,
                drop_extra_columns=drop_extra_columns,
                add_missing_columns=add_missing_columns,
                missing_column_default=missing_column_default,
            )
            fields.append(inner.alias(target_field.name))
        elif add_missing_columns:
            fields.append(
                _make_default_column(target_field, missing_column_default),
            )

    # Keep extra struct fields if requested
    if not drop_extra_columns and source_type:
        fields.extend(
            col_expr[src_field.name].alias(src_field.name)
            for src_field in source_type.fields
            if src_field.name.lower() not in target_fields
        )

    return functions.struct(*fields)


# ---------------------------------------------------------------------------
# Array casting
# ---------------------------------------------------------------------------


def _cast_array(
    col_expr: SparkColumn,
    source_type: ArrayType | None,
    target_type: ArrayType,
    *,
    safe_cast: bool,
    drop_extra_columns: bool,
    add_missing_columns: bool,
    missing_column_default: Any,
) -> SparkColumn:
    """Cast an array column — applies element-level casting via ``transform``."""
    src_element = source_type.elementType if source_type else None
    tgt_element = target_type.elementType

    if src_element and src_element == tgt_element:
        return col_expr

    # For complex element types, use the higher-order transform function
    if isinstance(tgt_element, (StructType, ArrayType, MapType, VariantType)):
        return functions.transform(
            col_expr,
            lambda elem: _cast_column(
                col_expr=elem,
                source_type=src_element or tgt_element,
                target_type=tgt_element,
                safe_cast=safe_cast,
                drop_extra_columns=drop_extra_columns,
                add_missing_columns=add_missing_columns,
                missing_column_default=missing_column_default,
            ),
        )

    # Simple element type — cast the whole array
    return functions.transform(
        col_expr, lambda elem: _simple_cast(elem, tgt_element, safe_cast=safe_cast)
    )


# ---------------------------------------------------------------------------
# Map casting
# ---------------------------------------------------------------------------


def _cast_map(
    col_expr: SparkColumn,
    source_type: MapType | None,
    target_type: MapType,
    *,
    safe_cast: bool,
    drop_extra_columns: bool,
    add_missing_columns: bool,
    missing_column_default: Any,
) -> SparkColumn:
    """Cast a map column — applies key / value casting separately."""
    src_key = source_type.keyType if source_type else None
    src_val = source_type.valueType if source_type else None
    tgt_key = target_type.keyType
    tgt_val = target_type.valueType

    result = col_expr

    # Cast keys
    if src_key != tgt_key:
        result = functions.transform_keys(
            result,
            lambda k, _v: _cast_column(
                col_expr=k,
                source_type=src_key or tgt_key,
                target_type=tgt_key,
                safe_cast=safe_cast,
                drop_extra_columns=drop_extra_columns,
                add_missing_columns=add_missing_columns,
                missing_column_default=missing_column_default,
            ),
        )

    # Cast values
    if src_val != tgt_val:
        result = functions.transform_values(
            result,
            lambda _k, v: _cast_column(
                col_expr=v,
                source_type=src_val or tgt_val,
                target_type=tgt_val,
                safe_cast=safe_cast,
                drop_extra_columns=drop_extra_columns,
                add_missing_columns=add_missing_columns,
                missing_column_default=missing_column_default,
            ),
        )

    return result


# ---------------------------------------------------------------------------
# Variant casting
# ---------------------------------------------------------------------------


def _cast_to_variant(col_expr: SparkColumn, source_type: DataType) -> SparkColumn:
    """Cast a column to ``VariantType``.

    * If the source is already ``VariantType`` → pass-through.
    * If the source is ``StringType`` → ``parse_json()``.
    * Otherwise → ``to_json()`` then ``parse_json()``.
    """
    from pyspark.sql.types import StringType

    if isinstance(source_type, VariantType):
        return col_expr
    if isinstance(source_type, StringType):
        return functions.parse_json(col_expr)
    return functions.parse_json(functions.to_json(col_expr))


# ---------------------------------------------------------------------------
# Default column helper
# ---------------------------------------------------------------------------


def _make_default_column(
    target_field: StructField,
    default_value: Any,
) -> SparkColumn:
    """Create a literal column with the given default and target type."""
    return functions.lit(default_value).cast(target_field.dataType).alias(target_field.name)
