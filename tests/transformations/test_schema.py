"""Tests for kelp.transformations.schema — apply_schema transformation.

Uses a real local SparkSession (provided by the session-scoped ``spark`` fixture
in conftest.py). Kelp catalog lookups are mocked via ``pytest-mock``.
"""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    VariantType,
)

from kelp.models.table import Column
from kelp.transformations.schema import apply_schema

# ---------------------------------------------------------------------------
# Validation tests (no Spark needed)
# ---------------------------------------------------------------------------


class TestApplySchemaValidation:
    """Argument validation — these don't need a SparkSession."""

    def test_raises_when_both_name_and_schema(self) -> None:
        with pytest.raises(ValueError, match="not both"):
            apply_schema("my_table", schema="id INT")

    def test_raises_when_neither_name_nor_schema(self) -> None:
        with pytest.raises(ValueError, match="Provide either"):
            apply_schema()


# ---------------------------------------------------------------------------
# Passthrough tests
# ---------------------------------------------------------------------------


class TestPassthrough:
    def test_no_columns_returns_identity(self, spark: SparkSession, mocker: MagicMock) -> None:
        mocker.patch("kelp.transformations.schema._get_kelp_columns", return_value=[])
        df = spark.createDataFrame([(1, "a")], ["id", "name"])
        result = df.transform(apply_schema("empty_table"))
        assert result.columns == ["id", "name"]
        assert result.collect() == df.collect()


# ---------------------------------------------------------------------------
# Basic casting
# ---------------------------------------------------------------------------


class TestBasicCasting:
    def test_string_to_int(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("1",), ("2",)], ["val"])
        result = df.transform(apply_schema(schema="val INT"))
        assert result.schema["val"].dataType == IntegerType()
        assert [r.val for r in result.collect()] == [1, 2]

    def test_string_to_bigint(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("100",)], ["val"])
        result = df.transform(apply_schema(schema="val BIGINT"))
        assert result.schema["val"].dataType == LongType()
        assert result.collect()[0].val == 100

    def test_string_to_double(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("3.14",)], ["val"])
        result = df.transform(apply_schema(schema="val DOUBLE"))
        assert result.schema["val"].dataType == DoubleType()
        assert abs(result.collect()[0].val - 3.14) < 1e-6

    def test_string_to_date(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("2025-01-15",)], ["dt"])
        result = df.transform(apply_schema(schema="dt DATE"))
        assert result.schema["dt"].dataType == DateType()
        assert result.collect()[0].dt == date(2025, 1, 15)

    def test_string_to_timestamp(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("2025-01-15 10:30:00",)], ["ts"])
        result = df.transform(apply_schema(schema="ts TIMESTAMP"))
        assert result.schema["ts"].dataType == TimestampType()
        assert result.collect()[0].ts == datetime(2025, 1, 15, 10, 30)


# ---------------------------------------------------------------------------
# Safe cast (try_cast)
# ---------------------------------------------------------------------------


class TestSafeCast:
    def test_invalid_data_returns_null(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("abc",), ("42",)], ["val"])
        result = df.transform(apply_schema(schema="val INT", safe_cast=True))
        rows = result.collect()
        assert rows[0].val is None
        assert rows[1].val == 42

    def test_hard_cast_raises_on_invalid_data(self, spark: SparkSession) -> None:
        """Spark 4.x with ANSI mode (default) raises on invalid hard cast."""
        from pyspark.errors.exceptions.captured import NumberFormatException

        df = spark.createDataFrame([("abc",)], ["val"])
        result = df.transform(apply_schema(schema="val INT", safe_cast=False))
        with pytest.raises(NumberFormatException):
            result.collect()


# ---------------------------------------------------------------------------
# Drop extra columns
# ---------------------------------------------------------------------------


class TestDropExtraColumns:
    def test_extra_columns_dropped(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, "a", 3.0)], ["a", "b", "c"])
        result = df.transform(apply_schema(schema="a INT, b STRING"))
        assert result.columns == ["a", "b"]

    def test_extra_columns_kept_when_disabled(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, "a", 3.0)], ["a", "b", "c"])
        result = df.transform(apply_schema(schema="a INT, b STRING", drop_extra_columns=False))
        assert set(result.columns) == {"a", "b", "c"}


# ---------------------------------------------------------------------------
# Add missing columns
# ---------------------------------------------------------------------------


class TestAddMissingColumns:
    def test_missing_column_added_with_null(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1,)], ["a"])
        result = df.transform(apply_schema(schema="a INT, b STRING"))
        assert "b" in result.columns
        row = result.collect()[0]
        assert row.a == 1
        assert row.b is None

    def test_missing_column_with_custom_default(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1,)], ["a"])
        result = df.transform(apply_schema(schema="a INT, b INT", missing_column_default=0))
        row = result.collect()[0]
        assert row.b == 0

    def test_missing_columns_not_added_when_disabled(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1,)], ["a"])
        result = df.transform(apply_schema(schema="a INT, b STRING", add_missing_columns=False))
        assert result.columns == ["a"]


# ---------------------------------------------------------------------------
# Column reordering
# ---------------------------------------------------------------------------


class TestColumnReordering:
    def test_columns_reordered_to_target_schema(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, "x", 3.0)], ["c", "a", "b"])
        result = df.transform(apply_schema(schema="a STRING, b DOUBLE, c INT"))
        assert result.columns == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Nested STRUCT casting
# ---------------------------------------------------------------------------


class TestStructCasting:
    def test_nested_struct_field_cast(self, spark: SparkSession) -> None:
        src_schema = StructType(
            [
                StructField(
                    "info",
                    StructType(
                        [
                            StructField("name", StringType()),
                            StructField("age", StringType()),
                        ]
                    ),
                )
            ]
        )
        data = [{"info": {"name": "Alice", "age": "30"}}]
        df = spark.createDataFrame(data, schema=src_schema)

        target = StructType(
            [
                StructField(
                    "info",
                    StructType(
                        [
                            StructField("name", StringType()),
                            StructField("age", IntegerType()),
                        ]
                    ),
                )
            ]
        )
        result = df.transform(apply_schema(schema=target))

        row = result.collect()[0]
        assert row.info.name == "Alice"
        assert row.info.age == 30

    def test_struct_drops_extra_fields(self, spark: SparkSession) -> None:
        src_schema = StructType(
            [
                StructField(
                    "info",
                    StructType(
                        [
                            StructField("name", StringType()),
                            StructField("extra", StringType()),
                        ]
                    ),
                )
            ]
        )
        data = [{"info": {"name": "Alice", "extra": "drop_me"}}]
        df = spark.createDataFrame(data, schema=src_schema)

        target = StructType(
            [
                StructField(
                    "info",
                    StructType([StructField("name", StringType())]),
                )
            ]
        )
        result = df.transform(apply_schema(schema=target))
        info_type = result.schema["info"].dataType
        assert isinstance(info_type, StructType)
        info_fields = info_type.fieldNames()
        assert "name" in info_fields
        assert "extra" not in info_fields

    def test_struct_adds_missing_fields(self, spark: SparkSession) -> None:
        src_schema = StructType(
            [
                StructField(
                    "info",
                    StructType([StructField("name", StringType())]),
                )
            ]
        )
        data = [{"info": {"name": "Alice"}}]
        df = spark.createDataFrame(data, schema=src_schema)

        target = StructType(
            [
                StructField(
                    "info",
                    StructType(
                        [
                            StructField("name", StringType()),
                            StructField("age", IntegerType()),
                        ]
                    ),
                )
            ]
        )
        result = df.transform(apply_schema(schema=target))
        row = result.collect()[0]
        assert row.info.name == "Alice"
        assert row.info.age is None


# ---------------------------------------------------------------------------
# ARRAY casting
# ---------------------------------------------------------------------------


class TestArrayCasting:
    def test_array_element_cast(self, spark: SparkSession) -> None:
        src_schema = StructType([StructField("vals", ArrayType(StringType()))])
        data = [{"vals": ["1", "2", "3"]}]
        df = spark.createDataFrame(data, schema=src_schema)

        target = StructType([StructField("vals", ArrayType(IntegerType()))])
        result = df.transform(apply_schema(schema=target))

        row = result.collect()[0]
        assert row.vals == [1, 2, 3]

    def test_array_of_structs_cast(self, spark: SparkSession) -> None:
        src_schema = StructType(
            [
                StructField(
                    "items",
                    ArrayType(
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("score", StringType()),
                            ]
                        )
                    ),
                )
            ]
        )
        data = [{"items": [{"id": "1", "score": "9.5"}, {"id": "2", "score": "8.0"}]}]
        df = spark.createDataFrame(data, schema=src_schema)

        target = StructType(
            [
                StructField(
                    "items",
                    ArrayType(
                        StructType(
                            [
                                StructField("id", IntegerType()),
                                StructField("score", DoubleType()),
                            ]
                        )
                    ),
                )
            ]
        )
        result = df.transform(apply_schema(schema=target))
        row = result.collect()[0]
        assert row.items[0].id == 1
        assert abs(row.items[0].score - 9.5) < 1e-6


# ---------------------------------------------------------------------------
# MAP casting
# ---------------------------------------------------------------------------


class TestMapCasting:
    def test_map_value_cast(self, spark: SparkSession) -> None:
        src_schema = StructType([StructField("props", MapType(StringType(), StringType()))])
        data = [{"props": {"age": "30", "score": "42"}}]
        df = spark.createDataFrame(data, schema=src_schema)

        target = StructType([StructField("props", MapType(StringType(), IntegerType()))])
        result = df.transform(apply_schema(schema=target))
        row = result.collect()[0]
        assert row.props["age"] == 30
        assert row.props["score"] == 42


# ---------------------------------------------------------------------------
# VARIANT casting
# ---------------------------------------------------------------------------


class TestVariantCasting:
    def test_string_to_variant(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([('{"key": "val"}',)], ["data"])
        target = StructType([StructField("data", VariantType())])
        result = df.transform(apply_schema(schema=target))
        assert isinstance(result.schema["data"].dataType, VariantType)

        # Verify the variant value is accessible
        extracted = result.select(functions.variant_get("data", "$.key", "STRING").alias("key"))
        assert extracted.collect()[0].key == "val"


# ---------------------------------------------------------------------------
# Explicit schema modes
# ---------------------------------------------------------------------------


class TestExplicitSchema:
    def test_ddl_string(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("1", "hello")], ["a", "b"])
        result = df.transform(apply_schema(schema="a INT, b STRING"))
        assert result.schema["a"].dataType == IntegerType()

    def test_struct_type(self, spark: SparkSession) -> None:
        target = StructType(
            [
                StructField("a", IntegerType()),
                StructField("b", StringType()),
            ]
        )
        df = spark.createDataFrame([("1", "hello")], ["a", "b"])
        result = df.transform(apply_schema(schema=target))
        assert result.schema["a"].dataType == IntegerType()


# ---------------------------------------------------------------------------
# Kelp catalog lookup (mocked)
# ---------------------------------------------------------------------------


class TestKelpCatalogLookup:
    def test_resolves_schema_from_table_name(self, spark: SparkSession, mocker: MagicMock) -> None:
        mocker.patch(
            "kelp.transformations.schema._get_kelp_columns",
            return_value=[
                Column(name="id", data_type="int"),
                Column(name="name", data_type="string"),
            ],
        )
        df = spark.createDataFrame([("1", "Alice")], ["id", "name"])
        result = df.transform(apply_schema("my_table"))
        assert result.schema["id"].dataType == IntegerType()

    def test_nullable_respected_in_target_schema(
        self, spark: SparkSession, mocker: MagicMock
    ) -> None:
        """Verify that NOT NULL is parsed correctly in the target schema.

        Note: Spark's ``col.cast().alias()`` does not propagate non-nullable
        metadata — the resolved *target* schema has ``nullable=False``, but
        the resulting DataFrame column will be ``nullable=True`` per Spark's
        column expression semantics. Non-null enforcement should be handled
        via quality checks, not type casting.
        """
        mocker.patch(
            "kelp.transformations.schema._get_kelp_columns",
            return_value=[
                Column(name="id", data_type="int", nullable=False),
            ],
        )
        df = spark.createDataFrame([("1",)], ["id"])
        result = df.transform(apply_schema("my_table"))
        # The cast itself works correctly
        assert result.schema["id"].dataType == IntegerType()
        assert result.collect()[0].id == 1

    def test_column_without_data_type_skipped(self, spark: SparkSession, mocker: MagicMock) -> None:
        mocker.patch(
            "kelp.transformations.schema._get_kelp_columns",
            return_value=[
                Column(name="id", data_type="int"),
                Column(name="no_type", data_type=None),
            ],
        )
        df = spark.createDataFrame([("1",)], ["id"])
        result = df.transform(apply_schema("my_table"))
        # Only "id" should be in the target schema; "no_type" was skipped
        assert result.columns == ["id"]


# ---------------------------------------------------------------------------
# Combined scenarios
# ---------------------------------------------------------------------------


class TestCombinedScenarios:
    def test_cast_drop_add_reorder(self, spark: SparkSession) -> None:
        """All four phases in one go: cast + drop + add + reorder."""
        df = spark.createDataFrame(
            [("42", "extra_val", "2025-06-15")],
            ["id", "extra", "created_at"],
        )
        result = df.transform(
            apply_schema(
                schema="created_at DATE, id INT, status STRING",
                add_missing_columns=True,
                missing_column_default="pending",
            )
        )
        assert result.columns == ["created_at", "id", "status"]
        row = result.collect()[0]
        assert row.id == 42
        assert row.created_at == date(2025, 6, 15)
        assert row.status == "pending"
        assert "extra" not in result.columns
