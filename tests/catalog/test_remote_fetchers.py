"""Tests for the remote_fetchers factory and its engine registry."""

import pytest

from kelp.catalog.remote_fetchers import DEFAULT_ENGINE, RemoteFetcherFactory
from kelp.catalog.remote_fetchers.sdk import SdkMetricViewFetcher, SdkTableFetcher
from kelp.catalog.remote_fetchers.spark import (
    SparkMetricViewFetcher,
    SparkTableFetcher,
    _format_column_type,
    _parse_table_constraints,
    _split_fqn,
)


def test_default_engine_is_sdk() -> None:
    assert DEFAULT_ENGINE == "sdk"


def test_get_table_fetcher_defaults_to_sdk() -> None:
    factory = RemoteFetcherFactory()

    assert isinstance(factory.get_table_fetcher("spark"), SparkTableFetcher)
    assert isinstance(factory.get_table_fetcher(), SdkTableFetcher)


def test_get_metric_view_fetcher_defaults_to_sdk() -> None:
    factory = RemoteFetcherFactory()

    assert isinstance(factory.get_metric_view_fetcher("spark"), SparkMetricViewFetcher)
    assert isinstance(factory.get_metric_view_fetcher(), SdkMetricViewFetcher)


def test_spark_table_fetcher_requires_active_session() -> None:
    fetcher = RemoteFetcherFactory().get_table_fetcher("spark")

    with pytest.raises(RuntimeError, match="active Spark session"):
        fetcher.fetch("main.sales.orders")


def test_spark_metric_view_fetcher_requires_active_session() -> None:
    fetcher = RemoteFetcherFactory().get_metric_view_fetcher("spark")

    with pytest.raises(RuntimeError, match="active Spark session"):
        fetcher.fetch("main.sales.order_metrics")


def test_get_table_fetcher_rejects_unknown_engine() -> None:
    factory = RemoteFetcherFactory()

    with pytest.raises(KeyError, match="bogus"):
        factory.get_table_fetcher("bogus")


def test_get_metric_view_fetcher_rejects_unknown_engine() -> None:
    factory = RemoteFetcherFactory()

    with pytest.raises(KeyError, match="bogus"):
        factory.get_metric_view_fetcher("bogus")


def test_split_fqn() -> None:
    assert _split_fqn("main.sales.orders") == ("main", "sales", "orders")


def test_split_fqn_rejects_non_three_part_names() -> None:
    with pytest.raises(ValueError, match="3-part"):
        _split_fqn("sales.orders")


def test_parse_table_constraints_primary_key() -> None:
    """Parses the stringified DESCRIBE output shape (real workspace sample)."""
    raw = "[(pk_1,PRIMARY KEY (`order_id`))]"

    assert _parse_table_constraints(raw) == [
        {"name": "pk_1", "type": "primary_key", "columns": ["order_id"]},
    ]


def test_parse_table_constraints_composite_pk_and_fk() -> None:
    raw = (
        "[(pk_orders,PRIMARY KEY (`order_id`, `version`)), "
        "(fk_user,FOREIGN KEY (`user_id`) REFERENCES `main`.`sales`.`users` (`id`))]"
    )

    constraints = _parse_table_constraints(raw)

    assert constraints == [
        {"name": "pk_orders", "type": "primary_key", "columns": ["order_id", "version"]},
        {
            "name": "fk_user",
            "type": "foreign_key",
            "columns": ["user_id"],
            "reference_table": "main.sales.users",
            "reference_columns": ["id"],
        },
    ]


def test_parse_table_constraints_ignores_column_options() -> None:
    """Options trailing a backticked column (e.g. TIMESERIES) are dropped."""
    raw = "[(pk_ts,PRIMARY KEY (`event_time` TIMESERIES))]"

    assert _parse_table_constraints(raw) == [
        {"name": "pk_ts", "type": "primary_key", "columns": ["event_time"]},
    ]


def test_parse_table_constraints_handles_missing_or_empty() -> None:
    assert _parse_table_constraints(None) == []
    assert _parse_table_constraints("") == []
    assert _parse_table_constraints("[]") == []


def test_format_column_type_primitive_with_collation() -> None:
    """Real workspace sample: string types carry a collation attribute."""
    assert _format_column_type({"name": "string", "collation": "UTF8_BINARY"}) == "string"


def test_format_column_type_scalar_variants() -> None:
    assert _format_column_type("bigint") == "bigint"
    assert _format_column_type(None) is None
    assert _format_column_type({"name": "int"}) == "int"
    assert _format_column_type({"name": "decimal", "precision": 10, "scale": 2}) == "decimal(10,2)"
    assert _format_column_type({"name": "varchar", "length": 50}) == "varchar(50)"
    assert (
        _format_column_type({"name": "interval", "start_unit": "year", "end_unit": "month"})
        == "interval year to month"
    )


def test_format_column_type_nested() -> None:
    array_of_struct = {
        "name": "array",
        "element_type": {
            "name": "struct",
            "fields": [
                {"name": "a", "type": {"name": "int"}, "nullable": True},
                {"name": "b", "type": {"name": "string", "collation": "UTF8_BINARY"}},
            ],
        },
    }
    assert _format_column_type(array_of_struct) == "array<struct<a:int,b:string>>"

    map_type = {
        "name": "map",
        "key_type": {"name": "string"},
        "value_type": {"name": "decimal", "precision": 18, "scale": 4},
    }
    assert _format_column_type(map_type) == "map<string,decimal(18,4)>"
