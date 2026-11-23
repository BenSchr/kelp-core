"""Tests for grant SQL generation and query-builder integration."""

from kelp.catalog.query_builders._sql import grant_statements
from kelp.catalog.query_builders.factory import UCQueryBuilderFactory
from kelp.catalog.query_builders.function import FunctionQueryBuilder
from kelp.catalog.query_builders.metric_view import MetricViewQueryBuilder
from kelp.catalog.uc_models import GrantDiff, RemoteCatalogConfig, TableDiff
from kelp.models.function import KelpFunction
from kelp.models.grant import Grant
from kelp.models.metric_view import MetricView


def test_grant_statements_order_full_revokes_before_partial_revokes_and_grants() -> None:
    statements = grant_statements(
        GrantDiff(
            revoke_all=["first"],
            revokes={"second": ["MODIFY"]},
            grants={"third": ["APPLY TAG", "SELECT"]},
        ),
        "main.sales.orders",
    )

    assert statements == [
        "REVOKE ALL PRIVILEGES ON main.sales.orders FROM `first`",
        "REVOKE MODIFY ON main.sales.orders FROM `second`",
        "GRANT APPLY TAG, SELECT ON main.sales.orders TO `third`",
    ]


def test_principal_is_escaped_as_a_delimited_identifier() -> None:
    statements = grant_statements(
        GrantDiff(grants={"team`name": ["SELECT"]}),
        "main.sales.orders",
    )

    assert statements == ["GRANT SELECT ON main.sales.orders TO `team``name`"]


def test_table_like_builders_use_bare_object_name_for_grants() -> None:
    factory = UCQueryBuilderFactory()
    diff = TableDiff(grants=GrantDiff(grants={"analysts": ["SELECT"]}))

    for kind in ("managed", "view", "materialized_view", "streaming_table"):
        assert factory.build("main.sales.orders", diff, kind) == [
            "GRANT SELECT ON main.sales.orders TO `analysts`"
        ]


def test_function_builder_uses_function_keyword_and_adds_after_create() -> None:
    function = KelpFunction(
        name="mask",
        catalog="main",
        schema_="security",
        language="SQL",
        body="'x'",
        grants=[Grant(principal="analysts", privileges=["EXECUTE"])],
    )

    statements = FunctionQueryBuilder(RemoteCatalogConfig()).build(function)

    assert statements[0].startswith("CREATE OR REPLACE FUNCTION main.security.mask")
    assert statements[1] == ("GRANT EXECUTE ON FUNCTION main.security.mask TO `analysts`")


def test_metric_view_builder_adds_grants_after_creation() -> None:
    metric_view = MetricView(
        name="order_metrics",
        catalog="main",
        schema_="sales",
        definition={"source": "main.sales.orders"},
        grants=[Grant(principal="analysts", privileges=["SELECT"])],
    )

    statements = MetricViewQueryBuilder(RemoteCatalogConfig()).build(metric_view, None)

    assert statements[0].startswith("CREATE OR REPLACE VIEW main.sales.order_metrics")
    assert statements[-1] == "GRANT SELECT ON main.sales.order_metrics TO `analysts`"
