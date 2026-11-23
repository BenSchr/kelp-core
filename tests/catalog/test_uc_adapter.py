"""Tests for Unity Catalog adapter profile plumbing."""

from kelp.catalog.uc_adapter import UnityCatalogAdapter
from kelp.catalog.uc_models import RemoteCatalogConfig, TableDiff
from kelp.models.function import KelpFunction
from kelp.models.grant import Grant
from kelp.models.metric_view import MetricView
from kelp.models.model import Model


def test_sync_table_passes_profile_to_databricks_lookup(monkeypatch) -> None:
    """sync_table forwards profile to the Databricks table lookup helper."""
    captured: dict[str, object] = {}
    local = Model(name="orders", catalog="main", schema_="sales")
    remote = Model(name="orders", catalog="main", schema_="sales")
    adapter = UnityCatalogAdapter(config=RemoteCatalogConfig())

    def fake_get_table_from_dbx_sdk(
        fqn: str,
        w=None,
        profile: str | None = None,
    ) -> Model:
        captured["fqn"] = fqn
        captured["profile"] = profile
        return remote

    class StubBuilderFactory:
        """Minimal builder factory stub."""

        def build(self, fqn: str, diff: object, table_type: str) -> list[str]:
            captured["builder_args"] = (fqn, diff, table_type)
            return ["ALTER TABLE main.sales.orders SET TAGS ('owner'='data')"]

    monkeypatch.setattr(
        "kelp.catalog.remote_fetchers.sdk.get_table_from_dbx_sdk",
        fake_get_table_from_dbx_sdk,
    )
    monkeypatch.setattr(
        "kelp.catalog.uc_adapter.UCQueryBuilderFactory",
        lambda: StubBuilderFactory(),
    )

    diff = TableDiff()

    def fake_calculate(
        current: Model,
        remote_state: Model,
        actual_grants: list[Grant] | None = None,
    ) -> TableDiff:
        del current, remote_state
        assert actual_grants is None
        return diff

    monkeypatch.setattr(adapter._differ, "calculate", fake_calculate)

    queries = adapter.sync_table(local, profile="analytics", engine="sdk")

    assert queries == ["ALTER TABLE main.sales.orders SET TAGS ('owner'='data')"]
    assert captured["fqn"] == "main.sales.orders"
    assert captured["profile"] == "analytics"
    assert captured["builder_args"] == ("main.sales.orders", diff, "managed")


def test_sync_metric_view_passes_profile_to_databricks_lookup(monkeypatch) -> None:
    """sync_metric_view forwards profile to the Databricks metric-view lookup helper."""
    captured: dict[str, object] = {}
    metric_view = MetricView(
        name="order_metrics",
        catalog="main",
        schema_="sales",
        definition={"source": "main.sales.orders", "comment": "Order metrics"},
        tags={"owner": "data"},
    )
    remote = MetricView(
        name="order_metrics",
        catalog="main",
        schema_="sales",
        definition={"source": "main.sales.orders", "comment": "Order metrics"},
        tags={"owner": "data"},
    )
    adapter = UnityCatalogAdapter(config=RemoteCatalogConfig())

    def fake_get_metric_view_from_dbx_sdk(
        fqn: str,
        w=None,
        profile: str | None = None,
    ) -> MetricView:
        captured["fqn"] = fqn
        captured["profile"] = profile
        return remote

    monkeypatch.setattr(
        "kelp.catalog.remote_fetchers.sdk.get_metric_view_from_dbx_sdk",
        fake_get_metric_view_from_dbx_sdk,
    )

    queries = adapter.sync_metric_view(metric_view, profile="analytics", engine="sdk")

    assert queries == []
    assert captured["fqn"] == "main.sales.order_metrics"
    assert captured["profile"] == "analytics"


def test_sync_metric_view_creates_when_remote_missing(monkeypatch) -> None:
    """sync_metric_view switches to create mode when the remote lookup returns None."""
    metric_view = MetricView(
        name="order_metrics",
        catalog="main",
        schema_="sales",
        definition={"source": "main.sales.orders", "comment": "Order metrics"},
        tags={"owner": "data"},
    )
    adapter = UnityCatalogAdapter(config=RemoteCatalogConfig())

    monkeypatch.setattr(
        "kelp.catalog.remote_fetchers.sdk.get_metric_view_from_dbx_sdk",
        lambda fqn, w=None, profile=None: None,
    )

    queries = adapter.sync_metric_view(metric_view, engine="sdk")

    assert len(queries) == 2
    assert queries[0].startswith("CREATE OR REPLACE VIEW main.sales.order_metrics")
    assert "ALTER VIEW main.sales.order_metrics SET TAGS ('owner' = 'data')" in queries[1]


def test_sync_table_fetches_grants_only_when_configured(monkeypatch) -> None:
    table = Model(
        name="orders",
        catalog="main",
        schema_="sales",
        grants=[Grant(principal="analysts", privileges=["SELECT"])],
    )
    remote = Model(name="orders", catalog="main", schema_="sales")
    captured: dict[str, object] = {}

    class FakeFetcherFactory:
        def get_table_fetcher(self, engine: str):
            del engine
            return self

        def get_grant_fetcher(self, engine: str):
            del engine
            return self

        def fetch(self, *args, **kwargs):
            if args and args[0] == "table":
                captured["grant_fetch"] = (args, kwargs)
                return []
            return remote

    class FakeBuilderFactory:
        def build(self, fqn: str, diff: TableDiff, table_type: str) -> list[str]:
            captured["diff"] = diff
            return []

    monkeypatch.setattr(
        "kelp.catalog.uc_adapter.RemoteFetcherFactory",
        FakeFetcherFactory,
    )
    monkeypatch.setattr(
        "kelp.catalog.uc_adapter.UCQueryBuilderFactory",
        lambda: FakeBuilderFactory(),
    )

    UnityCatalogAdapter(config=RemoteCatalogConfig()).sync_table(
        table,
        profile="analytics",
        engine="sdk",
    )

    assert captured["grant_fetch"] == (
        ("table", "main.sales.orders"),
        {"profile": "analytics"},
    )


def test_sync_function_fetches_grants_and_forwards_profile(monkeypatch) -> None:
    function = KelpFunction(
        name="mask",
        catalog="main",
        schema_="security",
        language="SQL",
        body="'x'",
        grants=[Grant(principal="analysts", privileges=["EXECUTE"])],
    )
    captured: dict[str, object] = {}

    class FakeFetcherFactory:
        def get_grant_fetcher(self, engine: str):
            captured["engine"] = engine
            return self

        def fetch(self, *args, **kwargs):
            captured["fetch"] = (args, kwargs)
            return []

    monkeypatch.setattr(
        "kelp.catalog.uc_adapter.RemoteFetcherFactory",
        FakeFetcherFactory,
    )

    statements = UnityCatalogAdapter(config=RemoteCatalogConfig()).sync_function(
        function,
        profile="analytics",
        engine="sdk",
    )

    assert statements[0].startswith("CREATE OR REPLACE FUNCTION")
    assert statements[1] == "GRANT EXECUTE ON FUNCTION main.security.mask TO `analysts`"
    assert captured["engine"] == "sdk"
    assert captured["fetch"] == (
        ("function", "main.security.mask"),
        {"profile": "analytics"},
    )
