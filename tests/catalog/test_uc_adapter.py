"""Tests for Unity Catalog adapter profile plumbing."""

from __future__ import annotations

from kelp.catalog.uc_adapter import UnityCatalogAdapter
from kelp.catalog.uc_models import RemoteCatalogConfig, TableDiff
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
        "kelp.catalog.uc_adapter.get_table_from_dbx_sdk",
        fake_get_table_from_dbx_sdk,
    )
    monkeypatch.setattr(
        "kelp.catalog.uc_adapter.UCQueryBuilderFactory",
        lambda: StubBuilderFactory(),
    )

    diff = TableDiff()

    def fake_calculate(current: Model, remote_state: Model) -> TableDiff:
        del current, remote_state
        return diff

    monkeypatch.setattr(adapter._differ, "calculate", fake_calculate)

    queries = adapter.sync_table(local, profile="analytics")

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
        description="Order metrics",
        definition={"source": "main.sales.orders"},
        tags={"owner": "data"},
    )
    remote = MetricView(
        name="order_metrics",
        catalog="main",
        schema_="sales",
        description="Order metrics",
        definition={"source": "main.sales.orders"},
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
        "kelp.utils.databricks.get_metric_view_from_dbx_sdk",
        fake_get_metric_view_from_dbx_sdk,
    )

    queries = adapter.sync_metric_view(metric_view, profile="analytics")

    assert queries == []
    assert captured["fqn"] == "main.sales.order_metrics"
    assert captured["profile"] == "analytics"
