"""Tests for catalog API helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from kelp.catalog import api


class _CatalogIndex:
    """Simple catalog index stub for API tests."""

    def __init__(self) -> None:
        self._items = {
            "models": [
                SimpleNamespace(name="orders"),
                SimpleNamespace(name="customers"),
            ],
            "functions": [SimpleNamespace(name="normalize_name")],
            "metric_views": [SimpleNamespace(name="order_metrics")],
            "abacs": [SimpleNamespace(name="pii_policy")],
        }

    def get_all(self, kind: str) -> list[SimpleNamespace]:
        """Return stubbed catalog items for the given kind."""
        return self._items[kind]


def test_sync_catalog_passes_profile_to_remote_fetch_paths(monkeypatch) -> None:
    """sync_catalog forwards profile to table and metric-view sync paths."""
    captured: dict[str, object] = {}
    ctx = SimpleNamespace(catalog_index=_CatalogIndex())

    class StubAdapter:
        """Capture adapter calls made by sync_catalog."""

        def sync_all_functions(self, functions: list[SimpleNamespace]) -> list[str]:
            captured["functions"] = functions
            return ["CREATE FUNCTION"]

        def sync_all_tables(
            self,
            tables: list[SimpleNamespace],
            profile: str | None = None,
        ) -> list[str]:
            captured["tables"] = tables
            captured["tables_profile"] = profile
            return ["ALTER TABLE"]

        def sync_all_metric_views(
            self,
            metric_views: list[SimpleNamespace],
            profile: str | None = None,
        ) -> list[str]:
            captured["metric_views"] = metric_views
            captured["metric_views_profile"] = profile
            return ["ALTER VIEW"]

        def sync_all_abac_policies(self, policies: list[SimpleNamespace]) -> list[str]:
            captured["abacs"] = policies
            return ["CREATE POLICY"]

    monkeypatch.setattr(api, "get_context", lambda: ctx)
    monkeypatch.setattr(api, "UnityCatalogAdapter", StubAdapter)

    queries = api.sync_catalog(sync_functions=True, profile="analytics")

    assert queries == [
        "CREATE FUNCTION",
        "ALTER TABLE",
        "ALTER VIEW",
        "CREATE POLICY",
    ]
    assert captured["tables_profile"] == "analytics"
    assert captured["metric_views_profile"] == "analytics"


def test_sync_tables_passes_profile_to_adapter(monkeypatch) -> None:
    """sync_tables forwards the requested profile to the adapter."""
    captured: dict[str, object] = {}
    ctx = SimpleNamespace(catalog_index=_CatalogIndex())

    class StubAdapter:
        """Capture table sync calls."""

        def sync_tables(
            self,
            tables: list[SimpleNamespace],
            profile: str | None = None,
        ) -> list[str]:
            captured["tables"] = tables
            captured["profile"] = profile
            return ["ALTER TABLE"]

    monkeypatch.setattr(api, "get_context", lambda: ctx)
    monkeypatch.setattr(api, "UnityCatalogAdapter", StubAdapter)

    queries = api.sync_tables(model_names=["orders"], profile="analytics")
    tables = cast(list[SimpleNamespace], captured["tables"])

    assert queries == ["ALTER TABLE"]
    assert [table.name for table in tables] == ["orders"]
    assert captured["profile"] == "analytics"


def test_sync_metric_views_passes_profile_to_adapter(monkeypatch) -> None:
    """sync_metric_views forwards the requested profile to the adapter."""
    captured: dict[str, object] = {}
    ctx = SimpleNamespace(catalog_index=_CatalogIndex())

    class StubAdapter:
        """Capture metric-view sync calls."""

        def sync_all_metric_views(
            self,
            metric_views: list[SimpleNamespace],
            profile: str | None = None,
        ) -> list[str]:
            captured["metric_views"] = metric_views
            captured["profile"] = profile
            return ["ALTER VIEW"]

    monkeypatch.setattr(api, "get_context", lambda: ctx)
    monkeypatch.setattr(api, "UnityCatalogAdapter", StubAdapter)

    queries = api.sync_metric_views(view_names=["order_metrics"], profile="analytics")
    metric_views = cast(list[SimpleNamespace], captured["metric_views"])

    assert queries == ["ALTER VIEW"]
    assert [view.name for view in metric_views] == ["order_metrics"]
    assert captured["profile"] == "analytics"
