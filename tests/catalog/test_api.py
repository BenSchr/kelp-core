"""Tests for catalog API helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from kelp.catalog import api
from kelp.meta.catalog_index import _is_filter_match


class _CatalogIndex:
    """Simple catalog index stub for API tests."""

    def __init__(self) -> None:
        self._items: dict[str, list[SimpleNamespace]] = {
            "models": [
                SimpleNamespace(name="orders", meta={"group": "sales"}),
                SimpleNamespace(name="customers", meta={"group": "crm"}),
            ],
            "functions": [SimpleNamespace(name="normalize_name", meta={"group": "sales"})],
            "metric_views": [SimpleNamespace(name="order_metrics", meta={"group": "sales"})],
            "abacs": [SimpleNamespace(name="pii_policy", meta={"group": "security"})],
        }

    def get_all(self, kind: str) -> list[SimpleNamespace]:
        """Return stubbed catalog items for the given kind."""
        return self._items.get(kind, [])

    def filter_by(
        self,
        kind: str,
        attr: str,
        value: Any,
    ) -> list[SimpleNamespace]:
        """Simple filter matching the real MetaCatalog behaviour."""
        return [
            obj for obj in self.get_all(kind) if _is_filter_match(getattr(obj, attr, None), value)
        ]


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


# ---------- filter_by_meta tests ----------


def test_sync_catalog_with_filter_by_meta(monkeypatch) -> None:
    """sync_catalog should only pass meta-matched objects to the adapter."""
    captured: dict[str, object] = {}
    ctx = SimpleNamespace(catalog_index=_CatalogIndex())

    class StubAdapter:
        def sync_all_functions(self, functions):
            captured["functions"] = functions
            return []

        def sync_all_tables(self, tables, profile=None):
            captured["tables"] = tables
            return []

        def sync_all_metric_views(self, metric_views, profile=None):
            captured["metric_views"] = metric_views
            return []

        def sync_all_abac_policies(self, policies):
            captured["abacs"] = policies
            return []

    monkeypatch.setattr(api, "get_context", lambda: ctx)
    monkeypatch.setattr(api, "UnityCatalogAdapter", StubAdapter)

    api.sync_catalog(
        sync_functions=True,
        sync_tables=True,
        sync_metric_views=True,
        sync_abacs=True,
        filter_by_meta={"group": "sales"},
    )

    tables = cast(list[SimpleNamespace], captured["tables"])
    functions = cast(list[SimpleNamespace], captured["functions"])
    metric_views = cast(list[SimpleNamespace], captured["metric_views"])
    abacs = cast(list[SimpleNamespace], captured["abacs"])

    assert [t.name for t in tables] == ["orders"]
    assert [f.name for f in functions] == ["normalize_name"]
    assert [mv.name for mv in metric_views] == ["order_metrics"]
    assert abacs == []  # pii_policy has group=security


def test_sync_tables_with_filter_by_meta(monkeypatch) -> None:
    """sync_tables should filter by meta when provided."""
    captured: dict[str, object] = {}
    ctx = SimpleNamespace(catalog_index=_CatalogIndex())

    class StubAdapter:
        def sync_tables(self, tables, profile=None):
            captured["tables"] = tables
            return ["ALTER TABLE"]

    monkeypatch.setattr(api, "get_context", lambda: ctx)
    monkeypatch.setattr(api, "UnityCatalogAdapter", StubAdapter)

    queries = api.sync_tables(filter_by_meta={"group": "sales"})
    tables = cast(list[SimpleNamespace], captured["tables"])

    assert queries == ["ALTER TABLE"]
    assert [t.name for t in tables] == ["orders"]


def test_sync_tables_with_names_and_meta_uses_and(monkeypatch) -> None:
    """When both model_names and filter_by_meta are given, they combine with AND."""
    captured: dict[str, object] = {}
    ctx = SimpleNamespace(catalog_index=_CatalogIndex())

    class StubAdapter:
        def sync_tables(self, tables, profile=None):
            captured["tables"] = tables
            return []

    monkeypatch.setattr(api, "get_context", lambda: ctx)
    monkeypatch.setattr(api, "UnityCatalogAdapter", StubAdapter)

    # "customers" exists but has group=crm, not sales → empty result
    api.sync_tables(model_names=["customers"], filter_by_meta={"group": "sales"})
    tables = cast(list[SimpleNamespace], captured["tables"])

    assert tables == []


def test_sync_tables_no_filter_returns_all(monkeypatch) -> None:
    """Without filter_by_meta or names, sync_tables should sync everything."""
    captured: dict[str, object] = {}
    ctx = SimpleNamespace(catalog_index=_CatalogIndex())

    class StubAdapter:
        def sync_tables(self, tables, profile=None):
            captured["tables"] = tables
            return []

    monkeypatch.setattr(api, "get_context", lambda: ctx)
    monkeypatch.setattr(api, "UnityCatalogAdapter", StubAdapter)

    api.sync_tables()
    tables = cast(list[SimpleNamespace], captured["tables"])

    assert len(tables) == 2
