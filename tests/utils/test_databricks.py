"""Tests for kelp.utils.databricks — column tags are fetched per-column and threaded."""

from types import SimpleNamespace
from typing import cast

from databricks.sdk import WorkspaceClient

from kelp.utils.databricks import (
    get_direct_grants_from_dbx_sdk,
    get_metric_view_from_dbx_sdk,
    get_table_from_dbx_sdk,
)


class _FakeTagAssignments:
    """Fake ``entity_tag_assignments.list(...)`` keyed by (entity_type, entity_name)."""

    def __init__(self, tags_by_entity: dict[tuple[str, str], dict[str, str]]) -> None:
        self._tags_by_entity = tags_by_entity

    def list(self, entity_type: str, entity_name: str):
        tags = self._tags_by_entity.get((entity_type, entity_name), {})
        return [SimpleNamespace(tag_key=k, tag_value=v) for k, v in tags.items()]


class _FakeTables:
    def __init__(self, info: object) -> None:
        self._info = info

    def get(self, fqn: str) -> object:
        return self._info


class _RaisingTables:
    def get(self, fqn: str) -> object:
        raise ValueError("not found")


class _FakeWorkspaceClient:
    def __init__(
        self, tables: object, tags_by_entity: dict[tuple[str, str], dict[str, str]]
    ) -> None:
        self.tables = tables
        self.entity_tag_assignments = _FakeTagAssignments(tags_by_entity)


class _FakeRawApiClient:
    def __init__(self, responses: list[dict]) -> None:
        self._responses = iter(responses)
        self.calls: list[tuple[str, str, dict | None]] = []

    def do(self, method: str, path: str, *, query: dict | None = None) -> dict:
        self.calls.append((method, path, query))
        return next(self._responses)


class _RaisingRawApiClient:
    def do(self, method: str, path: str, *, query: dict | None = None) -> dict:
        raise ValueError("not found")


def _fake_grant_client(responses: list[dict]) -> WorkspaceClient:
    client = SimpleNamespace(api_client=_FakeRawApiClient(responses))
    return cast(WorkspaceClient, client)


def _fake_raw_table_client(
    api_client: object, tags_by_entity: dict[tuple[str, str], dict[str, str]]
) -> WorkspaceClient:
    client = SimpleNamespace(
        api_client=api_client,
        entity_tag_assignments=_FakeTagAssignments(tags_by_entity),
    )
    return cast(WorkspaceClient, client)


def _fake_client(
    tables: object, tags_by_entity: dict[tuple[str, str], dict[str, str]]
) -> WorkspaceClient:
    """Build a fake client typed as WorkspaceClient for the functions under test."""
    return cast(WorkspaceClient, _FakeWorkspaceClient(tables, tags_by_entity))


def _column(
    name: str,
    *,
    comment: str | None = None,
    type_text: str = "string",
    nullable: bool = True,
    partition_index: int | None = None,
    type_json: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        comment=comment,
        type_text=type_text,
        nullable=nullable,
        partition_index=partition_index,
        type_json=type_json,
    )


def test_get_table_from_dbx_sdk_fetches_tags_per_column() -> None:
    info = SimpleNamespace(
        name="orders",
        catalog_name="main",
        schema_name="sales",
        table_type=SimpleNamespace(value="MANAGED"),
        comment="Orders table",
        properties={},
        columns=[_column("id"), _column("email")],
        table_constraints=[],
    )
    w = _fake_client(
        _FakeTables(info),
        tags_by_entity={
            ("tables", "main.sales.orders"): {"owner": "data"},
            ("columns", "main.sales.orders.id"): {"pii": "false"},
            ("columns", "main.sales.orders.email"): {"pii": "true"},
        },
    )

    model = get_table_from_dbx_sdk("main.sales.orders", w=w)

    assert model is not None
    assert model.tags == {"owner": "data"}
    columns_by_name = {c.name: c for c in model.columns}
    assert columns_by_name["id"].tags == {"pii": "false"}
    assert columns_by_name["email"].tags == {"pii": "true"}


def test_get_table_from_dbx_sdk_returns_none_when_not_found() -> None:
    w = _fake_client(_RaisingTables(), tags_by_entity={})

    assert get_table_from_dbx_sdk("main.sales.missing", w=w) is None


def test_get_metric_view_from_dbx_sdk_fetches_column_tags_and_injects_into_definition() -> None:
    info = {
        "name": "order_metrics",
        "catalog_name": "main",
        "schema_name": "sales",
        "comment": "Order metrics",
        "view_definition": (
            "dimensions:\n"
            "  - expr: source.* EXCEPT (region)\n"
            "  - name: region\n"
            "    expr: region\n"
            "measures:\n"
            "  - name: revenue\n"
            "    expr: sum(amount)\n"
            "source: main.sales.orders\n"
        ),
        "columns": [
            {
                "name": "region",
                "comment": "Region dimension",
                "metadata": {
                    "display_name": "Region",
                    "synonyms": ["territory"],
                    "unrelated": "ignored",
                },
            },
            {
                "name": "revenue",
                "comment": "Revenue measure",
                "metadata": {"format": {"type": "currency"}},
            },
        ],
    }
    api_client = _FakeRawApiClient([info])
    w = _fake_raw_table_client(
        api_client,
        tags_by_entity={
            ("tables", "main.sales.order_metrics"): {"owner": "data"},
            ("columns", "main.sales.order_metrics.region"): {"category": "geo"},
            ("columns", "main.sales.order_metrics.revenue"): {"sensitive": "true"},
        },
    )

    metric_view = get_metric_view_from_dbx_sdk("main.sales.order_metrics", w=w)

    assert metric_view is not None
    assert api_client.calls == [
        ("GET", "/api/2.1/unity-catalog/tables/main.sales.order_metrics", None)
    ]
    assert metric_view.tags == {"owner": "data"}
    # The UC object comment is mirrored into the definition (spec-verbatim state)
    assert metric_view.definition["comment"] == "Order metrics"
    # The remote 'dimensions' synonym is canonicalized to 'fields'
    assert "dimensions" not in metric_view.definition
    assert metric_view.definition["fields"][0] == {"expr": "source.* EXCEPT (region)"}
    fields = {f["name"]: f for f in metric_view.definition["fields"] if "name" in f}
    measures = {m["name"]: m for m in metric_view.definition["measures"]}
    assert fields["region"]["tags"] == {"category": "geo"}
    assert fields["region"]["comment"] == "Region dimension"
    assert fields["region"]["display_name"] == "Region"
    assert fields["region"]["synonyms"] == ["territory"]
    assert "unrelated" not in fields["region"]
    assert measures["revenue"]["tags"] == {"sensitive": "true"}
    assert measures["revenue"]["format"] == {"type": "currency"}


def test_get_metric_view_from_dbx_sdk_returns_none_when_not_found() -> None:
    w = _fake_raw_table_client(_RaisingRawApiClient(), tags_by_entity={})

    assert get_metric_view_from_dbx_sdk("main.sales.missing", w=w) is None


def test_get_direct_grants_from_sdk_preserves_unknown_privileges_and_paginates() -> None:
    w = _fake_grant_client(
        [
            {
                "privilege_assignments": [
                    {"principal": "analysts", "privileges": ["SELECT", "INSERT"]}
                ],
                "next_page_token": "next",
            },
            {
                "privilege_assignments": [
                    {"principal": "analysts", "privileges": ["MODIFY"]},
                    {"principal": "admins", "privileges": ["ALL_PRIVILEGES"]},
                ]
            },
        ]
    )

    grants = get_direct_grants_from_dbx_sdk("table", "main.sales.orders", w=w)

    assert grants is not None
    assert [(grant.principal, grant.privileges) for grant in grants] == [
        ("admins", ["ALL PRIVILEGES"]),
        ("analysts", ["INSERT", "MODIFY", "SELECT"]),
    ]
    calls = w.api_client.calls
    assert calls == [
        (
            "GET",
            "/api/2.1/unity-catalog/permissions/table/main.sales.orders",
            {"max_results": 0},
        ),
        (
            "GET",
            "/api/2.1/unity-catalog/permissions/table/main.sales.orders",
            {"max_results": 0, "page_token": "next"},
        ),
    ]
