"""Tests for the generate_alter_statements CLI helper."""

from __future__ import annotations

from kelp.cli.generate_alter_statements import generate_alter_statements


def test_generate_alter_statements_passes_profile_to_sync_catalog(
    monkeypatch,
) -> None:
    """The CLI helper forwards profile to catalog sync."""
    captured: dict[str, object] = {}

    def fake_init(
        project_file_path: str | None = None,
        target: str | None = None,
        manifest_file_path: str | None = None,
        log_level: str | None = None,
    ) -> None:
        captured["init_args"] = {
            "project_file_path": project_file_path,
            "target": target,
            "log_level": log_level,
        }

    def fake_sync_catalog(
        sync_functions: bool = False,
        sync_metric_views: bool = True,
        sync_tables: bool = True,
        sync_abacs: bool = True,
        profile: str | None = None,
    ) -> list[str]:
        captured["sync_catalog_args"] = {
            "sync_functions": sync_functions,
            "sync_metric_views": sync_metric_views,
            "sync_tables": sync_tables,
            "sync_abacs": sync_abacs,
            "profile": profile,
        }
        return ["ALTER TABLE main.sales.orders SET TAGS ('owner'='data')"]

    monkeypatch.setattr("kelp.config.init", fake_init)
    monkeypatch.setattr("kelp.catalog.api.sync_catalog", fake_sync_catalog)
    monkeypatch.setattr("kelp.cli.output.print_message", lambda message: None)
    monkeypatch.setattr("kelp.cli.output.print_warning", lambda message: None)

    generate_alter_statements(
        project_file_path=None,
        target=None,
        profile="analytics",
        output_file=None,
        dry_run=False,
        silent=True,
        debug=False,
    )

    assert captured["init_args"] == {
        "project_file_path": None,
        "target": None,
        "log_level": None,
    }
    assert captured["sync_catalog_args"] == {
        "sync_functions": True,
        "sync_metric_views": True,
        "sync_tables": True,
        "sync_abacs": True,
        "profile": "analytics",
    }
