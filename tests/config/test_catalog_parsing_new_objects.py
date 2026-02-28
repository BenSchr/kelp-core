"""Tests for parsing kelp_functions and kelp_abacs in catalog parsing."""

from pathlib import Path

from kelp.config.catalog import parse_catalog
from kelp.config.catalog_spec import CATALOG_PARSE_SPECS


def test_parse_catalog_with_functions_and_abacs(tmp_path: Path) -> None:
    """Parse functions and ABACs and verify in-memory catalog objects."""
    body_file = tmp_path / "functions" / "normalize_email.sql"
    body_file.parent.mkdir(parents=True, exist_ok=True)
    body_file.write_text("lower(email)", encoding="utf-8")

    raw_functions = [
        {
            "name": "normalize_email",
            "catalog": "main",
            "schema": "default",
            "language": "SQL",
            "returns_data_type": "STRING",
            "body_path": "functions/normalize_email.sql",
            "origin_file_path": "functions/functions.yml",
        }
    ]
    raw_abacs = [
        {
            "name": "mask_ssn",
            "securable_type": "TABLE",
            "securable_name": "main.hr.employees",
            "mode": "COLUMN_MASK",
            "udf_name": "main.security.mask_ssn",
            "target_column": "ssn",
            "origin_file_path": "abac/policies.yml",
        }
    ]

    catalog = parse_catalog(
        raw_objects={
            "kelp_models": [],
            "kelp_metric_views": [],
            "kelp_functions": raw_functions,
            "kelp_abacs": raw_abacs,
        },
        project_object_configs={
            "models": {},
            "metric_views": {},
            "functions": {},
            "abacs": {},
        },
        specs=CATALOG_PARSE_SPECS,
        project_root=str(tmp_path),
    )

    assert len(catalog.get_functions()) == 1
    assert catalog.get_function("normalize_email").body == "lower(email)"

    assert len(catalog.get_abacs()) == 1
    assert catalog.get_abac("mask_ssn").udf_name == "main.security.mask_ssn"
