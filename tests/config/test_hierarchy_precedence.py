"""Tests for hierarchy precedence when applying +config defaults."""

from kelp.utils.dict_parser import apply_cfg_hierarchy_to_dict_recursive


def test_deeper_folder_overrides_upper_defaults() -> None:
    """Folder-level defaults should override top-level defaults for matching paths."""
    cfg = {
        "+catalog": "root_catalog",
        "+schema": "root_schema",
        "abac": {
            "+catalog": "root_catalog_test",
            "+schema": "abac_schema",
        },
    }

    raw_item = {
        "name": "mask_id",
        "origin_file_path": "abac/mask_id.yml",
    }

    result = apply_cfg_hierarchy_to_dict_recursive(
        raw_item,
        cfg,
        tpl_path=raw_item["origin_file_path"],
    )

    assert result["catalog"] == "root_catalog_test"
    assert result["schema"] == "abac_schema"


def test_explicit_values_are_not_overwritten_by_defaults() -> None:
    """Explicit metadata values should remain even when hierarchy defaults exist."""
    cfg = {
        "+catalog": "root_catalog",
        "+schema": "root_schema",
        "abac": {
            "+catalog": "root_catalog_test",
            "+schema": "abac_schema",
        },
    }

    raw_item = {
        "name": "mask_id",
        "catalog": "manual_catalog",
        "schema": "manual_schema",
        "origin_file_path": "abac/mask_id.yml",
    }

    result = apply_cfg_hierarchy_to_dict_recursive(
        raw_item,
        cfg,
        tpl_path=raw_item["origin_file_path"],
    )

    assert result["catalog"] == "manual_catalog"
    assert result["schema"] == "manual_schema"


def test_nested_dict_defaults_merge_with_lower_precedence() -> None:
    """Nested dict defaults should merge and allow lower-level key overrides."""
    cfg = {
        "+tags": {
            "owner": "platform",
            "sensitivity": "medium",
        },
        "abac": {
            "+tags": {
                "sensitivity": "high",
                "domain": "security",
            },
        },
    }

    raw_item = {
        "name": "mask_id",
        "origin_file_path": "abac/mask_id.yml",
    }

    result = apply_cfg_hierarchy_to_dict_recursive(
        raw_item,
        cfg,
        tpl_path=raw_item["origin_file_path"],
    )

    assert result["tags"] == {
        "owner": "platform",
        "sensitivity": "high",
        "domain": "security",
    }
