"""Tests for meta loader and hierarchy utilities."""

from pathlib import Path

from kelp.meta.hierarchy import apply_hierarchy_defaults
from kelp.meta.loader import collect_yaml_file_paths, load_yaml_files_with_jinja_parallel


def test_collect_and_parallel_load_yaml_files(tmp_path: Path) -> None:
    """Loader should collect paths and load objects with origin metadata."""
    models_dir = tmp_path / "models"
    models_dir.mkdir(parents=True)

    file_a = models_dir / "a.yml"
    file_b = models_dir / "b.yaml"

    file_a.write_text(
        """
xy_models:
  - name: model_a
    schema: ${ target }
""",
        encoding="utf-8",
    )
    file_b.write_text(
        """
xy_models:
  - name: model_b
""",
        encoding="utf-8",
    )

    file_paths = collect_yaml_file_paths(models_dir)
    loaded = load_yaml_files_with_jinja_parallel(file_paths, jinja_context={"target": "dev"})

    assert len(file_paths) == 2
    assert len(loaded["xy_models"]) == 2
    assert {item["name"] for item in loaded["xy_models"]} == {"model_a", "model_b"}
    assert {item["origin_file_path"] for item in loaded["xy_models"]} == {
        "a.yml",
        "b.yaml",
    }


def test_apply_hierarchy_defaults_respects_folder_specific_plus_keys() -> None:
    """Folder-specific +defaults should override top-level defaults."""
    cfg = {
        "+catalog": "global_catalog",
        "silver": {
            "+catalog": "silver_catalog",
            "+schema": "silver_schema",
        },
    }

    item = {
        "name": "customers",
        "origin_file_path": "silver/customers.yml",
    }

    result = apply_hierarchy_defaults(
        item,
        cfg,
        origin_file_path=item["origin_file_path"],
    )

    assert result["catalog"] == "silver_catalog"
    assert result["schema"] == "silver_schema"


def test_apply_hierarchy_defaults_deep_three_level_nesting() -> None:
    """Defaults should accumulate correctly over 3+ nested folder levels."""
    cfg = {
        "+catalog": "root_catalog",
        "+schema": "root_schema",
        "bronze": {
            "+schema": "bronze_schema",
            "customers": {
                "+schema": "bronze_customers_schema",
                "premium": {
                    "+schema": "premium_schema",
                    "+table_properties": {"tier": "gold"},
                },
            },
        },
    }

    item = {
        "name": "premium_accounts",
        "origin_file_path": "bronze/customers/premium/accounts.yml",
    }

    result = apply_hierarchy_defaults(
        item,
        cfg,
        origin_file_path=item["origin_file_path"],
    )

    # Deepest matching folder wins for each key
    assert result["catalog"] == "root_catalog"
    assert result["schema"] == "premium_schema"
    assert result["table_properties"] == {"tier": "gold"}


def test_apply_hierarchy_defaults_deep_partial_path_match() -> None:
    """Defaults should only accumulate as far as the path matches the config."""
    cfg = {
        "+catalog": "root_catalog",
        "bronze": {
            "+schema": "bronze_schema",
            "orders": {
                "+schema": "orders_schema",
            },
        },
    }

    # Path goes through bronze/customers — 'customers' is not in the config, so
    # traversal stops at 'bronze' and only bronze defaults apply.
    item = {
        "name": "raw_events",
        "origin_file_path": "bronze/customers/raw_events.yml",
    }

    result = apply_hierarchy_defaults(
        item,
        cfg,
        origin_file_path=item["origin_file_path"],
    )

    assert result["catalog"] == "root_catalog"
    assert result["schema"] == "bronze_schema"


def test_apply_hierarchy_defaults_table_properties_dict_merged() -> None:
    """Hierarchy +table_properties dict should merge with model's own dict without overwriting."""
    cfg = {
        "+table_properties": {
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
        },
    }

    # Model has its own table_properties — existing keys must not be overwritten.
    item = {
        "name": "events",
        "origin_file_path": "bronze/events.yml",
        "table_properties": {
            "pipelines.reset.allowed": "false",
        },
    }

    result = apply_hierarchy_defaults(
        item,
        cfg,
        origin_file_path=item["origin_file_path"],
    )

    props = result["table_properties"]
    assert props["pipelines.reset.allowed"] == "false"
    assert props["delta.autoOptimize.optimizeWrite"] == "true"
    assert props["delta.autoOptimize.autoCompact"] == "true"


def test_apply_hierarchy_defaults_table_properties_model_key_not_overwritten() -> None:
    """A model's own table_property key must not be overwritten by hierarchy default."""
    cfg = {
        "+table_properties": {
            "delta.autoOptimize.optimizeWrite": "true",
        },
    }

    item = {
        "name": "events",
        "origin_file_path": "events.yml",
        "table_properties": {
            "delta.autoOptimize.optimizeWrite": "false",
        },
    }

    result = apply_hierarchy_defaults(
        item,
        cfg,
        origin_file_path=item["origin_file_path"],
    )

    # Model's explicit value must win
    assert result["table_properties"]["delta.autoOptimize.optimizeWrite"] == "false"


def test_apply_hierarchy_defaults_dict_keys_not_overwritten() -> None:
    """Existing dict keys in the model payload should not be overwritten by hierarchy defaults."""
    cfg = {
        "+tags": {
            "owner": "platform",
            "sensitivity": "high",
        },
    }

    item = {
        "name": "pii_events",
        "origin_file_path": "silver/pii_events.yml",
        "tags": {
            "sensitivity": "critical",
        },
    }

    result = apply_hierarchy_defaults(
        item,
        cfg,
        origin_file_path=item["origin_file_path"],
    )

    # 'sensitivity' belongs to the model — must not be overwritten.
    assert result["tags"]["sensitivity"] == "critical"
    # 'owner' was only in the default — must be added.
    assert result["tags"]["owner"] == "platform"


def test_apply_hierarchy_defaults_list_items_not_duplicated() -> None:
    """Hierarchy default list items already present in the model must not be duplicated."""
    cfg = {
        "+owners": ["platform_team", "data_eng"],
    }

    item = {
        "name": "transactions",
        "origin_file_path": "gold/transactions.yml",
        "owners": ["platform_team"],
    }

    result = apply_hierarchy_defaults(
        item,
        cfg,
        origin_file_path=item["origin_file_path"],
    )

    owners = result["owners"]
    # 'platform_team' was already present — must not appear twice.
    assert owners.count("platform_team") == 1
    # 'data_eng' is only in the default — must be added.
    assert "data_eng" in owners


def test_apply_hierarchy_defaults_list_explicit_items_preserved() -> None:
    """Model-explicit list items must survive after hierarchy default merging."""
    cfg = {
        "+owners": ["platform_team"],
    }

    item = {
        "name": "orders",
        "origin_file_path": "silver/orders.yml",
        "owners": ["domain_team", "analytics"],
    }

    result = apply_hierarchy_defaults(
        item,
        cfg,
        origin_file_path=item["origin_file_path"],
    )

    owners = result["owners"]
    assert "domain_team" in owners
    assert "analytics" in owners
    assert "platform_team" in owners


def test_collect_yaml_files_discovers_deeply_nested_structure(tmp_path: Path) -> None:
    """collect_yaml_file_paths should recurse into deeply nested sub-directories."""
    models_dir = tmp_path / "models"

    # Create files at different nesting levels
    (models_dir / "bronze").mkdir(parents=True)
    (models_dir / "bronze" / "customers" / "premium").mkdir(parents=True)
    (models_dir / "silver").mkdir(parents=True)

    (models_dir / "root.yml").write_text("kelp_models:\n  - name: root_model\n", encoding="utf-8")
    (models_dir / "bronze" / "orders.yml").write_text(
        "kelp_models:\n  - name: orders\n", encoding="utf-8"
    )
    (models_dir / "bronze" / "customers" / "premium" / "vip.yml").write_text(
        "kelp_models:\n  - name: vip_customers\n", encoding="utf-8"
    )
    (models_dir / "silver" / "facts.yml").write_text(
        "kelp_models:\n  - name: facts\n", encoding="utf-8"
    )

    file_paths = collect_yaml_file_paths(models_dir)
    loaded = load_yaml_files_with_jinja_parallel(file_paths, jinja_context={})

    names = {item["name"] for item in loaded["kelp_models"]}
    assert names == {"root_model", "orders", "vip_customers", "facts"}

    # origin_file_path should be relative and preserve folder structure
    paths = {item["origin_file_path"] for item in loaded["kelp_models"]}
    assert any(
        "bronze/customers/premium/vip.yml" in p or "bronze\\customers\\premium\\vip.yml" in p
        for p in paths
    )


def test_apply_hierarchy_defaults_root_level_item_gets_top_defaults_only() -> None:
    """A model at the root level should receive only top-level +defaults."""
    cfg = {
        "+catalog": "root_catalog",
        "+schema": "root_schema",
        "bronze": {
            "+schema": "bronze_schema",
        },
    }

    item = {
        "name": "shared_dim",
        "origin_file_path": "shared_dim.yml",
    }

    result = apply_hierarchy_defaults(
        item,
        cfg,
        origin_file_path=item["origin_file_path"],
    )

    assert result["catalog"] == "root_catalog"
    # Root-level item should NOT pick up bronze schema
    assert result["schema"] == "root_schema"
