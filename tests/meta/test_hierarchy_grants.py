"""Tests for metadata hierarchy list extension keys."""

from kelp.meta.catalog import build_catalog
from kelp.meta.spec import MetaObjectSpec
from kelp.models.grant import Grant
from kelp.models.model import Model


def test_catalog_spec_extends_grants_at_every_level_only() -> None:
    spec = MetaObjectSpec(
        root_key="kelp_models",
        project_config_key="models",
        path_attr="models_path",
        catalog_attr="models",
        model_class=Model,
        model_label="Model",
        extend_list_keys=("grants",),
    )
    settings = {
        "models": {
            "+grants": [{"principal": "engineering", "privileges": ["MODIFY"]}],
            "+cluster_by": ["root_col"],
            "gold": {
                "+grants": [{"principal": "analysts", "privileges": ["SELECT"]}],
                "+cluster_by": ["gold_col"],
                "regional": {
                    "+grants": [{"principal": "regional", "privileges": ["SELECT"]}],
                },
            },
        }
    }
    raw_objects = {
        "kelp_models": [
            {
                "name": "orders",
                "origin_file_path": "models/gold/regional/orders.yml",
                "grants": [{"principal": "analysts", "privileges": ["APPLY TAG"]}],
                "cluster_by": ["model_col"],
            }
        ]
    }

    model = build_catalog(
        raw_objects=raw_objects,
        project_settings=settings,
        object_specs=(spec,),
    )["models"][0]

    assert model.grants == [
        Grant(principal="analysts", privileges=["APPLY TAG", "SELECT"]),
        Grant(principal="engineering", privileges=["MODIFY"]),
        Grant(principal="regional", privileges=["SELECT"]),
    ]
    assert model.cluster_by == ["gold_col", "model_col"]


def test_null_grants_at_any_level_add_nothing() -> None:
    spec = MetaObjectSpec(
        root_key="kelp_models",
        project_config_key="models",
        path_attr="models_path",
        catalog_attr="models",
        model_class=Model,
        model_label="Model",
        extend_list_keys=("grants",),
    )
    settings = {
        "models": {
            "+grants": [{"principal": "engineering", "privileges": ["MODIFY"]}],
            "gold": {"+grants": None},
        }
    }
    raw_objects = {
        "kelp_models": [
            {
                "name": "orders",
                "origin_file_path": "models/gold/orders.yml",
                "grants": None,
            }
        ]
    }

    model = build_catalog(
        raw_objects=raw_objects,
        project_settings=settings,
        object_specs=(spec,),
    )["models"][0]

    assert model.grants == [Grant(principal="engineering", privileges=["MODIFY"])]
