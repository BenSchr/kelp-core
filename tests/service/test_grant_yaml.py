"""Tests for grant serialization through the model YAML writer."""

from kelp.models.grant import Grant
from kelp.models.model import Model
from kelp.service.yaml_manager import YamlManager


def test_model_to_dict_does_not_inline_hierarchical_grants() -> None:
    model = Model(
        name="orders",
        grants=[Grant(principal="analysts", privileges=["SELECT"])],
    )

    result = YamlManager.model_to_dict(model, include_hierarchy_defaults=False)

    assert "grants" not in result
