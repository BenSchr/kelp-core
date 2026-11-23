"""Tests for Unity Catalog grant configuration."""

import pytest
from pydantic import ValidationError

from kelp.models.function import KelpFunction
from kelp.models.grant import Grant, merge_grants, normalize_privilege
from kelp.models.metric_view import MetricView
from kelp.models.model import Model


def test_normalize_privilege() -> None:
    assert normalize_privilege("all_privileges") == "ALL PRIVILEGES"
    assert normalize_privilege(" apply   tag ") == "APPLY TAG"


def test_rejects_unsafe_privilege_tokens() -> None:
    with pytest.raises(ValueError, match="Invalid privilege token"):
        Grant(principal="analysts", privileges=["SELECT; DROP TABLE x"])


def test_privileges_are_required_but_null_is_a_revoke_marker() -> None:
    with pytest.raises(ValidationError):
        Grant.model_validate({"principal": "analysts"})

    assert Grant(principal="analysts", privileges=None).privileges == []
    assert Grant(principal="analysts", privileges=[]).privileges == []


def test_merge_grants_unions_and_sorts_by_principal() -> None:
    grants = merge_grants(
        [
            Grant(principal="z_group", privileges=["SELECT"]),
            Grant(principal="a_group", privileges=[]),
            Grant(principal="z_group", privileges=["MODIFY", "SELECT"]),
        ]
    )

    assert grants == [
        Grant(principal="a_group", privileges=[]),
        Grant(principal="z_group", privileges=["MODIFY", "SELECT"]),
    ]


def test_merge_grants_rejects_principal_case_conflicts() -> None:
    with pytest.raises(ValueError, match="differ only by case"):
        merge_grants(
            [
                Grant(principal="Analysts", privileges=["SELECT"]),
                Grant(principal="analysts", privileges=["MODIFY"]),
            ]
        )


@pytest.mark.parametrize(
    "model",
    [
        Model,
        MetricView,
        KelpFunction,
    ],
)
def test_object_models_merge_grants_on_construction(model: type) -> None:
    kwargs = {
        "name": "object",
        "grants": [
            {"principal": "analysts", "privileges": ["SELECT"]},
            {"principal": "analysts", "privileges": ["MODIFY"]},
        ],
    }
    if model is MetricView:
        kwargs["definition"] = {"source": "main.sales.orders"}

    result = model(**kwargs)

    assert result.grants == [Grant(principal="analysts", privileges=["MODIFY", "SELECT"])]
