"""Unit tests for the materialization config union."""

import pytest
from pydantic import ValidationError

from kelp.models.model import Model
from kelp.models.model_mat_config import (
    AppendConfig,
    ColumnSelector,
    MergeConfig,
    OverwriteConfig,
    Scd2Config,
    parse_materialization_config,
    resolve_config,
)


def test_mode_selects_variant() -> None:
    """The discriminator picks the matching config class."""
    assert isinstance(parse_materialization_config({"mode": "append"}), AppendConfig)
    assert isinstance(parse_materialization_config({"mode": "overwrite"}), OverwriteConfig)
    assert isinstance(parse_materialization_config({"mode": "merge", "keys": ["id"]}), MergeConfig)
    assert isinstance(
        parse_materialization_config(
            {"mode": "scd2", "keys": ["id"], "sequence_by": ["updated_at"]}
        ),
        Scd2Config,
    )


def test_missing_mode_defaults_to_append() -> None:
    """A materialization block without a mode means append."""
    assert parse_materialization_config({}) == AppendConfig()
    assert parse_materialization_config({"options": {"mergeSchema": "true"}}) == AppendConfig(
        options={"mergeSchema": "true"}
    )


def test_model_yaml_parses_materialization_block() -> None:
    """Model metadata resolves the block into the matching config class."""
    model = Model.model_validate(
        {"name": "orders", "materialization": {"mode": "merge", "keys": ["id"]}}
    )
    assert isinstance(model.materialization, MergeConfig)
    assert model.materialization.keys == ["id"]

    defaulted = Model.model_validate(
        {"name": "orders", "materialization": {"options": {"mergeSchema": "true"}}}
    )
    assert isinstance(defaulted.materialization, AppendConfig)


def test_model_yaml_rejects_invalid_materialization_block() -> None:
    """A mode's missing requirement fails when the model is loaded, not at write time."""
    with pytest.raises(ValidationError, match="keys"):
        Model.model_validate({"name": "orders", "materialization": {"mode": "merge"}})


def test_keys_are_required_for_merge_modes() -> None:
    """Merge and scd2 cannot be configured without keys."""
    with pytest.raises(ValidationError, match="keys"):
        parse_materialization_config({"mode": "merge"})
    with pytest.raises(ValidationError, match="keys"):
        parse_materialization_config({"mode": "scd2", "sequence_by": ["ts"]})


def test_scd2_requires_sequence_by() -> None:
    """SCD2 history is undefined without an ordering."""
    with pytest.raises(ValidationError, match="sequence_by"):
        parse_materialization_config({"mode": "scd2", "keys": ["id"]})


def test_merge_only_fields_rejected_on_append() -> None:
    """Fields that do not apply to the mode are rejected instead of ignored."""
    with pytest.raises(ValidationError):
        parse_materialization_config({"mode": "append", "keys": ["id"]})


def test_column_selector_is_exclusive() -> None:
    """A selector cannot include and exclude at the same time."""
    with pytest.raises(ValidationError, match="only one of"):
        ColumnSelector(include=["a"], exclude=["b"])


@pytest.mark.parametrize(
    ("candidates", "selector", "required", "expected"),
    [
        (["id", "a", "b"], ColumnSelector(), None, ["id", "a", "b"]),
        (["id", "a", "b"], ColumnSelector(include=["A"]), None, ["a"]),
        (["id", "a", "b"], ColumnSelector(include=["a"]), ["ID"], ["a", "id"]),
        (["id", "a", "b"], ColumnSelector(exclude=["B"]), None, ["id", "a"]),
        (["id", "a", "b"], ColumnSelector(exclude=["id"]), ["id"], ["a", "b", "id"]),
    ],
)
def test_column_selector_apply(
    candidates: list[str],
    selector: ColumnSelector,
    required: list[str] | None,
    expected: list[str],
) -> None:
    """Selection is case-insensitive, order preserving, and honours required columns."""
    assert selector.apply(candidates, required=required) == expected


def test_runtime_config_replaces_the_model_config() -> None:
    """A runtime config is used as-is; model fields never leak into it."""
    metadata = MergeConfig(keys=["id"], sequence_by=["ts"], ignore_null_updates=True)
    override = MergeConfig(keys=["id"])

    resolved = resolve_config(metadata, override)

    assert resolved is override
    assert resolved.sequence_by == []
    assert resolved.ignore_null_updates is False


def test_model_config_is_used_when_no_override() -> None:
    """Without a runtime config the model's config applies."""
    metadata = MergeConfig(keys=["id"])
    assert resolve_config(metadata, None) is metadata


def test_resolve_config_defaults_to_append() -> None:
    """No metadata and no override keeps the append default."""
    assert resolve_config(None, None) == AppendConfig()
