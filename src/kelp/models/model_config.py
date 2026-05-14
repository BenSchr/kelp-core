from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ModelConfig(BaseModel):
    """Configuration controlling how a kelp model is materialized.

    All fields are optional; set only what you need.

    Args:
        write_mode: How to write the result — ``'append'``, ``'overwrite'``,
            ``'view'``, or ``'merge'``.  When ``None`` (default) the DataFrame
            is returned without writing.
        options: Extra DataFrameWriter options
            (e.g. ``{'mergeSchema': 'true'}``).
        merge_condition: SQL join predicate between ``source`` and ``target``
            aliases, e.g. ``"source.id = target.id"``.
            Required when ``write_mode='merge'``.
        when_matched_update_all: Update all columns for matched rows.
        when_not_matched_insert_all: Insert all columns for unmatched source rows.
        when_not_matched_by_source_delete: Delete target rows not found in source.
    """

    write_mode: Literal["append", "overwrite", "view", "merge"] | None = Field(
        default=None,
        description=(
            "How to write the model: 'append', 'overwrite', 'view', or 'merge'. "
            "When None the DataFrame is returned without writing."
        ),
    )
    table_format: str | None = Field(
        default="delta",
        description=(
            "Table format to use when writing (e.g. 'delta', 'iceberg', 'parquet'). "
            "Only applicable when write_mode is not None. If None, the default format for the target catalog is used."
        ),
    )
    options: dict = Field(
        default_factory=dict,
        description="Extra DataFrameWriter options (e.g. {'mergeSchema': 'true'}).",
    )
    merge_condition: str | None = Field(
        default=None,
        description=(
            "SQL join predicate between 'source' and 'target' aliases. "
            "Required when write_mode='merge'."
        ),
    )
    when_matched_update_all: bool = Field(
        default=True,
        description="Update all columns for rows matched by the merge condition.",
    )
    when_matched_update: dict | None = Field(
        default=None,
        description=(
            "Custom column updates for rows matched by the merge condition, "
            "e.g. {'col1': 'source.col1', 'col2': 'target.col2 + source.col2'}. "
            "Overrides when_matched_update_all if both are set."
        ),
    )
    when_not_matched_insert_all: bool = Field(
        default=True,
        description="Insert all columns for source rows not matched by the merge condition.",
    )
    when_not_matched_by_source_delete: bool = Field(
        default=False,
        description="Delete target rows that have no matching source row.",
    )
    merge_with_schema_evolution: bool = Field(
        default=True,
        description=(
            "When True, allows schema evolution during merge operations. "
            "Requires that the underlying table format supports schema evolution."
        ),
    )
