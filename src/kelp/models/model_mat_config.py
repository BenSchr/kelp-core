from typing import Literal

from pydantic import BaseModel, Field


class ModelMaterializationConfig(BaseModel):
    """Configuration controlling how a kelp model is materialized.

    All fields are optional; set only what you need.

    Args:
        write_mode: How to write the result — ``'append'``, ``'overwrite'``, or ``'merge'``.
        When ``None`` (default) the DataFrame
            is returned without writing.
        options: Extra DataFrameWriter options
            (e.g. ``{'mergeSchema': 'true'}``).
        unique_keys: List of unique key columns for merge operations. Required when ``write_mode='merge'``.
        target_alias: Alias to use for the target table in merge conditions. Default is ``'target'``.
        source_alias: Alias to use for the source table in merge conditions. Default is ``'source'``.
        predicates: Extra predicates to prefilter source and target data before the merge condition is applied.
        matched_condition: Additional SQL condition to apply to matched rows in a merge.
        matched_condition_exclude_cols: List of columns to exclude from the matched condition in a merge. Used to prevent certain columns from being evaluated in the matched condition, which can be useful for handling cases where certain columns may have non-deterministic values or should not be considered for matching.
        matched_condition_include_cols: List of columns to include in the matched condition in a merge. Used to specify a subset of columns to be evaluated in the matched condition, which can be useful for optimizing the merge operation by only considering relevant columns for matching.
        not_matched_condition: Additional SQL condition to apply to unmatched source rows in a merge.
        not_matched_by_source_condition: Additional SQL condition to apply to target rows not found in source in a merge.
        not_matched_by_source_action: Action to take for target rows not found in source in a merge. If ``'delete'`` rows will be deleted, else a update with the provided condition will be performed. Only applicable when ``write_mode='merge'``.
        matched_update_exclude_cols: List of columns to exclude from updates in a merge. Only applicable when ``write_mode='merge'``. Used to prevent certain columns from being updated during a merge operation.
        matched_update_include_cols: List of columns to include in updates in a merge. Only applicable when ``write_mode='merge'``. Used to specify a subset of columns to be updated during a merge operation.
        merge_with_schema_evolution: Whether to enable schema evolution during merge operations. Only applicable when ``write_mode='merge'``.
        exclude_at_target_cols: List of columns to exclude from the target dataset during a merge operation. Only applicable when ``write_mode='merge'.
        include_at_target_cols: List of columns to include from the target dataset during a merge operation. Only applicable when ``write_mode='merge'``.
        sequence_by: List of columns to order source and target datasets by before applying the merge condition. Only applicable when ``write_mode='merge'``.
        ignore_null_updates: Whether to ignore updates to columns where the source value is null during a merge operation. Only applicable when ``write_mode='merge'``.
        apply_as_delete: Used for CDC scenarios. Whether to apply a condition as a delete operation instead of an update or insert during a merge. Only applicable when ``write_mode='merge'``.
        prevent_full_refresh: Whether to prevent full refreshes for this model. If True, attempts to perform a full refresh (e.g. via the ``'full_refresh'`` flag in the materialization context) will
            be overridden to perform an append or merge instead, and a warning will be logged. This can be useful for protecting critical tables from accidental full refreshes.
    """

    write_mode: Literal["append", "overwrite", "merge"] | None = Field(
        default=None,
        description=(
            "How to write the model: 'append', 'overwrite', or 'merge'. "
            "When None the DataFrame is returned without writing."
        ),
    )
    options: dict = Field(
        default_factory=dict,
        description="Extra DataFrameWriter options (e.g. {'mergeSchema': 'true'}).",
    )
    ### merge-specific options
    unique_keys: list[str] | None = Field(
        default=None,
        description=(
            "List of unique key columns for merge operations. Required when write_mode='merge'. "
            "Used to validate the merge condition and ensure it references a unique key."
        ),
    )
    target_alias: str = Field(
        default="target",
        description="Alias to use for the target table in merge conditions. Default is 'target'.",
    )
    source_alias: str = Field(
        default="source",
        description="Alias to use for the source table in merge conditions. Default is 'source'.",
    )
    predicates: str | None = Field(
        default=None,
        description=(
            "Extra predicates to prefilter source and target data before the merge condition is applied."
        ),
    )
    matched_condition: str | None = Field(
        default=None,
        description=("Additional SQL condition to apply to matched rows in a merge. "),
    )
    matched_condition_exclude_cols: list[str] | None = Field(
        default=None,
        description=(
            "List of columns to exclude from the matched condition in a merge. Used to prevent certain columns from being evaluated in the matched condition, which can be useful for handling cases where certain columns may have non-deterministic values or should not be considered for matching."
        ),
    )
    matched_condition_include_cols: list[str] | None = Field(
        default=None,
        description=(
            "List of columns to include in the matched condition in a merge. Used to specify a subset of columns to be evaluated in the matched condition, which can be useful for optimizing the merge operation by only considering relevant columns for matching."
        ),
    )
    not_matched_condition: str | None = Field(
        default=None,
        description=("Additional SQL condition to apply to unmatched source rows in a merge. "),
    )
    not_matched_by_source_condition: str | None = Field(
        default=None,
        description=(
            "Additional SQL condition to apply to target rows not found in source in a merge. "
        ),
    )
    not_matched_by_source_action: str | None = Field(
        default=None,
        description=(
            "Action to take for target rows not found in source in a merge. If 'delete' rows will be deleted, else a update with the provided condition will be performed. Only applicable when write_mode='merge'. "
        ),
    )
    matched_update_exclude_cols: list[str] | None = Field(
        default=None,
        description=(
            "List of columns to exclude from updates in a merge. Only applicable when write_mode='merge'. "
            "Used to prevent certain columns from being updated during a merge operation."
        ),
    )
    matched_update_include_cols: list[str] | None = Field(
        default=None,
        description=(
            "List of columns to include in updates in a merge. Only applicable when write_mode='merge'. "
            "Used to specify a subset of columns to be updated during a merge operation."
        ),
    )
    merge_with_schema_evolution: bool = Field(
        default=True,
        description=(
            "Whether to enable schema evolution during merge operations. Only applicable when write_mode='merge'. "
        ),
    )
    exclude_at_target_cols: list[str] | None = Field(
        default=None,
        description=(
            "List of columns to exclude from the target dataset during a merge operation. Only applicable when write_mode='merge'. "
        ),
    )
    include_at_target_cols: list[str] | None = Field(
        default=None,
        description=(
            "List of columns to include from the target dataset during a merge operation. Only applicable when write_mode='merge'. "
        ),
    )
    sequence_by: list[str] | None = Field(
        default=None,
        description=(
            "List of columns to order source and target datasets by before applying the merge condition. Only applicable when write_mode='merge'. "
        ),
    )
    ignore_null_updates: bool = Field(
        default=False,
        description=(
            "Whether to ignore updates to columns where the source value is null during a merge operation. Only applicable when write_mode='merge'. "
        ),
    )
    apply_as_delete: str | None = Field(
        default=None,
        description=(
            "Used for CDC scenarios. Whether to apply a condition as a delete operation instead of an update or insert during a merge. Only applicable when write_mode='merge'. "
        ),
    )
    prevent_full_refresh: bool = Field(
        default=False,
        description=(
            "Whether to prevent full refreshes for this model. If True, attempts to perform a full refresh (e.g. via the 'full_refresh' flag in the materialization context) will be overridden to perform an append or merge instead, and a warning will be logged. This can be useful for protecting critical tables from accidental full refreshes."
        ),
    )
