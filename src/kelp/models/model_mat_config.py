"""Materialization configuration models.

These models depend on pydantic only. They can be built in code and used without
a kelp project or any kelp metadata; when a kelp model YAML declares a
``materialization:`` block it is parsed into the very same models.

The configuration is a discriminated union on ``mode``, so each mode carries only
the fields that apply to it and invalid combinations fail at parse time instead of
mid-write:

- :class:`AppendConfig` — ``mode: append``
- :class:`OverwriteConfig` — ``mode: overwrite``
- :class:`MergeConfig` — ``mode: merge`` (Delta merge / SCD type 1)
- :class:`Scd2Config` — ``mode: scd2`` (history tracking / SCD type 2)
"""

import logging
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

logger = logging.getLogger(__name__)

DEFAULT_MODE = "append"
SOURCE_ALIAS = "source"
TARGET_ALIAS = "target"


class MaterializationOptions(BaseModel):
    """Switches for the steps kelp runs around a materialization write.

    These are operational choices, not part of a model's write semantics, so they
    live outside the mode config: project-wide defaults come from
    ``kelp_project.yml``, and a call site may override individual switches.
    """

    model_config = ConfigDict(extra="forbid")

    apply_quality_checks: bool = Field(
        default=True,
        description="Whether DQX checks declared in model metadata are applied.",
    )
    sync_metadata: bool = Field(
        default=True,
        description="Whether catalog metadata is synced afterwards. Requires a model.",
    )
    apply_optimize: bool = Field(
        default=True,
        description="Whether OPTIMIZE runs after the write.",
    )
    apply_vacuum: bool = Field(
        default=True,
        description="Whether VACUUM runs after the write.",
    )
    vacuum_lite: bool = Field(
        default=True,
        description="Whether VACUUM uses LITE mode. Only used when apply_vacuum is set.",
    )

    def merged_with(
        self, override: "MaterializationOptions | dict | None"
    ) -> "MaterializationOptions":
        """Return these options with the explicitly set fields of ``override`` applied.

        Args:
            override: Options or mapping overriding individual switches, or ``None``.

        Returns:
            The effective options. Unset fields keep this instance's values, so a
            call site only has to state what it wants to change.
        """
        if override is None:
            return self
        if isinstance(override, dict):
            return self.model_copy(update=override)
        return self.model_copy(update=override.model_dump(exclude_unset=True))


class ColumnSelector(BaseModel):
    """Include/exclude selector for a set of columns.

    Exactly one of ``include`` or ``exclude`` may be set. Matching is
    case-insensitive.

    Args:
        include: Only these columns are selected.
        exclude: All columns except these are selected.
    """

    include: list[str] | None = Field(
        default=None,
        description="Only these columns are selected (case-insensitive).",
    )
    exclude: list[str] | None = Field(
        default=None,
        description="All columns except these are selected (case-insensitive).",
    )

    @model_validator(mode="after")
    def _validate_exclusive(self) -> "ColumnSelector":
        """Reject selectors that set both include and exclude."""
        if self.include is not None and self.exclude is not None:
            raise ValueError("Set only one of 'include' or 'exclude' on a column selector.")
        return self

    def apply(self, candidates: list[str], required: list[str] | None = None) -> list[str]:
        """Select from ``candidates``, preserving their order.

        Args:
            candidates: Columns to select from.
            required: Columns always kept, even when not selected.

        Returns:
            Selected column names in ``candidates`` order.
        """
        selected = list(candidates)
        if self.include is not None:
            wanted = {name.lower() for name in self.include}
            selected = [col for col in candidates if col.lower() in wanted]
        elif self.exclude is not None:
            unwanted = {name.lower() for name in self.exclude}
            selected = [col for col in candidates if col.lower() not in unwanted]

        selected_lower = {col.lower() for col in selected}
        lookup = {col.lower(): col for col in candidates}
        for name in required or []:
            if name.lower() in lookup and name.lower() not in selected_lower:
                selected.append(lookup[name.lower()])
                selected_lower.add(name.lower())
        return selected

    def names(self) -> list[str]:
        """Return the configured column names, whichever side is set."""
        return list(self.include or self.exclude or [])


class SqlConditions(BaseModel):
    """Raw SQL conditions for the merge clauses.

    Escape hatch for cases the declarative fields cannot express. Every entry is a
    boolean SQL expression guarding one merge clause and may reference the
    ``source`` and ``target`` aliases — nothing else about the merge is settable here.

    Args:
        when_matched: Replaces the derived "row changed" condition on updates.
        when_not_matched: Extra condition applied to inserts of unmatched source rows.
        when_not_matched_by_source: Extra condition applied to target rows missing
            from the source. Requires ``missing_in_source: delete``.
    """

    when_matched: str | None = Field(
        default=None,
        description="Replaces the derived 'row changed' condition on matched updates.",
    )
    when_not_matched: str | None = Field(
        default=None,
        description="Extra condition applied when inserting unmatched source rows.",
    )
    when_not_matched_by_source: str | None = Field(
        default=None,
        description=(
            "Extra condition applied to target rows missing from the source. "
            "Requires missing_in_source='delete'."
        ),
    )


class Scd2Columns(BaseModel):
    """Names of the history-tracking columns maintained by ``mode: scd2``.

    Defaults match Databricks AUTO CDC (``apply_changes``) so SCD2 tables stay
    interchangeable between SDP pipelines and kelp Spark jobs.

    Args:
        valid_from: Column holding the sequence value a version becomes valid at.
        valid_to: Column holding the sequence value a version is superseded at.
        is_current: Optional boolean column maintained alongside ``valid_to``.
        open_value: SQL expression ``valid_to`` takes while a version is current,
            instead of ``NULL`` — e.g. ``"'2999-12-31'"`` or ``"9999999999"``.
    """

    valid_from: str = Field(
        default="__START_AT",
        description="Column holding the sequence value a version becomes valid at.",
    )
    valid_to: str = Field(
        default="__END_AT",
        description=(
            "Column holding the sequence value a version is superseded at "
            "(NULL for the current version unless open_value is set)."
        ),
    )
    is_current: str | None = Field(
        default=None,
        description="Optional boolean column maintained alongside valid_to.",
    )
    open_value: str | None = Field(
        default=None,
        description=(
            "SQL expression valid_to takes while a version is current, instead of NULL "
            "(e.g. \"'2999-12-31'\"). Must be castable to the sequence_by type."
        ),
    )

    def all_names(self) -> list[str]:
        """Return every history column name that is configured."""
        names = [self.valid_from, self.valid_to]
        if self.is_current:
            names.append(self.is_current)
        return names


class _BaseMaterialization(BaseModel):
    """Fields shared by every materialization mode.

    Unknown fields are rejected: a setting that does not apply to the chosen mode
    (or a typo) fails at parse time instead of being silently ignored.
    """

    model_config = ConfigDict(extra="forbid")

    options: dict[str, str] = Field(
        default_factory=dict,
        description="Extra Delta writer/merge options (e.g. {'mergeSchema': 'true'}).",
    )
    allow_full_refresh: bool = Field(
        default=True,
        description=(
            "Whether a caller-requested full refresh may drop and rebuild the target. "
            "Set to False to protect critical tables; the refresh is then skipped with a warning."
        ),
    )


class AppendConfig(_BaseMaterialization):
    """Append the DataFrame to the target table."""

    mode: Literal["append"] = "append"


class OverwriteConfig(_BaseMaterialization):
    """Replace the target table contents with the DataFrame.

    Args:
        replace_where: Optional predicate limiting the overwrite to matching rows
            (Delta ``replaceWhere``) instead of the whole table.
    """

    mode: Literal["overwrite"] = "overwrite"
    replace_where: str | None = Field(
        default=None,
        description="Predicate limiting the overwrite to matching rows (Delta replaceWhere).",
    )


class _BaseMerge(_BaseMaterialization):
    """Fields shared by the key-based modes (``merge`` and ``scd2``).

    These mirror the parameters of Databricks AUTO CDC (``create_auto_cdc_flow``),
    where ``mode`` plays the role of ``stored_as_scd_type``. Only settings that
    genuinely differ between the two live on the mode-specific classes.
    """

    keys: list[str] = Field(
        min_length=1,
        description="Business key columns identifying a row. Required.",
    )
    sequence_by: list[str] = Field(
        default_factory=list,
        description=(
            "Columns ordering source rows in time. Multiple columns are compared as a struct. "
            "Used to deduplicate the batch and to ignore out-of-order rows."
        ),
    )
    columns: ColumnSelector | None = Field(
        default=None,
        description="Which source columns reach the target. Keys are always included.",
    )
    track_changes: ColumnSelector | None = Field(
        default=None,
        description=(
            "Which columns are compared to decide whether a row changed at all. "
            "When none of them differ, nothing is written: no update (merge) and no new "
            "version (scd2). Defaults to every written column except the keys."
        ),
    )
    when_deleted: str | None = Field(
        default=None,
        description="SQL predicate marking source rows as deletes (CDC tombstones).",
    )
    where: str | None = Field(
        default=None,
        description="Predicate narrowing the target rows taken into account.",
    )
    ignore_null_updates: bool = Field(
        default=False,
        description=(
            "Whether a NULL source value leaves the stored value alone instead of "
            "replacing it, so partial CDC rows keep the previous value."
        ),
    )
    ignore_null_updates_columns: ColumnSelector | None = Field(
        default=None,
        description=(
            "Which columns ignore_null_updates applies to. Defaults to every written "
            "column except the keys."
        ),
    )
    schema_evolution: bool = Field(
        default=True,
        description="Whether new source columns are added to the target during the merge.",
    )

    def ignore_null_columns(self, candidates: list[str]) -> list[str]:
        """Return the columns whose NULL source values must not replace stored values.

        Args:
            candidates: Columns eligible for the rule, normally the written columns
                except the keys.

        Returns:
            Selected column names, empty when ``ignore_null_updates`` is off.
        """
        if not self.ignore_null_updates:
            return []
        return (self.ignore_null_updates_columns or ColumnSelector()).apply(candidates)


class MergeConfig(_BaseMerge):
    """Merge rows by key, keeping one current version per key (SCD type 1)."""

    mode: Literal["merge"] = "merge"
    sql_conditions: SqlConditions | None = Field(
        default=None,
        description="Raw SQL conditions for the merge clauses.",
    )
    insert_only_columns: list[str] = Field(
        default_factory=list,
        description=(
            "Columns written on insert but left out of the update, so their first value "
            "survives (e.g. created_at). Unlike track_changes, which decides whether an "
            "update happens at all, this decides which columns an update may touch."
        ),
    )
    missing_in_source: Literal["ignore", "delete"] = Field(
        default="ignore",
        description="What to do with target rows that the source does not contain.",
    )


class Scd2Config(_BaseMerge):
    """Track full row history by key, closing superseded versions (SCD type 2)."""

    mode: Literal["scd2"] = "scd2"
    sequence_by: list[str] = Field(
        min_length=1,
        description=(
            "Columns ordering source rows in time. Required for scd2: they become the "
            "valid_from/valid_to interval bounds."
        ),
    )
    history: Scd2Columns = Field(
        default_factory=Scd2Columns,
        description="Names of the history-tracking columns kelp maintains.",
    )


MaterializationConfig = Annotated[
    AppendConfig | OverwriteConfig | MergeConfig | Scd2Config,
    Field(discriminator="mode"),
]

_ADAPTER: TypeAdapter[Any] = TypeAdapter(MaterializationConfig)


def parse_materialization_config(
    value: "MaterializationConfig | dict | None",
) -> "MaterializationConfig | None":
    """Coerce a mapping or config instance into a validated config.

    Args:
        value: Mapping, an already built config instance, or ``None``. A mapping
            without ``mode`` is treated as :class:`AppendConfig`.

    Returns:
        Validated config, or ``None`` when ``value`` is ``None``.

    Raises:
        TypeError: If ``value`` is neither a mapping nor a config instance.
    """
    if value is None:
        return None
    if isinstance(value, _BaseMaterialization):
        return value
    if isinstance(value, dict):
        return _ADAPTER.validate_python({"mode": DEFAULT_MODE, **value})
    raise TypeError(f"Unsupported materialization config type: {type(value).__name__}")


def resolve_config(
    metadata: "MaterializationConfig | None",
    override: "MaterializationConfig | None",
) -> "MaterializationConfig":
    """Pick the config to materialize with.

    A config passed at runtime replaces the model's config entirely — the two are
    never combined field by field, so what a caller passes is exactly what runs.

    Args:
        metadata: Config resolved from kelp metadata, if any.
        override: Config passed at runtime, if any.

    Returns:
        The effective config, defaulting to append when neither is set.
    """
    if override is not None:
        if metadata is not None:
            logger.info(
                "Runtime materialization config (mode '%s') replaces the model's config "
                "(mode '%s').",
                override.mode,
                metadata.mode,
            )
        return override
    return metadata if metadata is not None else AppendConfig()
