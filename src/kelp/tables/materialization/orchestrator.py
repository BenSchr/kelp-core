"""Materialization entry point: the steps around a Delta write, in order.

One place decides the order of everything a materialization does — quality checks,
quarantine and monitoring writes, the write itself, catalog sync and table upkeep —
so the sequence is readable and changeable in one file. The layers it calls are
each responsible for one thing and write nothing on their own:

- :mod:`kelp.tables.materialization.resolve` turns a name and a config into
  explicit inputs.
- :mod:`kelp.tables.quality_validation.dqx` builds quality result frames.
- :mod:`kelp.tables.materialization.strategies` performs the write for one mode.
- :mod:`kelp.tables.materialization.maintenance` resets, optimizes and syncs.
"""

import logging
from typing import TYPE_CHECKING, Any, Protocol

from pyspark.sql import DataFrame, SparkSession

from kelp.models.model_mat_config import MaterializationConfig, MaterializationOptions
from kelp.tables.materialization import maintenance
from kelp.tables.materialization.resolve import (
    FullRefreshStrategy,
    ResolvedMaterializationInputs,
    resolve_materialization_inputs,
)
from kelp.tables.materialization.strategies import append_overwrite, merge, scd2

if TYPE_CHECKING:
    from kelp.tables.quality_validation.dqx import QualityResult

logger = logging.getLogger(__name__)


class WriteStrategy(Protocol):
    """The call signature every mode's write function implements.

    ``config`` is untyped here because each strategy narrows it to the config class
    of its own mode, which a shared signature cannot express. The registry is still
    checked for everything else, so a renamed or missing parameter fails type
    checking instead of at runtime.
    """

    def __call__(
        self,
        *,
        spark: SparkSession,
        dataframe: DataFrame,
        target_name: str,
        config: Any,
        create_table_ddl: str | None = None,
        model_name: str | None = None,
    ) -> None: ...


#: Write function per materialization mode. Adding a mode is one entry here plus
#: the config class in :mod:`kelp.models.model_mat_config`.
WRITE_STRATEGIES: dict[str, WriteStrategy] = {
    "append": append_overwrite.run,
    "overwrite": append_overwrite.run,
    "merge": merge.run,
    "scd2": scd2.run,
}


def resolve_options(
    override: "MaterializationOptions | dict | None" = None,
) -> MaterializationOptions:
    """Resolve the effective options from project defaults and a call-site override.

    Args:
        override: Options or mapping overriding individual switches.

    Returns:
        The project's ``materialization_options`` with ``override`` applied, or
        plain defaults when no kelp project is available.
    """
    from kelp.config import project_settings

    try:
        defaults = project_settings().materialization_options
    except FileNotFoundError:
        defaults = MaterializationOptions()
    return defaults.merged_with(override)


def _run_quality_checks(
    *,
    dataframe: DataFrame,
    resolved: ResolvedMaterializationInputs,
    monitoring_fqn: str | None,
) -> "QualityResult":
    """Run the DQX checks declared in kelp metadata. Nothing is written here."""
    from kelp.tables.quality_validation.base import ensure_dqx_installed
    from kelp.tables.quality_validation.dqx import run_dqx_quality_checks

    ensure_dqx_installed()

    dqx = resolved.dqx_quality
    assert dqx is not None  # noqa: S101 - guarded by the caller
    quarantine_fqn = (
        resolved.kelp_model.quarantine_table
        if resolved.kelp_model and dqx.spark_quarantine
        else None
    )
    return run_dqx_quality_checks(
        dataframe=dataframe,
        checks=dqx.checks,
        violation_action=dqx.spark_violation_action,
        target_table=resolved.target_name,
        quarantine_enabled=bool(dqx.spark_quarantine),
        quarantine_fqn=quarantine_fqn,
        monitoring_fqn=monitoring_fqn,
    )


def _store_quality_outputs(
    result: "QualityResult",
    *,
    resolved: ResolvedMaterializationInputs,
    monitoring_fqn: str | None,
) -> None:
    """Persist the quarantine and monitoring rows a quality run produced."""
    from kelp.tables.quality_validation.dqx import store_dqx_stats_table, store_quarantine_rows

    quarantine_fqn = resolved.kelp_model.quarantine_table if resolved.kelp_model else None
    if result.quarantine_df is not None and quarantine_fqn:
        store_quarantine_rows(result.quarantine_df, quarantine_fqn)

    if result.stats_df is not None and monitoring_fqn:
        store_dqx_stats_table(result.stats_df, monitoring_fqn)


def _write(
    *,
    spark: SparkSession,
    dataframe: DataFrame,
    resolved: ResolvedMaterializationInputs,
    full_refresh: bool,
    full_refresh_strategy: FullRefreshStrategy = "drop",
) -> None:
    """Apply the full-refresh policy and dispatch to the mode's write strategy.

    Raises:
        TypeError: If the config's mode has no registered write strategy.
    """
    config = resolved.effective_config
    target_name = resolved.target_name

    if full_refresh and not config.allow_full_refresh:
        logger.warning(
            "Full refresh requested for '%s' but prevented by allow_full_refresh=false.",
            target_name,
        )
    elif full_refresh:
        maintenance.prepare_full_refresh(
            spark=spark, resolved=resolved, strategy=full_refresh_strategy
        )

    strategy = WRITE_STRATEGIES.get(config.mode)
    if strategy is None:  # pragma: no cover - the config union is exhaustive
        raise TypeError(f"Unsupported materialization mode: {config.mode}")

    strategy(
        spark=spark,
        dataframe=dataframe,
        target_name=target_name,
        config=config,
        create_table_ddl=resolved.create_table_ddl,
        model_name=resolved.model_name,
    )


def materialize_resolved(
    *,
    dataframe: DataFrame,
    resolved: ResolvedMaterializationInputs,
    options: "MaterializationOptions | dict | None" = None,
    full_refresh: bool = False,
    full_refresh_strategy: FullRefreshStrategy = "drop",
    spark: SparkSession | None = None,
) -> DataFrame:
    """Materialize a DataFrame using already-resolved inputs.

    Use this when the caller has already resolved the target — the decorator does,
    to build its context — so metadata is not looked up twice per run.

    Args:
        dataframe: DataFrame to materialize.
        resolved: Resolved target, config and metadata.
        options: Overrides for the steps run around the write.
        full_refresh: Whether to rebuild the target from scratch first. Ignored with
            a warning when the config sets ``allow_full_refresh: false``.
        full_refresh_strategy: How a full refresh resets the target.
        spark: SparkSession to use. Defaults to the active session.

    Returns:
        The DataFrame that was written, which is the input frame minus any rows
        quality checks dropped.

    Raises:
        RuntimeError: If no SparkSession is available.
        ValueError: If quality checks found errors and the model's
            ``spark_violation_action`` is ``error``.
    """
    spark = spark or SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession available for materialization.")

    effective_options = resolve_options(options)
    dqx = resolved.dqx_quality
    run_checks = bool(effective_options.apply_quality_checks and dqx and dqx.checks)

    logger.info(
        "Materializing '%s' to target '%s' with config: %s",
        resolved.model_name,
        resolved.target_name,
        resolved.effective_config.model_dump_json(),
    )

    result_df = dataframe
    if run_checks:
        from kelp.tables.quality_validation.base import quality_monitoring_target

        monitoring_fqn = quality_monitoring_target()
        quality = _run_quality_checks(
            dataframe=dataframe, resolved=resolved, monitoring_fqn=monitoring_fqn
        )
        result_df = quality.dataframe

        _store_quality_outputs(quality, resolved=resolved, monitoring_fqn=monitoring_fqn)
        if dqx and dqx.spark_violation_action == "error" and quality.has_errors:
            raise ValueError("Data quality checks failed with action 'error'.")

    _write(
        spark=spark,
        dataframe=result_df,
        resolved=resolved,
        full_refresh=full_refresh,
        full_refresh_strategy=full_refresh_strategy,
    )

    if resolved.kelp_model and effective_options.sync_metadata:
        maintenance.sync_metadata(spark, resolved.kelp_model)

    maintenance.perform_maintenance(
        spark,
        resolved.target_name,
        apply_vacuum=effective_options.apply_vacuum,
        vacuum_lite=effective_options.vacuum_lite,
        apply_optimize=effective_options.apply_optimize,
    )

    return result_df


def materialize(
    *,
    dataframe: DataFrame,
    name: str,
    config: "MaterializationConfig | dict | None" = None,
    options: "MaterializationOptions | dict | None" = None,
    full_refresh: bool = False,
    full_refresh_strategy: FullRefreshStrategy = "drop",
    spark: SparkSession | None = None,
) -> DataFrame:
    """Materialize a DataFrame to Delta Lake according to a materialization config.

    Args:
        dataframe: DataFrame to materialize.
        name: Unqualified kelp model name, or a qualified table name to write
            without metadata.
        config: Materialization config or mapping. Replaces the model's config
            entirely when both are present.
        options: Overrides for the steps run around the write — quality checks,
            catalog sync, OPTIMIZE and VACUUM. Unset switches fall back to the
            project's ``materialization_options``.
        full_refresh: Whether to rebuild the target from scratch first. Ignored with
            a warning when the config sets ``allow_full_refresh: false``.
        full_refresh_strategy: How a full refresh resets the target — ``drop`` to
            drop and recreate it, ``replace`` to keep the table (and its grants and
            history) via ``CREATE OR REPLACE TABLE``, or ``TRUNCATE`` when no DDL
            is available.
        spark: SparkSession to use. Defaults to the active session.

    Returns:
        The DataFrame that was written, which is the input frame minus any rows
        quality checks dropped.

    Raises:
        RuntimeError: If no SparkSession is available.
        LookupError: If ``name`` is unqualified and resolves to no kelp model.
        ValueError: If quality checks found errors and the model's
            ``spark_violation_action`` is ``error``.
    """
    resolved = resolve_materialization_inputs(table_name=name, config=config)
    return materialize_resolved(
        dataframe=dataframe,
        resolved=resolved,
        options=options,
        full_refresh=full_refresh,
        full_refresh_strategy=full_refresh_strategy,
        spark=spark,
    )
