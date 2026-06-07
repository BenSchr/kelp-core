import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from kelp.catalog import sync_tables
from kelp.models.model import DQXQuality
from kelp.models.model_mat_config import ModelMaterializationConfig
from kelp.service.model_manager import KelpModel, ModelManager
from kelp.tables.materialization.append_overwrite import AppendOverwriteMaterializer
from kelp.tables.materialization.base import merge_materialization_config
from kelp.tables.materialization.merge import MergeMaterializer
from kelp.tables.quality_validation.base import ensure_dqx_installed

logger = logging.getLogger(__name__)


@dataclass
class ResolvedMaterializationInputs:
    """Resolved runtime inputs for a materialization invocation."""

    target_name: str
    effective_config: ModelMaterializationConfig
    create_table_ddl: str | None
    model_name: str
    kelp_model: KelpModel | None
    dqx_quality: DQXQuality | None


def _resolve_model(table_name: str) -> KelpModel | None:
    model = ModelManager.build_model(table_name, soft_handle=True)
    if model.root_model is None:
        return None
    return model


def _resolve_materialization_inputs(
    *,
    table_name: str,
    config: ModelMaterializationConfig | None,
) -> ResolvedMaterializationInputs:
    """Resolve model/runtime inputs into a single explicit runtime object."""
    kelp_model = _resolve_model(table_name)

    target_name = kelp_model.fqn if kelp_model and kelp_model.fqn else table_name
    effective_config = merge_materialization_config(
        kelp_model.materialization if kelp_model else None,
        config,
    )

    create_table_ddl = kelp_model.get_ddl(if_not_exists=True) if kelp_model else None
    model_name = kelp_model.name if kelp_model else table_name

    dqx_quality: DQXQuality | None = kelp_model.dqx_quality if kelp_model else None

    return ResolvedMaterializationInputs(
        target_name=target_name,
        effective_config=effective_config,
        create_table_ddl=create_table_ddl,
        model_name=model_name,
        kelp_model=kelp_model,
        dqx_quality=dqx_quality,
    )


def _perform_maintenance(
    spark: SparkSession,
    fqn: str,
    apply_vacuum: bool = True,
    vacuum_lite: bool = True,
    apply_optimize: bool = True,
) -> None:
    """Perform post-materialization maintenance operations like OPTIMIZE and VACUUM."""
    if apply_optimize:
        try:
            spark.sql(f"OPTIMIZE {fqn}")
            logger.info("OPTIMIZE completed for %s", fqn)
        except Exception as e:  # noqa: BLE001
            logger.warning("OPTIMIZE failed for %s: %s", fqn, str(e)[:500])
    if apply_vacuum:
        query = f"VACUUM {fqn}"
        if vacuum_lite:
            query += " LITE"
        try:
            spark.sql(query)
            logger.info("VACUUM completed for %s", fqn)
        except Exception as e:  # noqa: BLE001
            logger.warning("VACUUM failed for %s: %s", fqn, str(e)[:500])


def _sync_metadata(
    spark: SparkSession,
    model: KelpModel,
) -> None:
    """Sync metadata for the materialized model's table."""
    for query in sync_tables([model.name]):
        try:
            spark.sql(query)
            logger.info("%s | %s", model.name, query)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Metadata sync query failed for model '%s': %s\nQuery: %s",
                model.name,
                str(e)[:500],
                query,
            )


def materialize(
    *,
    dataframe: DataFrame,
    name: str,
    config: ModelMaterializationConfig | None = None,
    full_refresh: bool = False,
    sync_metadata: bool = True,
    apply_vacuum: bool = True,
    vacuum_lite: bool = True,
    apply_optimize: bool = True,
    apply_quality_checks: bool = True,
    spark: SparkSession | None = None,
) -> DataFrame:
    """Materialize a DataFrame to Delta Lake based on materialization config.

    Strategy:
    - Resolve model metadata by table name (if available).
    - Merge metadata materialization config with passed runtime config.
    - Dispatch to append/overwrite or merge materializers.

    Args:
        dataframe: DataFrame to materialize.
        name: Kelp model name or fully qualified table name.
        config: Optional runtime override materialization config.
        full_refresh: Whether to perform a full refresh, which may be prevented by model config.
        sync_metadata: Whether to perform metadata sync after materialization.
        apply_vacuum: Whether to apply VACUUM after materialization.
        vacuum_lite: Whether to use VACUUM LITE (if False, full VACUUM is applied). Only applicable if apply_vacuum is True.
        apply_optimize: Whether to apply OPTIMIZE after materialization.
        spark: Optional SparkSession to use for materialization. If not provided, the active SparkSession will be used.

    Returns:
        The same input DataFrame (for chaining).
    """
    spark = spark or SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession available for materialization.")

    resolved = _resolve_materialization_inputs(table_name=name, config=config)

    logger.info(
        "Materializing '%s' to target '%s' with config: %s",
        name,
        resolved.target_name,
        resolved.effective_config.model_dump_json(),
    )

    result_df = dataframe
    if apply_quality_checks and resolved.dqx_quality and resolved.dqx_quality.checks:
        logger.debug(
            "Model '%s' has DQX checks defined. Applying quality checks before materialization.",
            resolved.target_name,
        )
        ensure_dqx_installed()

        from kelp.tables.quality_validation.dqx import apply_dqx_quality_checks

        dqx = resolved.dqx_quality
        quarantine_fqn = (
            resolved.kelp_model.quarantine_table
            if resolved.kelp_model and dqx.spark_quarantine
            else None
        )
        result_df = apply_dqx_quality_checks(
            dataframe=result_df,
            checks=dqx.checks,
            violation_action=dqx.spark_violation_action,
            target_table=resolved.target_name,
            quarantine_enabled=bool(dqx.spark_quarantine),
            quarantine_fqn=quarantine_fqn,
        )

    _materialize_model(
        spark=spark,
        dataframe=result_df,
        target_name=resolved.target_name,
        create_table_ddl=resolved.create_table_ddl,
        model_name=resolved.model_name,
        effective_config=resolved.effective_config,
        full_refresh=full_refresh,
    )

    # Apply metadata sync for the model after materialization
    if resolved.kelp_model and sync_metadata:
        # Skip if not on databricks
        from kelp.utils.databricks import on_databricks

        if on_databricks():
            _sync_metadata(spark, resolved.kelp_model)
        else:
            logger.info(
                "Skipping metadata sync for model '%s' since not running on Databricks",
                resolved.kelp_model.name,
            )

    _perform_maintenance(
        spark,
        resolved.target_name,
        apply_vacuum=apply_vacuum,
        vacuum_lite=vacuum_lite,
        apply_optimize=apply_optimize,
    )

    return dataframe


def _materialize_model(
    spark: SparkSession,
    dataframe: DataFrame,
    target_name: str,
    create_table_ddl: str | None,
    model_name: str,
    effective_config: ModelMaterializationConfig,
    full_refresh: bool = False,
):
    if full_refresh and not effective_config.prevent_full_refresh:
        logger.info("Full refresh requested for '%s', dropping table", target_name)
        try:
            spark.sql(f"DROP TABLE IF EXISTS {target_name}")
            logger.info("Dropped table '%s' for full refresh.", target_name)
        except Exception as e:
            raise RuntimeError(
                f"Failed to drop table '{target_name}' for full refresh: {e!s}"
            ) from e

    if full_refresh and effective_config.prevent_full_refresh:
        logger.warning(
            "Full refresh requested for '%s' but prevented by model config.",
            target_name,
        )

    if effective_config.write_mode == "merge":
        MergeMaterializer.run(
            spark=spark,
            dataframe=dataframe,
            target_name=target_name,
            config=effective_config,
            create_table_ddl=create_table_ddl,
            model_name=model_name,
        )
    elif effective_config.write_mode in {"append", "overwrite"}:
        AppendOverwriteMaterializer.run(
            spark=spark,
            dataframe=dataframe,
            target_name=target_name,
            config=effective_config,
            create_table_ddl=create_table_ddl,
            model_name=model_name,
        )
    else:
        AppendOverwriteMaterializer.run(
            spark=spark,
            dataframe=dataframe,
            target_name=target_name,
            config=ModelMaterializationConfig(
                write_mode="append",
                options=effective_config.options,
            ),
            create_table_ddl=create_table_ddl,
            model_name=model_name,
        )
