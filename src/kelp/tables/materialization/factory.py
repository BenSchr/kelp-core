import logging

from pyspark.sql import DataFrame, SparkSession

from kelp.catalog import sync_tables
from kelp.models.model_mat_config import ModelMaterializationConfig
from kelp.service.model_manager import KelpModel, ModelManager
from kelp.tables.materialization.append_overwrite import AppendOverwriteMaterializer
from kelp.tables.materialization.base import merge_materialization_config
from kelp.tables.materialization.merge import MergeMaterializer

logger = logging.getLogger(__name__)


def _resolve_model(table_name: str) -> KelpModel | None:
    model = ModelManager.build_model(table_name, soft_handle=True)
    if model.root_model is None:
        return None
    return model


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
    table_name: str,
    config: ModelMaterializationConfig | None = None,
    full_refresh: bool = False,
    sync_metadata: bool = True,
    apply_vacuum: bool = True,
    vacuum_lite: bool = True,
    apply_optimize: bool = True,
    spark: SparkSession | None = None,
) -> DataFrame:
    """Materialize a DataFrame to Delta Lake based on materialization config.

    Strategy:
    - Resolve model metadata by table name (if available).
    - Merge metadata materialization config with passed runtime config.
    - Dispatch to append/overwrite or merge materializers.

    Args:
        dataframe: DataFrame to materialize.
        table_name: Kelp model name or fully qualified table name.
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
    kelp_model = _resolve_model(table_name)
    target_name = kelp_model.fqn if kelp_model and kelp_model.fqn else table_name
    effective_config = merge_materialization_config(
        kelp_model.config if kelp_model else None,
        config,
    )
    logger.info(
        "Materializing '%s' to target '%s' with config: %s",
        table_name,
        target_name,
        effective_config.model_dump_json(),
    )

    # PLACEHOLDER(quality): Apply metadata-driven quality checks and quarantine handling
    # prior to write/merge operations.

    if full_refresh and not effective_config.prevent_full_refresh:
        logger.info("Full refresh requested for '%s', dropping table", target_name)
        try:
            spark.sql(f"DROP TABLE IF EXISTS {target_name}")
            logger.info("Dropped table '%s' for full refresh.", target_name)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Failed to drop table '%s' for full refresh: %s", target_name, str(e)[:500]
            )

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
            kelp_model=kelp_model,
        )
    elif effective_config.write_mode in {"append", "overwrite"}:
        AppendOverwriteMaterializer.run(
            spark=spark,
            dataframe=dataframe,
            target_name=target_name,
            config=effective_config,
            kelp_model=kelp_model,
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
            kelp_model=kelp_model,
        )

    # Apply metadata sync for the model after materialization
    if kelp_model and sync_metadata:
        # Skip if not on databricks
        from kelp.utils.databricks import on_databricks

        if on_databricks():
            _sync_metadata(spark, kelp_model)
        else:
            logger.info(
                "Skipping metadata sync for model '%s' since not running on Databricks",
                kelp_model.name,
            )

    _perform_maintenance(
        spark,
        target_name,
        apply_vacuum=apply_vacuum,
        vacuum_lite=vacuum_lite,
        apply_optimize=apply_optimize,
    )

    return dataframe
