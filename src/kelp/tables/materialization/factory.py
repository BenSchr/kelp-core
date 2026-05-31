from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from kelp.models.model_mat_config import ModelMaterializationConfig
from kelp.service.model_manager import KelpModel, ModelManager
from kelp.tables.materialization.append_overwrite import AppendOverwriteMaterializer
from kelp.tables.materialization.base import merge_materialization_config
from kelp.tables.materialization.merge import MergeMaterializer


def _resolve_model(table_name: str) -> KelpModel | None:
    model = ModelManager.build_model(table_name, soft_handle=True)
    if model.root_model is None:
        return None
    return model


def materialize(
    *,
    spark: SparkSession,
    dataframe: DataFrame,
    table_name: str,
    config: ModelMaterializationConfig | None = None,
) -> DataFrame:
    """Materialize a DataFrame to Delta Lake based on materialization config.

    Strategy:
    - Resolve model metadata by table name (if available).
    - Merge metadata materialization config with passed runtime config.
    - Dispatch to append/overwrite or merge materializers.

    Args:
        spark: Active SparkSession.
        dataframe: DataFrame to materialize.
        table_name: Kelp model name or fully qualified table name.
        config: Optional runtime override materialization config.

    Returns:
        The same input DataFrame (for chaining).
    """
    kelp_model = _resolve_model(table_name)
    target_name = kelp_model.fqn if kelp_model and kelp_model.fqn else table_name
    effective_config = merge_materialization_config(
        kelp_model.config if kelp_model else None,
        config,
    )

    # PLACEHOLDER(metadata): Apply model-level metadata propagation before materialization
    # (e.g., tags, comments, table properties sync behavior).
    # PLACEHOLDER(quality): Apply metadata-driven quality checks and quarantine handling
    # prior to or during write/merge operations.
    # PLACEHOLDER(maintenance): Add post-write maintenance hooks (OPTIMIZE/VACUUM/ZORDER,
    # stats collection, and retention controls) controlled by future config.

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

    return dataframe
