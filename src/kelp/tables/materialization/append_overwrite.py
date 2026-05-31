from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from kelp.models.model_mat_config import ModelMaterializationConfig
from kelp.service.model_manager import KelpModel
from kelp.tables.materialization.base import ensure_table_created


class AppendOverwriteMaterializer:
    """Delta materializer for append/overwrite write modes."""

    @classmethod
    def run(
        cls,
        *,
        spark: SparkSession,
        dataframe: DataFrame,
        target_name: str,
        config: ModelMaterializationConfig,
        kelp_model: KelpModel | None,
    ) -> None:
        """Write the DataFrame using append/overwrite mode.

        If metadata exists and the target table is missing, create it using Kelp DDL
        first (to preserve generated columns and metadata-driven schema details).

        Args:
            spark: Active SparkSession.
            dataframe: Source DataFrame to materialize.
            target_name: Target table name/FQN.
            config: Effective materialization config.
            kelp_model: Resolved Kelp model (if found).
        """
        ensure_table_created(spark, kelp_model, target_name)

        mode = config.write_mode or "append"
        dataframe.write.format("delta").options(**config.options).mode(mode).saveAsTable(
            target_name
        )
