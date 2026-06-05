from pyspark.sql import DataFrame, SparkSession

from kelp.models.model_mat_config import ModelMaterializationConfig
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
        create_table_ddl: str | None = None,
        model_name: str | None = None,
    ) -> None:
        """Write the DataFrame using append/overwrite mode.

        If metadata exists and the target table is missing, create it using Kelp DDL
        first (to preserve generated columns and metadata-driven schema details).

        Args:
            spark: Active SparkSession.
            dataframe: Source DataFrame to materialize.
            target_name: Target table name/FQN.
            config: Effective materialization config.
            create_table_ddl: Optional DDL used to create target table when missing.
            model_name: Optional model name for contextual logging.
        """
        ensure_table_created(
            spark,
            target_name,
            create_table_ddl=create_table_ddl,
            model_name=model_name,
        )

        mode = config.write_mode or "append"
        dataframe.write.format("delta").options(**config.options).mode(mode).saveAsTable(
            target_name
        )
