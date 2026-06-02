import logging

from pyspark.sql import SparkSession

from kelp.models.model_mat_config import ModelMaterializationConfig
from kelp.service.model_manager import KelpModel

logger = logging.getLogger(__name__)

DEFAULT_WRITE_MODE = "append"


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """Return whether a table exists in the current Spark catalog.

    Args:
        spark: Active SparkSession.
        table_name: Fully qualified (preferred) or session-resolved table name.

    Returns:
        ``True`` if the table exists, else ``False``.
    """
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:  # noqa: BLE001
        logger.debug("Unable to determine table existence for '%s'.", table_name)
        return False


def ensure_table_created(
    spark: SparkSession, kelp_model: KelpModel | None, target_name: str
) -> bool:
    """Create a missing target table via Kelp model DDL when available.

    Args:
        spark: Active SparkSession.
        kelp_model: Resolved Kelp model, when metadata exists.
        target_name: Target table name/FQN.

    Returns:
        ``True`` when the table exists after this function returns, else ``False``.
    """
    if table_exists(spark, target_name):
        return True

    if kelp_model is None:
        return False

    ddl = kelp_model.get_ddl(if_not_exists=True)

    if not ddl:
        return False
    try:
        spark.sql(ddl)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Failed to create table '%s' for model '%s' using DDL: %s",
            target_name,
            kelp_model.name,
            ddl,
        )
        # Raise since creating table may contain generated columns or other metadata-driven schema details that are required for correct materialization
        raise RuntimeError(
            f"Failed to create table '{target_name}' for model '{kelp_model.name}'."
        ) from e

    return table_exists(spark, target_name)


def merge_materialization_config(
    model_config: ModelMaterializationConfig | None,
    override_config: ModelMaterializationConfig | None,
) -> ModelMaterializationConfig:
    """Merge a metadata config with a decorator/runtime override.

    Override fields are applied only for explicitly provided values.

    Args:
        model_config: Config sourced from Kelp metadata.
        override_config: User override config.

    Returns:
        Effective merged config.
    """
    if model_config is None and override_config is None:
        return ModelMaterializationConfig(write_mode=DEFAULT_WRITE_MODE)
    if model_config is None:
        return override_config or ModelMaterializationConfig(write_mode=DEFAULT_WRITE_MODE)
    if override_config is None:
        return model_config

    merged = {
        **model_config.model_dump(),
        **override_config.model_dump(exclude_unset=True),
    }
    effective = ModelMaterializationConfig(**merged)
    if effective.write_mode is None:
        effective.write_mode = DEFAULT_WRITE_MODE
    return effective


def build_null_safe_change_condition(
    *,
    source_alias: str,
    target_alias: str,
    columns: list[str],
) -> str | None:
    """Build a null-aware "row changed" condition across columns.

    The expression uses Spark null-safe equality (`<=>`) and returns a SQL condition
    equivalent to: any column has changed.

    Args:
        source_alias: Source alias used in merge.
        target_alias: Target alias used in merge.
        columns: Columns to compare.

    Returns:
        SQL condition string or ``None`` when no columns are provided.
    """
    if not columns:
        return None

    comparisons = [f"NOT ({source_alias}.`{col}` <=> {target_alias}.`{col}`)" for col in columns]
    return " OR ".join(comparisons)


def apply_full_refresh(spark: SparkSession, table_name: str) -> None:
    """Apply a full refresh by dropping the target table.

    Args:
        spark: Active SparkSession.
        table_name: Fully qualified target table name.
    """
    spark.sql(f"DROP TABLE {table_name}")
