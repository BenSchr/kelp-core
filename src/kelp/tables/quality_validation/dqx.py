from pyspark.sql import DataFrame

# from base import
from kelp.tables.quality_validation.base import (
    get_quality_monitorng_table_name,
    should_apply_quality_monitoring,
)


def apply_dqx_quality_checks(
    dataframe: DataFrame,
    checks: list[dict],
    violation_action: str,
    target_table: str | None = None,
    quarantine_enabled: bool = False,
    quarantine_fqn: str | None = None,
) -> DataFrame:
    """Apply DQX quality checks to the given DataFrame and handle violations based on the configured action. Returns observers for monitoring if enabled."""
    from databricks.labs.dqx.engine import DQEngine
    from databricks.sdk import WorkspaceClient

    from kelp.utils.databricks import on_databricks

    # If not on Databricks mock client for local development and testing
    if not on_databricks():
        from unittest.mock import MagicMock

        ws = MagicMock(WorkspaceClient)
    else:
        ws = WorkspaceClient()

    dqx_engine = DQEngine(ws)
    apply_monitoring = should_apply_quality_monitoring()
    monitoring_fqn = get_quality_monitorng_table_name()

    if violation_action != "ignore":
        check_result, bad_df = dqx_engine.apply_checks_by_metadata_and_split(
            df=dataframe, checks=checks
        )
        if not bad_df.isEmpty():
            if quarantine_enabled and quarantine_fqn:
                bad_df.write.format("delta").mode("append").saveAsTable(quarantine_fqn)

            if apply_monitoring and monitoring_fqn:
                build_and_store_dqx_stats_table(
                    df=bad_df,
                    target_table=target_table,
                    quarantine_table=quarantine_fqn if quarantine_enabled else None,
                    stats_table_fqn=monitoring_fqn,
                )

            if violation_action == "error":
                # Check if _errors column has no values
                has_errors = bad_df.filter("_errors IS NOT NULL").isEmpty() is False
                if has_errors:
                    raise ValueError("Data quality checks failed with action 'error'.")

        # Action 'drop' just returns the good_df, effectively dropping the bad records

    else:
        check_result = dqx_engine.apply_checks_by_metadata(df=dataframe, checks=checks)
        if apply_monitoring and monitoring_fqn:
            build_and_store_dqx_stats_table(
                df=check_result,
                target_table=target_table,
                quarantine_table=None,
                stats_table_fqn=monitoring_fqn,
            )

    return check_result


def build_dqx_stats_tabele(
    df: DataFrame, target_table: str | None = None, quarantine_table: str | None = None
) -> DataFrame:
    """Build a DQX stats table with the relevant metadata and metrics."""
    from pyspark.sql.functions import array, coalesce, col, count, explode, first, lit, size

    errors = (
        df.where(size(coalesce(col("_errors"), array())) > 0)
        .select(
            lit("error").alias("severity"),
            explode("_errors").alias("issue"),
        )
        .select("severity", "issue.*")
    )

    warnings = (
        df.where(size(coalesce(col("_warnings"), array())) > 0)
        .select(
            lit("warning").alias("severity"),
            explode("_warnings").alias("issue"),
        )
        .select("severity", "issue.*")
    )

    result = errors.unionByName(warnings)

    result = (
        result.groupBy("rule_fingerprint")
        .agg(
            count("*").alias("issue_count"),
            first("severity", ignorenulls=True).alias("severity"),
            first("name", ignorenulls=True).alias("name"),
            first("message", ignorenulls=True).alias("message"),
            first("columns", ignorenulls=True).alias("columns"),
            first("filter", ignorenulls=True).alias("filter"),
            first("function", ignorenulls=True).alias("function"),
            first("run_time", ignorenulls=True).alias("run_time"),
            first("run_id", ignorenulls=True).alias("run_id"),
            first("user_metadata", ignorenulls=True).alias("user_metadata"),
            first("rule_set_fingerprint", ignorenulls=True).alias("rule_set_fingerprint"),
            first("skipped", ignorenulls=True).alias("skipped"),
        )
        .withColumn("target_table", lit(target_table))
        .withColumn("quarantine_table", lit(quarantine_table))
        .select(
            "target_table",
            "quarantine_table",
            "severity",
            "name",
            "message",
            "columns",
            "issue_count",
            "filter",
            "function",
            "run_time",
            "run_id",
            "user_metadata",
            "rule_fingerprint",
            "rule_set_fingerprint",
            "skipped",
        )
    )

    return result


def store_dqx_stats_table(
    stats_df: DataFrame,
    stats_table_fqn: str,
) -> None:
    """Store the DQX stats DataFrame in the specified table."""
    stats_df.write.format("delta").mode("append").saveAsTable(stats_table_fqn)


def build_and_store_dqx_stats_table(
    df: DataFrame,
    target_table: str | None,
    quarantine_table: str | None,
    stats_table_fqn: str,
) -> None:
    """Build the DQX stats table from the given DataFrame and store it in the specified table."""
    stats_df = build_dqx_stats_tabele(df, target_table, quarantine_table)
    store_dqx_stats_table(stats_df, stats_table_fqn)
