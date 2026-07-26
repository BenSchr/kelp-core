import logging
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


@dataclass
class QualityResult:
    """Outcome of running DQX checks, with nothing written yet.

    Attributes:
        dataframe: Rows to materialize into the target.
        quarantine_df: Rows failing checks, or None when there are none to quarantine.
        stats_df: Monitoring rows, or None when monitoring is off or there is nothing to report.
        has_errors: Whether any error-severity violation was found.
    """

    dataframe: DataFrame
    quarantine_df: DataFrame | None = None
    stats_df: DataFrame | None = None
    has_errors: bool = False


class _OfflineConfig:
    """Minimal stand-in for ``databricks.sdk.core.Config`` used off Databricks."""

    _product_info: tuple[str, str] | None = None

    def copy(self) -> "_OfflineConfig":
        """Return a copy of this config (itself, since it carries no state)."""
        return self

    def with_user_agent_extra(self, key: str, value: str) -> "_OfflineConfig":
        """Ignore the telemetry user-agent extra and return this config."""
        return self


class _OfflineClusters:
    """Minimal stand-in for the ``clusters`` API of ``WorkspaceClient``."""

    def select_spark_version(self, *args: Any, **kwargs: Any) -> str:
        """Answer DQX's workspace connectivity probe without contacting a workspace."""
        return ""


class _OfflineWorkspaceClient:
    """Minimal stand-in for ``WorkspaceClient`` for runs without a workspace.

    DQX only touches two things on the client while applying checks: ``config``
    (it reads and writes the private ``_product_info`` attribute and calls
    ``copy()`` / ``with_user_agent_extra()`` for telemetry) and
    ``clusters.select_spark_version()`` (a connectivity probe that is not
    error-handled at construction time). Telemetry also rebuilds the client as
    ``type(client)(config=...)``, hence the keyword argument.

    Args:
        config: Config object to expose; a fresh offline config by default.
    """

    def __init__(self, config: _OfflineConfig | None = None) -> None:
        self.config = config if config is not None else _OfflineConfig()
        self.clusters = _OfflineClusters()


def _resolve_workspace_client(workspace_client: Any | None) -> Any:
    """Resolve the workspace client handed to ``DQEngine``.

    Args:
        workspace_client: Explicit client, used as-is when provided.

    Returns:
        The injected client, a real ``WorkspaceClient`` when running on
        Databricks, or an offline placeholder for local development.
    """
    if workspace_client is not None:
        return workspace_client

    from kelp.utils.databricks import on_databricks

    if on_databricks():
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient()

    logger.debug(
        "Not running on Databricks: using an offline placeholder workspace client for DQX."
    )
    return _OfflineWorkspaceClient()


def run_dqx_quality_checks(
    *,
    dataframe: DataFrame,
    checks: list[dict],
    violation_action: str,
    target_table: str | None = None,
    quarantine_enabled: bool = False,
    quarantine_fqn: str | None = None,
    monitoring_fqn: str | None = None,
    workspace_client: Any | None = None,
) -> QualityResult:
    """Run DQX checks and return the resulting DataFrames.

    Writes nothing and raises nothing for violations: the caller decides what to
    persist, in which order, and whether violations are fatal.

    Args:
        dataframe: Input DataFrame to check.
        checks: DQX checks in metadata (dict) form.
        violation_action: One of ``"error"``, ``"ignore"`` or ``"drop"``. Anything
            other than ``"ignore"`` splits the frame into good and bad rows;
            ``"ignore"`` returns the annotated frame unchanged.
        target_table: Fully qualified name of the target table, recorded in the
            monitoring rows.
        quarantine_enabled: Whether failing rows should be quarantined.
        quarantine_fqn: Fully qualified name of the quarantine table.
        monitoring_fqn: Fully qualified name of the monitoring table; None
            disables stats building.
        workspace_client: Optional ``WorkspaceClient`` for DQX. When omitted, a
            real client is created on Databricks and an offline placeholder is
            used elsewhere.

    Returns:
        A QualityResult with the rows to materialize plus the optional quarantine
        and monitoring frames.
    """
    from databricks.labs.dqx.engine import DQEngine

    dqx_engine = DQEngine(_resolve_workspace_client(workspace_client))

    if violation_action == "ignore":
        annotated_df = dqx_engine.apply_checks_by_metadata(df=dataframe, checks=checks)
        return QualityResult(
            dataframe=annotated_df,
            stats_df=build_dqx_stats_table(annotated_df, target_table, None)
            if monitoring_fqn
            else None,
        )

    good_df, bad_df = dqx_engine.apply_checks_by_metadata_and_split(df=dataframe, checks=checks)

    if bad_df.isEmpty():
        return QualityResult(dataframe=good_df)

    return QualityResult(
        dataframe=good_df,
        quarantine_df=bad_df if quarantine_enabled and quarantine_fqn else None,
        stats_df=build_dqx_stats_table(
            bad_df, target_table, quarantine_fqn if quarantine_enabled else None
        )
        if monitoring_fqn
        else None,
        has_errors=not bad_df.filter("_errors IS NOT NULL").isEmpty(),
    )


def store_quarantine_rows(
    quarantine_df: DataFrame,
    quarantine_fqn: str,
    merge_schema: bool = True,
) -> None:
    """Append failing rows to the quarantine table.

    Args:
        quarantine_df: Failing rows to append.
        quarantine_fqn: Fully qualified name of the quarantine table.
        merge_schema: Whether columns missing from the target are added to it
            (Delta ``mergeSchema``). The DQX result columns evolve with the kelp
            and DQX versions, so an additive schema change must not fail a write
            into an existing quarantine table.
    """
    (
        quarantine_df.write.format("delta")
        .option("mergeSchema", "true" if merge_schema else "false")
        .mode("append")
        .saveAsTable(quarantine_fqn)
    )


def build_dqx_stats_table(
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
        result.groupBy("rule_fingerprint", "severity")
        .agg(
            count("*").alias("issue_count"),
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
    merge_schema: bool = True,
) -> None:
    """Store the DQX stats DataFrame in the specified table.

    Args:
        stats_df: Stats rows to append.
        stats_table_fqn: Fully qualified name of the monitoring table.
        merge_schema: Whether columns missing from the target are added to it
            (Delta ``mergeSchema``). Independent pipelines on different kelp or DQX
            versions append to the same central monitoring table, so an additive
            schema change must not fail whichever producer is not upgraded yet.
    """
    (
        stats_df.write.format("delta")
        .option("mergeSchema", "true" if merge_schema else "false")
        .mode("append")
        .saveAsTable(stats_table_fqn)
    )


def build_and_store_dqx_stats_table(
    df: DataFrame,
    target_table: str | None,
    quarantine_table: str | None,
    stats_table_fqn: str,
    merge_schema: bool = True,
) -> None:
    """Build the DQX stats table from the given DataFrame and store it in the specified table."""
    stats_df = build_dqx_stats_table(df, target_table, quarantine_table)
    store_dqx_stats_table(stats_df, stats_table_fqn, merge_schema=merge_schema)
