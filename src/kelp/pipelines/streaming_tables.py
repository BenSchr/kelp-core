from collections.abc import Callable
from typing import Any

from pyspark import pipelines as dp  # Databricks / Spark SDP
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr

from kelp.pipelines.utils import merge_params
from kelp.service.table_manager import TableManager

# -----------------------
# Helpers
# -----------------------


def _combine_rules(rules: dict[str, str], apply_not: bool = False) -> str:
    """Combine multiple SQL rule expressions with AND for a single validity predicate.

    Args:
        rules: Dictionary mapping rule names to SQL expressions.
        apply_not: If True, wraps the combined expression in NOT(...).

    Returns:
        Combined SQL predicate expression or "TRUE" if rules is empty.
    """
    if not rules:
        return "TRUE"
    predicate = " AND ".join(f"({sql})" for sql in rules.values())
    if apply_not:
        predicate = f"NOT ({predicate})"
    return predicate


def _apply_expectations(
    func: Callable[..., DataFrame],
    expect_all: dict[str, str] | None = None,
    expect_all_or_drop: dict[str, str] | None = None,
    expect_all_or_fail: dict[str, str] | None = None,
) -> Callable[..., DataFrame]:
    """Apply SDP expectation decorators based on quality configuration.

    Wraps a DataFrame-returning function with Databricks SDP expectation decorators
    to enforce data quality checks at query execution time.

    Args:
        func: Function returning a DataFrame to decorate.
        expect_all: Dictionary of SQL expressions that must all pass.
        expect_all_or_drop: Dictionary of SQL expressions; failing rows are dropped.
        expect_all_or_fail: Dictionary of SQL expressions; job fails if any fail.

    Returns:
        Decorated function with quality checks applied.
    """
    expect_all_func: Any = getattr(dp, "expect_all", None)
    expect_all_or_drop_func: Any = getattr(dp, "expect_all_or_drop", None)
    expect_all_or_fail_func: Any = getattr(dp, "expect_all_or_fail", None)
    if expect_all and expect_all_func:
        func = expect_all_func(expect_all)(func)
    if expect_all_or_drop and expect_all_or_drop_func:
        func = expect_all_or_drop_func(expect_all_or_drop)(func)
    if expect_all_or_fail and expect_all_or_fail_func:
        func = expect_all_or_fail_func(expect_all_or_fail)(func)

    return func


# -----------------------
# The user-facing decorator
# -----------------------


def table(
    # --- standard @dp.table parameters (keep signature compatible) ---
    query_function: Callable[..., DataFrame] | None = None,
    *,
    name: str | None = None,
    comment: str | None = None,
    spark_conf: dict[str, str] | None = None,
    table_properties: dict[str, str] | None = None,
    path: str | None = None,
    partition_cols: list[str] | None = None,
    cluster_by_auto: bool = False,
    cluster_by: list[str] | None = None,
    schema: str | Any | None = None,
    row_filter: str | None = None,
    private: bool | None = None,
    # Kelp specific expectation parameters
    expect_all: dict[str, str] | None = None,
    expect_all_or_drop: dict[str, str] | None = None,
    expect_all_or_fail: dict[str, str] | None = None,
    expect_all_or_quarantine: dict[str, str] | None = None,
    # Exclude
    exclude_params: list[str] | None = None,
    # Future extension point
    **kwargs,
) -> Callable[[Callable[..., DataFrame]], None] | None:
    """Drop-in replacement for @dp.table with built-in data quality and quarantine support.

    This decorator extends Databricks' standard @dp.table with enhanced data quality
    processing. It supports both SDP (Spark Data Pruning) expectations and DQX checks,
    with automatic quarantine table creation for failed records.

    Features:
    - Automatic schema and parameter discovery from table metadata
    - SDP expectations (expect_all, expect_all_or_drop, expect_all_or_fail)
    - Quarantine table support for failed records
    - DQX integration for complex quality checks
    - Parameter override support for flexibility

    Args:
        query_function: The decorated function returning a DataFrame (for @decorator form).
        name: Table name. If not provided, uses function name.
        comment: Table description/comment.
        spark_conf: Spark configuration properties.
        table_properties: Databricks table properties.
        path: Physical path for external tables or custom locations.
        partition_cols: List of column names for partitioning.
        cluster_by_auto: Enable automatic clustering optimization.
        cluster_by: List of column names for explicit clustering (max 4).
        schema: Table schema definition (DDL or StructType).
        row_filter: SQL row filter expression.
        private: Whether the table should be private.
        expect_all: Dictionary of SQL expressions that must all pass.
        expect_all_or_drop: Dictionary of SQL expressions; failing rows are dropped.
        expect_all_or_fail: Dictionary of SQL expressions; job fails if any fail.
        expect_all_or_quarantine: Dictionary of SQL expressions; failing rows quarantined.
        exclude_params: List of parameter keys to exclude from metadata discovery.
        **kwargs: Additional arguments passed to the underlying @dp.table.

    Returns:
        Decorator function or None if called with a query function directly.

    Raises:
        ValueError: If validation or quarantine table names cannot be determined.
        ImportError: If DQX checks are configured but databricks-labs-dqx is not installed.

    Example:
        @streaming_table(name="my_table", expect_all={"non_null": "column1 IS NOT NULL"})
        def get_my_data():
            return spark.read.table("source_table")
    """
    # Build a dict of explicit overwrites provided at callsite.
    spark = SparkSession.active()
    params_passed = dict(
        name=name,
        comment=comment,
        spark_conf=spark_conf,
        table_properties=table_properties,
        path=path,
        partition_cols=partition_cols,
        cluster_by_auto=cluster_by_auto,
        cluster_by=cluster_by,
        schema=schema,
        row_filter=row_filter,
        private=private,
        expect_all=expect_all,
        expect_all_or_drop=expect_all_or_drop,
        expect_all_or_fail=expect_all_or_fail,
        expect_all_or_quarantine=expect_all_or_quarantine,
    )
    passed_kwargs = kwargs

    def outer(decorated: Callable[..., DataFrame]) -> None:

        table_name = name or getattr(decorated, "__name__", "unknown")
        sdp_table = TableManager.build_sdp_table(table_name)
        # table_def = TableManager(table_name, soft_handle=True)
        # meta_params = table_def.get_sdp_table_params_as_dict(exclude=exclude_params or [])
        meta_params = sdp_table.params_raw(exclude=exclude_params or [])

        params = merge_params(params_passed, meta_params, passed_kwargs)

        fqn = str(params.get("name") or table_name)
        expect_all = params.pop("expect_all", None)
        expect_all_or_drop = params.pop("expect_all_or_drop", None)
        expect_all_or_fail = params.pop("expect_all_or_fail", None)
        expect_all_or_quarantine = params.pop("expect_all_or_quarantine", None)

        # validation_table_name = table_def.get_validation_table_name()
        # quarantine_table_name = table_def.get_quarantine_table_name()
        validation_table_name = sdp_table.validation_table
        quarantine_table_name = sdp_table.quarantine_table
        if validation_table_name is None or quarantine_table_name is None:
            raise ValueError("Validation or quarantine table name is missing.")

        dqx_obj = sdp_table.get_dqx_check_obj()

        if dqx_obj:
            try:
                from databricks.labs.dqx.engine import DQEngine
            except ImportError as e:
                raise ImportError(
                    "DQX is required for using DQX checks in @streaming_table. Please install databricks-labs-dqx."
                    "For more information check https://databrickslabs.github.io/dqx/",
                ) from e

            from databricks.sdk import WorkspaceClient

            dq_engine = DQEngine(WorkspaceClient())
            dqx_checks = dqx_obj.checks

            def _apply_dqx_checks() -> DataFrame:
                df = decorated()
                result = dq_engine.apply_checks_by_metadata(df, dqx_checks)
                # result = df
                if isinstance(result, tuple):
                    # DQX may return (df, observation)
                    return result[0]
                return result

            sdp_level = dqx_obj.sdp_expect_level
            if sdp_level != "deactivate":
                if sdp_level == "warn":
                    expect_all = {"dqx_error": "_errors IS NULL"}
                elif sdp_level == "fail":
                    expect_all_or_fail = {"dqx_error": "_errors IS NULL"}
                elif sdp_level == "drop":
                    expect_all_or_drop = {"dqx_error": "_errors IS NULL"}

            validty_func = _apply_expectations(
                _apply_dqx_checks,
                expect_all,
                expect_all_or_drop,
                expect_all_or_fail,
            )

            if dqx_obj.sdp_quarantine:
                dp.table(
                    name=validation_table_name,
                    private=True,
                )(validty_func)  # ty:ignore[no-matching-overload]

                @dp.table(**params)
                def valid_table():
                    df = spark.readStream.table(validation_table_name)
                    return dq_engine.get_valid(df)

                @dp.table(
                    name=quarantine_table_name,
                    comment=f"Quarantined rows from {fqn}",
                )
                def invalid_table():
                    df = spark.readStream.table(validation_table_name)
                    return dq_engine.get_invalid(df)
            else:
                dp.table(**params)(validty_func)

        elif expect_all_or_quarantine:
            not_combined = _combine_rules(expect_all_or_quarantine, apply_not=True)
            quarantine_col = "is_quarantined"

            def validity_wrapper():
                return decorated().withColumn(quarantine_col, expr(not_combined))

            if not expect_all:
                expect_all = {}
            expect_all.update({"quarantine_col": f"{quarantine_col} = false"})

            validty_func = _apply_expectations(
                validity_wrapper,
                expect_all,
                expect_all_or_drop,
                expect_all_or_fail,
            )

            dp.table(
                name=validation_table_name,
                private=True,
                partition_cols=[quarantine_col],
            )(validty_func)  # ty:ignore[no-matching-overload]

            # Create two table one quarantined and one valid rows only.
            # Original table now only has valid rows.

            @dp.table(**params)
            def valid_table():
                return (
                    spark.readStream.table(validation_table_name)
                    .filter(f"{quarantine_col} = false")
                    .drop(quarantine_col)
                )

            # Quarantined table with invalid rows only.

            @dp.table(
                name=quarantine_table_name,
                comment=f"Quarantined rows from {fqn}",
            )
            def invalid_table():
                return (
                    spark.readStream.table(validation_table_name)
                    .filter(f"{quarantine_col} = true")
                    .drop(quarantine_col)
                )

        else:
            dp.table(**params)(
                _apply_expectations(decorated, expect_all, expect_all_or_drop, expect_all_or_fail),
            )

    # Support both @decorator and @decorator(...) forms, like @dp.table.
    if query_function is not None and callable(query_function):
        outer(query_function)
        return None
    return outer


def create_streaming_table(
    *,
    name: str,
    comment: str | None = None,
    spark_conf: dict[str, str] | None = None,
    table_properties: dict[str, str] | None = None,
    path: str | None = None,
    partition_cols: list[str] | None = None,
    cluster_by_auto: bool | None = None,
    cluster_by: list[str] | None = None,
    schema: str | Any | None = None,
    row_filter: str | None = None,
    # Databricks supports expectation params directly on create_streaming_table
    expect_all: dict[str, str] | None = None,
    expect_all_or_drop: dict[str, str] | None = None,
    expect_all_or_fail: dict[str, str] | None = None,
    # Exclude
    exclude_params: list[str] | None = None,
    # Future extension point
    **kwargs,
) -> None:
    """Enhanced version of dp.create_streaming_table with Kelp metadata discovery.

    Creates a Databricks streaming table with automatic parameter discovery from
    table metadata. This is useful for programmatic table creation where table
    configuration is defined in YAML.

    Args:
        name: Table name (required). Used to look up configuration in metadata.
        comment: Table description/comment.
        spark_conf: Spark configuration properties.
        table_properties: Databricks table properties.
        path: Physical path for external tables or custom locations.
        partition_cols: List of column names for partitioning.
        cluster_by_auto: Enable automatic clustering optimization.
        cluster_by: List of column names for explicit clustering (max 4).
        schema: Table schema definition (DDL or StructType).
        row_filter: SQL row filter expression.
        expect_all: Dictionary of SQL expressions that must all pass.
        expect_all_or_drop: Dictionary of SQL expressions; failing rows are dropped.
        expect_all_or_fail: Dictionary of SQL expressions; job fails if any fail.
        exclude_params: List of parameter keys to exclude from metadata discovery.
        **kwargs: Additional arguments passed to dp.create_streaming_table.

    Raises:
        KeyError: If the table name is not found in the catalog.
    """

    params = dict(
        name=name,
        comment=comment,
        spark_conf=spark_conf,
        table_properties=table_properties,
        path=path,
        partition_cols=partition_cols,
        cluster_by_auto=cluster_by_auto,
        cluster_by=cluster_by,
        schema=schema,
        row_filter=row_filter,
        expect_all=expect_all,
        expect_all_or_drop=expect_all_or_drop,
        expect_all_or_fail=expect_all_or_fail,
    )

    meta_params = TableManager.build_sdp_table(name).params_cst(exclude=exclude_params or [])

    params = merge_params(params, meta_params, kwargs)

    dp.create_streaming_table(**params)
