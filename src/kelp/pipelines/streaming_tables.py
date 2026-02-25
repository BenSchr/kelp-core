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
    """Combine multiple SQL rule expressions with AND for a single validity predicate."""
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
    """Apply expectation decorators based on quality configuration."""
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


def streaming_table(
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
    """Drop-in replacement for @dp.table geared for streaming tables with
    built-in data quality quarantine support.
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
    """Drop-in replacement for dp.create_streaming_table."""

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
