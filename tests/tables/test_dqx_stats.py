"""Unit tests for the DQX monitoring stats DataFrame builder."""

from pyspark.sql import Row, SparkSession

from kelp.tables.quality_validation.dqx import build_dqx_stats_table

STATS_COLUMNS = [
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
]

ISSUE_SCHEMA = (
    "struct<name:string,message:string,columns:array<string>,filter:string,function:string,"
    "run_time:timestamp,run_id:string,user_metadata:map<string,string>,rule_fingerprint:string,"
    "rule_set_fingerprint:string,skipped:boolean>"
)
CHECK_RESULT_SCHEMA = f"id int, _errors array<{ISSUE_SCHEMA}>, _warnings array<{ISSUE_SCHEMA}>"


def _issue(name: str, fingerprint: str) -> dict:
    """Build a DQX issue struct as a dict.

    Args:
        name: Check name.
        fingerprint: Rule fingerprint the issue belongs to.

    Returns:
        A dict matching ``ISSUE_SCHEMA``.
    """
    return {
        "name": name,
        "message": f"{name} failed",
        "columns": ["col_a"],
        "filter": None,
        "function": "is_not_null",
        "run_time": None,
        "run_id": "run-1",
        "user_metadata": {},
        "rule_fingerprint": fingerprint,
        "rule_set_fingerprint": "set-1",
        "skipped": False,
    }


def test_build_dqx_stats_table_keeps_columns_and_order(spark: SparkSession) -> None:
    """The output schema is fixed: same columns, same order, target metadata filled in."""
    df = spark.createDataFrame(
        [Row(id=1, _errors=[_issue("a", "fp-a")], _warnings=None)],
        schema=CHECK_RESULT_SCHEMA,
    )

    stats = build_dqx_stats_table(df, "cat.sch.tgt", "cat.sch.tgt_quarantine")

    assert stats.columns == STATS_COLUMNS
    row = stats.collect()[0]
    assert (row["target_table"], row["quarantine_table"]) == (
        "cat.sch.tgt",
        "cat.sch.tgt_quarantine",
    )
    assert (row["severity"], row["name"], row["issue_count"]) == ("error", "a", 1)


def test_build_dqx_stats_table_splits_severities_of_one_fingerprint(spark: SparkSession) -> None:
    """A fingerprint seen as both error and warning stays two rows with separate counts."""
    shared = "fp-shared"
    df = spark.createDataFrame(
        [
            Row(id=1, _errors=[_issue("shared", shared)], _warnings=None),
            Row(id=2, _errors=[_issue("shared", shared)], _warnings=None),
            Row(id=3, _errors=None, _warnings=[_issue("shared", shared)]),
        ],
        schema=CHECK_RESULT_SCHEMA,
    )

    stats = build_dqx_stats_table(df)

    counts = {row["severity"]: row["issue_count"] for row in stats.collect()}
    assert counts == {"error": 2, "warning": 1}
    assert {row["rule_fingerprint"] for row in stats.collect()} == {shared}


def test_build_dqx_stats_table_without_issues_is_empty(spark: SparkSession) -> None:
    """Rows without errors or warnings produce no stats rows."""
    df = spark.createDataFrame(
        [Row(id=1, _errors=None, _warnings=[])],
        schema=CHECK_RESULT_SCHEMA,
    )

    assert build_dqx_stats_table(df).isEmpty()
