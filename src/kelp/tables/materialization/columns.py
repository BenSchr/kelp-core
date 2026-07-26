"""Pure column-resolution helpers shared by the materialization strategies.

Nothing in this module touches Spark: every function maps column names and
config onto SQL fragments, which keeps the merge combinatorics unit-testable.
"""

from kelp.models.model_mat_config import SOURCE_ALIAS, TARGET_ALIAS

#: Source-side helper column carrying the ``when_deleted`` predicate result.
IS_DELETE_COL = "__kelp_is_delete"
#: Source-side helper column carrying the effective sequence value.
SEQUENCE_COL = "__kelp_seq"
#: Source-side helper column marking whether a row came from the target (1) or source (2).
ORIGIN_COL = "__kelp_origin"
#: Helper column carrying a window row number used to deduplicate rows.
ROW_NUMBER_COL = "__kelp_rn"
#: Helper column marking whether a rebuilt version survives deduplication.
KEEP_COL = "__kelp_keep"
#: Helper column carrying the sequence value that closes a version's interval.
SUPERSEDED_AT_COL = "__kelp_superseded_at"

#: Helper columns never written to a target table.
HELPER_COLUMNS = (
    IS_DELETE_COL,
    SEQUENCE_COL,
    ORIGIN_COL,
    ROW_NUMBER_COL,
    KEEP_COL,
    SUPERSEDED_AT_COL,
)


def index(columns: list[str]) -> dict[str, str]:
    """Index column names by their lowercase form.

    Args:
        columns: Column names.

    Returns:
        Mapping of lowercase name to the name as written.
    """
    return {col.lower(): col for col in columns}


def resolve(names: list[str], available: dict[str, str]) -> tuple[list[str], list[str]]:
    """Split requested names into the ones that exist and the ones that do not.

    Args:
        names: Requested column names.
        available: Index produced by :func:`index`.

    Returns:
        Tuple of (resolved names as written, missing names as requested).
    """
    resolved = [available[name.lower()] for name in names if name.lower() in available]
    missing = [name for name in names if name.lower() not in available]
    return resolved, missing


def without(columns: list[str], excluded: list[str]) -> list[str]:
    """Return ``columns`` minus ``excluded``, case-insensitively."""
    drop = {name.lower() for name in excluded}
    return [col for col in columns if col.lower() not in drop]


def qualified(alias: str, column: str) -> str:
    """Return a backtick-quoted ``alias.column`` reference."""
    return f"{alias}.`{column}`"


def change_condition(
    columns: list[str],
    *,
    source_alias: str = SOURCE_ALIAS,
    target_alias: str = TARGET_ALIAS,
) -> str | None:
    """Build a null-safe "any of these columns changed" condition.

    Args:
        columns: Columns to compare between source and target.
        source_alias: Merge alias of the source.
        target_alias: Merge alias of the target.

    Returns:
        SQL condition, or ``None`` when ``columns`` is empty.
    """
    if not columns:
        return None
    comparisons = [
        f"NOT ({qualified(source_alias, col)} <=> {qualified(target_alias, col)})"
        for col in columns
    ]
    return " OR ".join(comparisons)


def newer_condition(
    sequence_columns: list[str],
    *,
    source: dict[str, str],
    target: dict[str, str],
    source_alias: str = SOURCE_ALIAS,
    target_alias: str = TARGET_ALIAS,
) -> str | None:
    """Build a condition requiring the source row to be newer than the target row.

    Multi-column sequences are compared as structs, so ordering follows the
    configured column order.

    Args:
        sequence_columns: Configured sequence columns.
        source: Index of source columns.
        target: Index of target columns.
        source_alias: Merge alias of the source.
        target_alias: Merge alias of the target.

    Returns:
        SQL condition, or ``None`` when no sequence column exists on both sides.
    """
    comparable = [
        col for col in sequence_columns if col.lower() in source and col.lower() in target
    ]
    if not comparable:
        return None

    if len(comparable) == 1:
        col = comparable[0]
        return (
            f"{qualified(source_alias, source[col.lower()])} > "
            f"{qualified(target_alias, target[col.lower()])}"
        )

    source_struct = ", ".join(qualified(source_alias, source[c.lower()]) for c in comparable)
    target_struct = ", ".join(qualified(target_alias, target[c.lower()]) for c in comparable)
    return f"struct({source_struct}) > struct({target_struct})"


def combine(*conditions: str | None) -> str | None:
    """AND together the conditions that are set, parenthesising each one."""
    parts = [condition for condition in conditions if condition]
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return " AND ".join(f"({part})" for part in parts)
