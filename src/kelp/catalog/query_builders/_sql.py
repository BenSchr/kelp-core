"""Shared SQL templates and string-escaping helpers for UC query builders."""

from kelp.catalog.uc_models import GrantDiff
from kelp.models.grant import normalize_privilege

# ---------------------------------------------------------------------------
# ALTER / COMMENT templates
# ---------------------------------------------------------------------------

BASE_ALTER = "ALTER {table_type} {fqn} {action}"
ALTER_COLUMN = "ALTER COLUMN {col} {action}"

COMMENT_ON = "COMMENT ON {type} {path} IS '{comment}'"
SET_COMMENT = "COMMENT '{comment}'"

# ---------------------------------------------------------------------------
# Tag templates
# ---------------------------------------------------------------------------

SET_TAGS = "SET TAGS ({tags})"
UNSET_TAGS = "UNSET TAGS ({tags})"

# Used by views for per-column tag mutations (no ALTER TABLE equivalent)
SET_TAG_ON = "SET TAG ON {type} {path} `{key}`=`{value}`"
UNSET_TAG_ON = "UNSET TAG ON {type} {path} `{key}`"

# ---------------------------------------------------------------------------
# TBLPROPERTIES templates
# ---------------------------------------------------------------------------

SET_TBLPROPERTIES = "SET TBLPROPERTIES ({props})"
UNSET_TBLPROPERTIES = "UNSET TBLPROPERTIES ({props})"

# ---------------------------------------------------------------------------
# CLUSTER BY templates
# ---------------------------------------------------------------------------

CLUSTER_BY_AUTO = "ALTER TABLE {fqn} CLUSTER BY AUTO"
CLUSTER_BY_NONE = "ALTER TABLE {fqn} CLUSTER BY NONE"
CLUSTER_BY_COLS = "ALTER TABLE {fqn} CLUSTER BY ({cols})"

# ---------------------------------------------------------------------------
# Constraint templates
# ---------------------------------------------------------------------------

ADD_PK = "ALTER TABLE {fqn} ADD CONSTRAINT {name} PRIMARY KEY ({cols})"
ADD_FK = (
    "ALTER TABLE {fqn} ADD CONSTRAINT {name} "
    "FOREIGN KEY ({cols}) REFERENCES {ref_table} ({ref_cols})"
)
DROP_CONSTRAINT = "ALTER TABLE {fqn} DROP CONSTRAINT {name}"

AUTO_TTL = "ALTER TABLE {fqn} DELETE ROWS {expiration} DAYS AFTER {time_col}"
DROP_AUTO_TTL = "ALTER TABLE {fqn} DROP ROW DELETION"

# ---------------------------------------------------------------------------
# Grant templates
# ---------------------------------------------------------------------------

GRANT_ON = "GRANT {privileges} ON {target} TO {principal}"
REVOKE_ON = "REVOKE {privileges} ON {target} FROM {principal}"

# ---------------------------------------------------------------------------
# String helpers
# ---------------------------------------------------------------------------


def esc(value: str) -> str:
    """Escape single-quotes inside a SQL string literal."""
    return value.replace("'", "''")


def kv_tags(tags: dict[str, str]) -> str:
    """Format ``{k: v, ...}`` as ``'k1'='v1', 'k2'='v2'`` for SET TAGS."""
    return ", ".join(f"'{esc(k)}'='{esc(v)}'" for k, v in tags.items())


def key_list(keys: list[str]) -> str:
    """Format ``[k1, k2]`` as ``'k1', 'k2'`` for UNSET TAGS / TBLPROPERTIES."""
    return ", ".join(f"'{esc(k)}'" for k in keys)


def quote_principal(principal: str) -> str:
    """Quote a SQL principal as a delimited identifier."""
    return f"`{principal.replace('`', '``')}`"


def grant_statements(diff: GrantDiff, target: str) -> list[str]:
    """Render a grant diff, applying full revokes before grants.

    Args:
        diff: Direct grant changes for one object.
        target: SQL securable name, optionally prefixed with ``FUNCTION``.

    Returns:
        SQL statements in revoke-all, partial-revoke, then grant order.
    """
    statements = [
        REVOKE_ON.format(
            privileges="ALL PRIVILEGES",
            target=target,
            principal=quote_principal(principal),
        )
        for principal in diff.revoke_all
    ]
    statements.extend(
        REVOKE_ON.format(
            privileges=", ".join(normalize_privilege(value) for value in privileges),
            target=target,
            principal=quote_principal(principal),
        )
        for principal, privileges in sorted(
            diff.revokes.items(), key=lambda item: item[0].casefold()
        )
        if privileges
    )
    statements.extend(
        GRANT_ON.format(
            privileges=", ".join(normalize_privilege(value) for value in privileges),
            target=target,
            principal=quote_principal(principal),
        )
        for principal, privileges in sorted(
            diff.grants.items(), key=lambda item: item[0].casefold()
        )
        if privileges
    )
    return statements
