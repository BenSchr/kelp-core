"""Shared SQL templates and string-escaping helpers for UC query builders."""

from __future__ import annotations

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
