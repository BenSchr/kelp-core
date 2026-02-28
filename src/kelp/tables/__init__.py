"""Generic table metadata API for use in any Spark job.

Provides access to Kelp-managed table metadata (schema, DDL, columns, FQN)
without requiring Spark Declarative Pipeline (SDP) dependencies. Use this
module when you need table definitions in regular Spark jobs or
transformations.

Example::

    from kelp import tables

    ddl = tables.ddl("my_table")
    cols = tables.columns("my_table")
    fqn = tables.ref("my_table")
"""

from kelp.tables.api import columns, ddl, get_table, ref, schema, schema_lite

__all__ = [
    "columns",
    "ddl",
    "get_table",
    "ref",
    "schema",
    "schema_lite",
]
