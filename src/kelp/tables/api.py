"""Table metadata accessor functions backed by :class:`~kelp.service.table_manager.TableManager`.

Every function in this module delegates to
:meth:`TableManager.build_table` which returns a lightweight
:class:`~kelp.service.table_manager.KelpTable` — no SDP expectations or
pipeline-specific logic is involved.
"""

from kelp.models.table import Column
from kelp.service.table_manager import KelpTable, TableManager


def get_table(name: str) -> KelpTable:
    """Get the KelpTable object for a given table name.

    Retrieves a KelpTable instance with all computed properties including
    fully qualified name, schema DDL, and table metadata — without any
    Spark Declarative Pipeline (SDP) expectations.

    Args:
        name: Table name to retrieve.

    Returns:
        KelpTable with all generic properties and computed values.

    Raises:
        KeyError: If the table name is not found in the catalog.
    """
    return TableManager.build_table(name)


def ref(name: str) -> str:
    """Get the fully qualified name for a table.

    Returns the fully qualified name (``catalog.schema.name``) of the table.
    Suitable for use in ``spark.table()`` or SQL queries in any Spark job.

    Args:
        name: Table name.

    Returns:
        Fully qualified table name.
    """
    return TableManager.build_table(name).fqn or name


def schema(name: str) -> str | None:
    """Get the Spark schema DDL for a table.

    Returns the full Spark schema definition including constraints and
    generated columns.

    Args:
        name: Table name.

    Returns:
        Spark schema DDL string, or None if not available.
    """
    return TableManager.build_table(name).schema


def schema_lite(name: str) -> str | None:
    """Get the raw Spark schema without constraints or generated columns.

    Returns the basic Spark schema definition without any modifications,
    suitable for StructType operations or type references.

    Args:
        name: Table name.

    Returns:
        Raw Spark schema DDL string, or None if not available.
    """
    return TableManager.build_table(name).schema_lite


def ddl(name: str, if_not_exists: bool = True) -> str | None:
    """Get the full CREATE TABLE DDL statement for a table.

    Generates the Databricks SQL DDL including column definitions,
    constraints, clustering, and table properties.

    Args:
        name: Table name.
        if_not_exists: If ``True``, generates ``CREATE TABLE IF NOT EXISTS``.

    Returns:
        DDL statement string, or ``None`` if not available.
    """
    return TableManager.build_table(name).get_ddl(if_not_exists=if_not_exists)


def columns(name: str) -> list[Column]:
    """Get the column definitions for a table.

    Returns the list of :class:`~kelp.models.table.Column` objects from the
    table metadata. Useful for building ``StructType`` schemas or inspecting
    column types programmatically.

    Args:
        name: Table name.

    Returns:
        List of Column definitions. Empty list if no columns are defined.

    Raises:
        KeyError: If the table name is not found in the catalog.
    """
    table = TableManager.build_table(name)
    if table.root_table:
        return table.root_table.columns
    return []


def func(name: str) -> str:
    """Get the fully qualified name for a Unity Catalog function.

    Returns the fully qualified name (``catalog.schema.function_name``) of the
    function for use in PySpark expressions and SQL queries.

    Args:
        name: Function name.

    Returns:
        Fully qualified function name.

    Raises:
        KeyError: If the function name is not found in the catalog.
    """
    from kelp.config import get_context

    context = get_context()
    return context.catalog.get_function(name).get_qualified_name()


def source(name: str) -> str:
    """Get the path for a data source.

    Returns the path for a source (volume, table, or raw path) that can be used
    in any Spark job for reading or writing data.

    For table sources: returns the fully qualified name (catalog.schema.table_name)
    For volume sources: returns the volume path (/Volumes/catalog/schema/volume)
    For raw_path sources: returns the configured path

    Args:
        name: Source name.

    Returns:
        Path string suitable for use with spark.read or spark.write.

    Raises:
        KeyError: If the source is not found in the catalog.
        ValueError: If the source configuration is incomplete.
    """
    from kelp.service.source_manager import SourceManager

    return SourceManager.get_path(name)


def source_options(name: str) -> dict:
    """Get the options dictionary for a data source.

    Returns source-specific options that can be passed to Spark readers/writers
    or used for configuring the source behavior.

    Args:
        name: Source name.

    Returns:
        Dictionary of source-specific options.

    Raises:
        KeyError: If the source is not found in the catalog.
    """
    from kelp.service.source_manager import SourceManager

    return SourceManager.get_options(name)
