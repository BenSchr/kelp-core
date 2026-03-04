from kelp.service.source_manager import SourceManager
from kelp.service.table_manager import KelpSdpTable, TableManager


def get_table(name: str) -> KelpSdpTable:
    """Get the KelpSdpTable object for a given table name.

    Retrieves a KelpSdpTable instance with all computed properties including
    fully qualified name, schema, quality checks, and partition information.

    Args:
        name: Table name to retrieve.

    Returns:
        KelpSdpTable: Table object with all properties and computed values.

    Raises:
        KeyError: If the table name is not found in the catalog.
    """
    return TableManager.build_sdp_table(name)


def target(name: str) -> str:
    """Get the target table name for a given table.

    Returns the appropriate target table name based on quality configuration:
    - If quarantine is enabled, returns the quarantine/validation table
    - Otherwise returns the main table name (FQN)

    Used to determine which table should be written to in data pipelines based on
    quality configuration.

    Args:
        name: Table name.

    Returns:
        Fully qualified target table name.
    """
    return TableManager.build_sdp_table(name).target_table or name


def ref(name: str) -> str:
    """Get the source reference name for a table.

    Returns the fully qualified name of the source table. This is always the
    main table name, regardless of quality configuration.

    Used to reference the source table in SQL queries and pipeline definitions.

    Args:
        name: Table name.

    Returns:
        Fully qualified source table name (catalog.schema.name).
    """
    return TableManager.build_sdp_table(name).fqn or name


def schema(name: str) -> str | None:
    """Get the Spark schema DDL for a table.

    Returns the full Spark schema definition including constraints and generated
    columns, suitable for use with the @dp.table decorator.

    Args:
        name: Table name.

    Returns:
        Spark schema DDL string, or None if not available.
    """
    return TableManager.build_sdp_table(name).schema


def schema_lite(name: str) -> str | None:
    """Get the raw Spark schema without constraints or generated columns.

    Returns the basic Spark schema definition without any modifications,
    suitable for use with Struct operations or type references.

    Args:
        name: Table name.

    Returns:
        Raw Spark schema DDL string, or None if not available.
    """
    return TableManager.build_sdp_table(name).schema_lite


def params(name: str, exclude: list[str] | None = None) -> dict[str, str]:
    """Get the streaming table parameters as a dictionary.

    Returns all table parameters suitable for the @dp.table decorator, excluding
    quality expectations (expect_all, expect_all_or_drop, expect_all_or_fail,
    expect_all_or_quarantine).

    Args:
        name: Table name.
        exclude: List of parameter keys to additionally exclude from the result.

    Returns:
        Dictionary of streaming table parameters.

    Raises:
        KeyError: If the table name is not found in the catalog.
    """
    exclude = exclude or []
    return TableManager.build_sdp_table(name).params(exclude=exclude)


def params_cst(name: str, exclude: list[str] | None = None) -> dict[str, str]:
    """Get the create_streaming_table API parameters.

    Returns table parameters suitable for the Databricks create_streaming_table API,
    excluding expect_all_or_quarantine which is not supported by the API.

    Args:
        name: Table name.
        exclude: List of parameter keys to additionally exclude from the result.

    Returns:
        Dictionary of create_streaming_table parameters.

    Raises:
        KeyError: If the table name is not found in the catalog.
    """
    exclude = exclude or []
    return TableManager.build_sdp_table(name).params_cst(exclude=exclude)


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
    from kelp.tables import func as tables_func

    return tables_func(name)


def source(name: str) -> str:
    """Get the path for a data source.

    Returns the path for a source (volume, table, or raw path) that can be used
    in pipelines for reading or writing data.

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
    return SourceManager.get_options(name)
