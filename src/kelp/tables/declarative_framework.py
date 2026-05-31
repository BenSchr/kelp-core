# import functools
# import logging
# from collections.abc import Callable
# from typing import Any

# from pyspark.sql import DataFrame, SparkSession
# from pyspark.sql import functions as F

# from kelp.models.model_config import ModelConfig
# from kelp.service.model_manager import KelpModel, ModelManager
# from kelp.tables.model_context import ModelContext

# logger = logging.getLogger(__name__)


# def _parse_model_name(name: str) -> tuple[str | None, str | None, str]:
#     """Split a potentially qualified name into ``(catalog, db_schema, table_name)``.

#     Accepts ``table``, ``schema.table``, or ``catalog.schema.table`` forms.
#     """
#     parts = name.split(".", 2)
#     if len(parts) == 3:
#         return parts[0], parts[1], parts[2]
#     if len(parts) == 2:
#         return None, parts[0], parts[1]
#     return None, None, parts[0]


# def _table_exists(spark: SparkSession, fqn: str) -> bool:
#     """Return whether *fqn* exists in the active catalog, defaulting to ``False`` on error."""
#     try:
#         return spark.catalog.tableExists(fqn)
#     except Exception:  # noqa: BLE001
#         logger.debug("Could not determine if target '%s' exists.", fqn)
#         return False


# def _make_kelp_model(
#     table_name: str,
#     spark_schema: str | None = None,
#     config: ModelConfig | None = None,
# ) -> KelpModel:
#     """Build a minimal :class:`KelpModel` from explicit name components."""
#     fqn_parts = table_name.split(".")
#     kelp_model = KelpModel(name=fqn_parts[-1], fqn=table_name, schema=spark_schema, config=config)
#     if spark_schema is not None:
#         kelp_model.schema = spark_schema
#     return kelp_model


# def _resolve_model(
#     table_name: str,
#     *,
#     is_custom: bool,
#     spark_schema: str | None = None,
#     config: ModelConfig | None = None,
# ) -> KelpModel:
#     """Return the :class:`KelpModel` to use for *table_name*.

#     Custom models (multipart name or not present in metadata) are built
#     directly from the supplied components. Metadata models are fetched via
#     :class:`~kelp.service.model_manager.ModelManager` and optionally patched
#     with decorator overrides.
#     """
#     if is_custom:
#         return _make_kelp_model(table_name, spark_schema=spark_schema, config=config)

#     from kelp.config import get_context

#     ctx = get_context()
#     model_in_catalog = ctx.catalog_index.get_index("models").get(table_name)

#     if model_in_catalog is None:
#         logger.debug("Model '%s' not found in metadata; treating as custom.", table_name)
#         return _make_kelp_model(table_name, spark_schema=spark_schema, config=config)

#     kelp_model = ModelManager.build_model(table_name, ctx=ctx)

#     return kelp_model


# def _materialize(
#     spark: SparkSession,
#     result: DataFrame,
#     fqn: str,
#     write_config: ModelConfig,
# ) -> None:
#     """Apply *write_config* to persist *result* to *fqn*."""
#     if write_config.write_mode == "merge":
#         _execute_merge(result, fqn, write_config)
#     elif write_config.write_mode == "view":
#         _execute_view(spark, result, fqn)
#     elif write_config.write_mode in ("append", "overwrite"):
#         result.write.format(write_config.table_format).options(**write_config.options).mode(
#             write_config.write_mode
#         ).saveAsTable(fqn)


# def _execute_view(spark: SparkSession, result: DataFrame, fqn: str) -> None:
#     """Register *result* as a view named *fqn*."""
#     import uuid

#     tmp = f"_kelp_tmp_{uuid.uuid4().hex}"
#     result.createOrReplaceTempView(tmp)
#     spark.sql(f"CREATE OR REPLACE VIEW {fqn} AS SELECT * FROM {tmp}")  # noqa: S608
#     spark.catalog.dropTempView(tmp)


# def _execute_merge(source: DataFrame, fqn: str, config: ModelConfig) -> None:
#     """Execute a ``MERGE INTO`` of *source* into *fqn* using the native PySpark API."""
#     from delta import DeltaTable

#     target = DeltaTable.forName(source.sparkSession, fqn)  # type: ignore[attr-defined]

#     writer = target.alias("target").merge(source.alias("source"), F.expr(config.merge_condition))  # type: ignore[attr-defined]

#     if config.when_matched_update_all:
#         writer = writer.whenMatchedUpdateAll()
#     if config.when_matched_update:
#         writer = writer.whenMatchedUpdate(config.when_matched_update)
#     if config.when_not_matched_insert_all:
#         writer = writer.whenNotMatchedInsertAll()
#     if config.when_not_matched_by_source_delete:
#         writer = writer.whenNotMatchedBySourceDelete()
#     if config.merge_with_schema_evolution:
#         writer = writer.withSchemaEvolution()

#     writer.execute()  # type: ignore[attr-defined]


# def model(
#     query_function: Callable | None = None,
#     *,
#     name: str | None = None,
#     schema: str | None = None,
#     config: ModelConfig | dict | None = None,
# ) -> Callable:
#     """Declarative decorator for kelp model functions.

#     Wraps a function that returns a DataFrame. Kelp YAML metadata is used to
#     populate the :class:`~kelp.tables.model_context.ModelContext`; decorator
#     parameters override individual metadata fields.

#     When ``name`` contains dots (``schema.table`` or ``catalog.schema.table``)
#     the model is treated as *custom* and metadata is not consulted. The same
#     applies when the model name is not present in the loaded catalog.

#     The decorated function receives a :class:`~kelp.tables.model_context.ModelContext`
#     as its first argument.  Additional positional or keyword arguments are
#     forwarded transparently by the wrapper.

#     Materialization is controlled by the ``config`` parameter (or by calling
#     ``ctx.config(...)`` inside the function to set it dynamically).  When no
#     config is set, the DataFrame is returned without writing.

#     Args:
#         query_function: The wrapped function when used as a bare decorator (``@model``).
#         name: Model name or fully qualified name (``catalog.schema.table``,
#             ``schema.table``, or bare ``table``). Defaults to the function name.
#         catalog: Override catalog. Takes precedence over a catalog embedded in
#             ``name`` and over metadata.
#         schema: Spark DDL schema string (e.g. ``"id BIGINT, name STRING"``) that
#             overrides the schema resolved from kelp metadata.
#         config: Configuration controlling materialization — a
#             :class:`~kelp.models.model_config.ModelConfig` instance or an
#             equivalent ``dict``.  Fields are merged *on top of* any
#             metadata-backed config; only supplied keys override the base.
#             When ``None`` (default), the metadata config (if any) is used
#             unchanged.  Can be further patched at runtime via ``ctx.config(...)``.

#     Returns:
#         Callable that accepts
#         ``(spark=None, /, *args, full_refresh=False, **kwargs) -> DataFrame``.

#     Example::

#         from kelp import tables as kt


#         # Overwrite on every run (ModelConfig or plain dict both accepted)
#         @kt.model("prod.sales.dim_customer", config={"write_mode": "overwrite"})
#         def dim_customer(ctx: kt.ModelContext) -> DataFrame:
#             return ctx.spark.table(kt.ref("stg_customer"))


#         # Dynamic config override inside the function
#         @kt.model("prod.sales.fact_orders")
#         def fact_orders(ctx: kt.ModelContext) -> DataFrame:
#             if ctx.is_incremental():
#                 ctx.config(
#                     write_mode="merge",
#                     merge_condition="source.order_id = target.order_id",
#                 )
#             else:
#                 ctx.config(write_mode="overwrite")
#             return ctx.spark.table(kt.ref("stg_orders"))


#         df = fact_orders()  # uses active SparkSession
#     """

#     # Normalise dict → ModelConfig once at decoration time.
#     _config: ModelConfig | None = ModelConfig(**config) if isinstance(config, dict) else config  # type: ignore[arg-type]

#     def decorator(fn: Callable) -> Callable:
#         raw_name = name or fn.__name__
#         _name_catalog, _name_db_schema, table_name = _parse_model_name(raw_name)
#         _is_custom = _name_catalog is not None or _name_db_schema is not None

#         @functools.wraps(fn)
#         def wrapper(
#             spark: SparkSession | None = None,
#             /,
#             *args: Any,
#             full_refresh: bool = False,
#             **kwargs: Any,
#         ) -> DataFrame:
#             active_spark = spark or SparkSession.getActiveSession()
#             if active_spark is None:
#                 raise RuntimeError(
#                     "No active SparkSession. Pass one explicitly or start a session first."
#                 )

#             kelp_model = _resolve_model(
#                 table_name,
#                 is_custom=_is_custom,
#                 spark_schema=schema,
#                 config=_config,
#             )

#             ctx = ModelContext(
#                 spark=active_spark,
#                 this=kelp_model,
#                 full_refresh=full_refresh,
#                 target_exists=_table_exists(active_spark, kelp_model.fqn or table_name),
#             )
#             # Seed config: decorator config is merged on top of metadata config.
#             if _config is not None:
#                 base = kelp_model.config or ModelConfig()
#                 ctx.model_config = ModelConfig(
#                     **{**base.model_dump(), **_config.model_dump(exclude_unset=True)}
#                 )
#             else:
#                 ctx.model_config = kelp_model.config

#             result = fn(ctx, *args, **kwargs)

#             if not isinstance(result, DataFrame):
#                 raise TypeError(
#                     f"Model '{table_name}' must return a DataFrame, got {type(result).__name__}."
#                 )

#             active_config = ctx.model_config
#             if active_config is not None:
#                 # Merge requires an existing target; fall back to append on first run.
#                 if active_config.write_mode == "merge" and not ctx.target_exists:
#                     # active_config = ModelConfig(
#                     #     **{**active_config.model_dump(), "write_mode": "append"}
#                     # )
#                     if not kelp_model.schema:
#                         kelp_model.schema = result.schema.toDDL()  # type: ignore[attr-defined]
#                     logger.warning(kelp_model.build_ddl())
#                     active_spark.sql(kelp_model.build_ddl())  # type: ignore[attr-defined]

#                 _materialize(active_spark, result, kelp_model.fqn or table_name, active_config)

#             return result

#         wrapper.__kelp_model__ = True  # type: ignore[attr-defined]
#         wrapper.__kelp_model_name__ = table_name  # type: ignore[attr-defined]

#         return wrapper

#     if query_function is not None and callable(query_function):
#         return decorator(query_function)
#     return decorator
