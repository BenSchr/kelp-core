import functools
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from kelp.models.model_mat_config import ModelMaterializationConfig
from kelp.tables.materialization.base import table_exists
from kelp.tables.materialization.factory import _resolve_materialization_inputs, materialize
from kelp.tables.materialization.runner import _REGISTRY, ModelSpec


@dataclass
class MaterializedContext:
    """Execution context optionally injected into materialized functions.

    Attributes:
        spark: Active SparkSession.
        this: Fully qualified target table name (or provided name when unresolved).
        target_exists: Whether the target table exists before materialization.
        full_refresh: Whether a full refresh was requested by the caller.
    """

    spark: SparkSession
    this: str
    target_exists: bool
    full_refresh: bool = False

    def is_incremental(self) -> bool:
        """Return ``True`` when target exists and full refresh is not requested."""
        return self.target_exists and not self.full_refresh


def materialized(
    *,
    name: str | None = None,
    config: ModelMaterializationConfig | dict | None = None,
    depends_on: list[str] | None = None,
    full_refresh: bool = False,
    apply_vacuum: bool = True,
    vacuum_lite: bool = True,
    apply_optimize: bool = True,
    apply_quality_checks: bool = True,
) -> Callable[[Callable[..., DataFrame]], Callable[..., DataFrame]]:
    """Decorator that materializes the returned DataFrame.

    Model matching uses `name` when provided; otherwise the wrapped function
    name is used.

    Args:
        name: Optional kelp model/table name.
        config: Optional materialization override config.

    Returns:
        Decorated callable returning the same DataFrame after materialization.
    """
    cfg = ModelMaterializationConfig(**config) if isinstance(config, dict) else config
    depends_on = depends_on or []

    def decorator(fn: Callable[..., DataFrame]) -> Callable[..., DataFrame]:
        function_name = getattr(fn, "__name__", fn.__class__.__name__)
        target_name = name or function_name

        signature = inspect.signature(fn)
        parameters = list(signature.parameters.values())
        inject_ctx = bool(
            parameters
            and parameters[0].kind
            in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }
            and (
                parameters[0].name in {"ctx", "context"}
                or parameters[0].annotation is MaterializedContext
            )
        )

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            runtime_full_refresh = kwargs.pop("full_refresh", full_refresh)

            spark = SparkSession.getActiveSession()
            if spark is None:
                raise RuntimeError("No active SparkSession available for materialization.")

            resolved_inputs = _resolve_materialization_inputs(
                table_name=target_name,
                config=cfg,
            )
            resolved_target_name = resolved_inputs.target_name

            call_args = args
            if inject_ctx:
                context = MaterializedContext(
                    spark=spark,
                    this=resolved_target_name,
                    target_exists=table_exists(spark, resolved_target_name),
                    full_refresh=runtime_full_refresh,
                )
                call_args = (context, *args)

            result = fn(*call_args, **kwargs)
            if not isinstance(result, DataFrame):
                raise TypeError(
                    f"Materialized function '{target_name}' must return DataFrame, "
                    f"got {type(result).__name__}."
                )

            materialize(
                spark=spark,
                dataframe=result,
                name=target_name,
                config=cfg,
                full_refresh=runtime_full_refresh,
                apply_vacuum=apply_vacuum,
                vacuum_lite=vacuum_lite,
                apply_optimize=apply_optimize,
                apply_quality_checks=apply_quality_checks,
            )
            return result

        effective_name = target_name.split(".")[-1]
        _REGISTRY[effective_name] = ModelSpec(
            name=effective_name,
            fn=wrapper,
            depends_on=list(depends_on),
        )
        return wrapper

    return decorator
