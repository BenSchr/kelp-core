import functools
import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, overload

from pyspark.sql import DataFrame, SparkSession

from kelp.models.model_mat_config import (
    MaterializationConfig,
    MaterializationOptions,
    parse_materialization_config,
)
from kelp.tables.materialization.base import table_exists
from kelp.tables.materialization.orchestrator import materialize_resolved
from kelp.tables.materialization.resolve import (
    FullRefreshStrategy,
    resolve_materialization_inputs,
)
from kelp.tables.materialization.runner import REGISTRY, ModelSpec


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


@overload
def materialized(func: Callable[..., DataFrame]) -> Callable[..., DataFrame]: ...


@overload
def materialized(
    *,
    name: str | None = None,
    config: MaterializationConfig | dict | None = None,
    options: MaterializationOptions | dict | None = None,
    depends_on: list[str] | None = None,
    full_refresh: bool = False,
    full_refresh_strategy: FullRefreshStrategy = "drop",
) -> Callable[[Callable[..., DataFrame]], Callable[..., DataFrame]]: ...


def materialized(
    func: Callable[..., DataFrame] | None = None,
    *,
    name: str | None = None,
    config: MaterializationConfig | dict | None = None,
    options: MaterializationOptions | dict | None = None,
    depends_on: list[str] | None = None,
    full_refresh: bool = False,
    full_refresh_strategy: FullRefreshStrategy = "drop",
) -> Callable[..., DataFrame] | Callable[[Callable[..., DataFrame]], Callable[..., DataFrame]]:
    """Decorator that materializes the returned DataFrame.

    Usable bare (``@materialized``) or called (``@materialized(name=...)``).
    Model matching uses `name` when provided; otherwise the wrapped function
    name is used. An unqualified name must match a kelp model; pass a qualified
    table name to materialize without metadata.

    The wrapper accepts ``full_refresh`` and ``spark`` keywords at call time, which
    override the decorator's value and the active session and are not passed on to
    the wrapped function.

    Args:
        func: The decorated function when used bare.
        name: Optional kelp model name, or a qualified table name.
        config: Optional materialization config, replacing the model's config.
        options: Overrides for the steps run around the write — quality checks,
            catalog sync, OPTIMIZE and VACUUM. Unset switches fall back to the
            project's ``materialization_options``.
        depends_on: Model names this model must run after, for the runner.
        full_refresh: Whether to rebuild the target from scratch before writing.
        full_refresh_strategy: How a full refresh resets the target — ``drop`` to
            drop and recreate it, ``replace`` to keep the table (and its grants and
            history) in place.

    Returns:
        The decorated callable, or the decorator when called with options.
    """
    cfg = parse_materialization_config(config)
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

            # A caller may hand the session over: PySpark's active session is
            # thread-local, so a model run by the Runner in a worker thread cannot
            # find it itself.
            spark = kwargs.pop("spark", None) or SparkSession.getActiveSession()
            if spark is None:
                raise RuntimeError("No active SparkSession available for materialization.")

            # Resolved once here and handed to the orchestrator, so metadata is not
            # looked up twice for the same run.
            resolved = resolve_materialization_inputs(table_name=target_name, config=cfg)

            call_args = args
            if inject_ctx:
                context = MaterializedContext(
                    spark=spark,
                    this=resolved.target_name,
                    target_exists=table_exists(spark, resolved.target_name),
                    full_refresh=runtime_full_refresh,
                )
                call_args = (context, *args)

            result = fn(*call_args, **kwargs)
            if not isinstance(result, DataFrame):
                raise TypeError(
                    f"Materialized function '{target_name}' must return DataFrame, "
                    f"got {type(result).__name__}."
                )

            return materialize_resolved(
                dataframe=result,
                resolved=resolved,
                options=options,
                full_refresh=runtime_full_refresh,
                full_refresh_strategy=full_refresh_strategy,
                spark=spark,
            )

        REGISTRY.register(
            ModelSpec(
                name=target_name,
                fn=wrapper,
                depends_on=list(depends_on),
            )
        )
        return wrapper

    if func is not None and callable(func):
        return decorator(func)

    return decorator
