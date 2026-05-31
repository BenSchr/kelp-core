from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from kelp.models.model_mat_config import ModelMaterializationConfig
from kelp.tables.materialization.factory import materialize


def materialized(
    *,
    name: str | None = None,
    config: ModelMaterializationConfig | dict | None = None,
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

    def decorator(fn: Callable[..., DataFrame]) -> Callable[..., DataFrame]:
        function_name = getattr(fn, "__name__", fn.__class__.__name__)
        target_name = name or function_name

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            result = fn(*args, **kwargs)
            if not isinstance(result, DataFrame):
                raise TypeError(
                    f"Materialized function '{target_name}' must return DataFrame, "
                    f"got {type(result).__name__}."
                )

            spark = result.sparkSession or SparkSession.getActiveSession()
            if spark is None:
                raise RuntimeError("No active SparkSession available for materialization.")

            materialize(
                spark=spark,
                dataframe=result,
                table_name=target_name,
                config=cfg,
            )
            return result

        return wrapper

    return decorator
