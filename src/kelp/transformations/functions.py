"""Function application transformations for PySpark DataFrames.

Provides :func:`apply_func` — a composable transformation that applies
Unity Catalog functions to DataFrame columns and creates new derived columns.

Usage with ``DataFrame.transform()``::

    from kelp.transformations import apply_func

    # Apply a function to a single column (literal)
    df = input_df.transform(
        apply_func(
            func_name="normalize_email", new_column="email_normalized", parameters="input_email"
        )
    )

    # Apply a function with parameter mapping
    df = input_df.transform(
        apply_func(
            func_name="mask_id", new_column="customer_id_masked", parameters={"id": "customer_id"}
        )
    )

    # Chain multiple function applications
    df = input_df.transform(
        apply_func(
            func_name="normalize_customer_id",
            new_column="cust_id_normalized",
            parameters="customer_id",
        )
    ).transform(
        apply_func(
            func_name="format_full_name",
            new_column="full_name",
            parameters={"first_name": "first_name", "last_name": "last_name"},
        )
    )
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame, functions

logger = logging.getLogger(__name__)


def apply_func(
    func_name: str,
    new_column: str,
    parameters: dict[str, str] | str | None = None,
) -> Callable[[DataFrame], DataFrame]:
    """Return a transformation that applies a Unity Catalog function to a DataFrame.

    The returned callable is designed for :pyspark:`DataFrame.transform
    <pyspark.sql.DataFrame.transform>`.

    Args:
        func_name: Name of the Unity Catalog function (e.g., 'normalize_email').
            The function is resolved from the Kelp metadata catalog and invoked
            with the fully qualified name.
        new_column: Name of the new column to be created with the function result.
        parameters: Function parameters or literal column name.
            Can be:
            - A string (literal): single column name to pass to the function
              (e.g., ``"customer_id"`` passes the customer_id column).
            - A dict: mapping of function parameter names to DataFrame column names
              (e.g., ``{"id": "customer_id"}`` maps function param 'id' to column 'customer_id').
            - None: assumes function parameter name matches the column name.

    Returns:
        A ``Callable[[DataFrame], DataFrame]`` suitable for ``df.transform(…)``.

    Raises:
        KeyError: If the function name is not found in the catalog.
        ValueError: If parameters is invalid.

    Examples:
        Apply a function to a single column::

            from kelp.transformations import apply_func

            df = spark.read.table("customers")
            result = df.transform(
                apply_func(
                    func_name="normalize_email", new_column="email_normalized", parameters="email"
                )
            )

        Apply a function with multiple parameters::

            result = df.transform(
                apply_func(
                    func_name="format_full_name",
                    new_column="full_name",
                    parameters={"first_name": "first_name", "last_name": "last_name"},
                )
            )
    """
    from kelp.tables import func as get_func_fqn

    # Resolve the fully qualified function name from the catalog
    fqn = get_func_fqn(func_name)

    logger.debug(
        "Creating apply_func transformation: %s -> %s with parameters: %s",
        func_name,
        new_column,
        parameters,
    )

    def _apply(df: DataFrame) -> DataFrame:
        """Apply the function to the DataFrame."""
        # Handle different parameters types
        if isinstance(parameters, str):
            # Literal column name - pass directly to function
            func_expr = functions.expr(f"{fqn}({parameters})")
        elif isinstance(parameters, dict):
            # Parameter mapping - build expression with mapped column names
            col_names = [f"`{col}`" for col in parameters.values()]
            func_expr = functions.expr(f"{fqn}({', '.join(col_names)})")
        else:
            # No mapping - assume parameter matches column name
            func_expr = functions.expr(f"{fqn}(*)")

        result_df = df.withColumn(new_column, func_expr)

        logger.debug(
            "Applied function %s to create column '%s'",
            fqn,
            new_column,
        )

        return result_df

    return _apply
