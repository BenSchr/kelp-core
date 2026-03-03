from pyspark.sql import DataFrame, SparkSession

from kelp import pipelines as kp
from kelp.transformations import apply_func

spark = SparkSession.active()
# kp.init("../../kelp_metadata/kelp_project.yml")


@kp.table(exclude_params=["schema"])
def silver_customer_profile() -> DataFrame:
    """Create an enriched customer profile demonstrating function application.

    Uses Kelp functions to normalize customer IDs, format names, and
    standardize country values via the apply_func transformation.
    """
    customers = spark.readStream.table(kp.ref("silver_customers_cleaned"))

    return (
        customers.transform(
            apply_func(
                func_name="normalize_customer_id",
                new_column="customer_id",
                parameters="user_id",
            )
        )
        .transform(
            apply_func(
                func_name="format_full_name",
                new_column="full_name",
                parameters={"first_name": "first_name", "last_name": "last_name"},
            )
        )
        .transform(
            apply_func(
                func_name="standardize_country",
                new_column="country_standardized",
                parameters="country",
            )
        )
        .select(
            "customer_id",
            "full_name",
            "country_standardized",
            "user_id",  # Keep source for reference
        )
    )
