from pyspark.sql import SparkSession

from kelp import pipelines as kp
from kelp.transformations import apply_schema

spark = SparkSession.active()
# kp.init("../../kelp_metadata/kelp_project.yml")


@kp.table
def silver_orders_cleaned():
    return spark.readStream.table(kp.ref("bronze_orders")).transform(
        apply_schema("silver_orders_cleaned")
    )
