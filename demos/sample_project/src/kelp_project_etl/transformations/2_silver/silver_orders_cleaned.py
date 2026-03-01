from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from kelp import pipelines as kp
from kelp.transformations import apply_schema

spark = SparkSession.active()
# kp.init("../../kelp_metadata/kelp_project.yml")


@kp.table()
def silver_orders_cleaned():
    return (
        spark.readStream.table(kp.ref("bronze_orders"))
        .drop("_rescued_data")
        .drop("_errors")
        .drop("_warnings")
    )
