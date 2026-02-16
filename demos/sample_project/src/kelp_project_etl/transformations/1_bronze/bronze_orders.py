from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from kelp import pipelines as kp

spark = SparkSession.active()
kp.init("../../kelp_metadata/kelp_project.yml")


@kp.streaming_table()  # exclude_params=["schema"]
def bronze_orders():
    """
    Bronze transformation for orders. Reads from the source_orders temporary view and writes to the bronze layer.
    """
    return spark.readStream.table("source_orders")
