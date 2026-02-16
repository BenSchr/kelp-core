from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from kelp import pipelines as kp

spark = SparkSession.active()
kp.init("../../kelp_metadata/kelp_project.yml")


@kp.streaming_table(exclude_params=["schema"])
def silver_customers_cleaned():
    """
    Bronze transformation for customers. Reads from the source_customers temporary view and writes to the bronze layer.
    """
    return spark.readStream.table(kp.ref("bronze_customers")).drop("_rescued_data")
