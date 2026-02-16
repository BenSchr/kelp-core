from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.active()

source_catalog = spark.conf.get("parameters.source_catalog")
source_schema = spark.conf.get("parameters.source_schema")


@dp.temporary_view
def source_customers():
    """
    Reads the raw sample orders JSON data as a streaming source.
    """
    path = f"/Volumes/{source_catalog}/{source_schema}/landing_volume/customers/"

    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load(path)
