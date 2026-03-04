from pyspark import pipelines as dp
from pyspark.sql import SparkSession

import kelp.pipelines as kp

spark = SparkSession.active()


@dp.temporary_view
def source_orders():
    """
    Reads the raw sample orders data from the landing volume.
    """
    path = kp.source("landing_volume_orders")
    options = kp.source_options("landing_volume_orders")

    return spark.readStream.format("cloudFiles").options(**options).load(path)
