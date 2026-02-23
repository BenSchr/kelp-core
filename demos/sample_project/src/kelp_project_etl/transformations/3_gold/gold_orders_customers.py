from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from kelp import pipelines as kp

spark = SparkSession.active()
# kp.init("../../kelp_metadata/kelp_project.yml")


@dp.materialized_view(**kp.params("gold_orders_customers"))
def gold_orders_customers():
    """Gold materialized view: enriched orders joined with customers."""
    orders = spark.read.table(kp.ref("silver_orders_cleaned"))
    customers = spark.read.table(kp.ref("silver_customers_cleaned"))

    from pyspark.sql.functions import col

    df = orders.join(customers, orders.user_id == customers.user_id, how="left")

    df = df.withColumn("total_price", col("price") * col("quantity")).select(
        col("order_id"),
        col("order_state"),
        col("product"),
        col("quantity"),
        col("price"),
        col("total_price"),
        col("store"),
        customers["user_id"],
        customers["country"],
    )

    return df
