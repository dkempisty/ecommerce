from pyspark import pipelines as dp
from pyspark.sql.functions import *

valid_conditions = (
    (col("order_id").isNotNull()) &
    (col("product_id").isNotNull()) &
    (col("price") > 0)
)

@dp.expect("order_id_is_not_null", "order_id IS NOT NULL")
@dp.expect("product_id_isn_not_null", "product_id IS NOT NULL")
@dp.expect("positive_price", "price > 0")

@dp.table(
    name="dev_ecommerce.silver.orders_items_silver"
)
def orders_items_silver():
    df = spark.readStream.table("dev_ecommerce.bronze.orders_items_bronze")

    return(
        df
        .filter(valid_conditions)
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("price", col("price").cast("double"))
        .select("order_id", "product_id", "quantity", "price")
    )

@dp.table(
    name="dev_ecommerce.silver.orders_items_quarantine_silver"
)
def orders_items_quarantine_silver():
    df = spark.readStream.table("dev_ecommerce.bronze.orders_items_bronze")

    return(
        df
        .filter(~valid_conditions | col("_rescued_data").isNotNull())
    )
