from pyspark import pipelines as dp
from pyspark.sql.functions import *

valid_conditions = (
    (col("order_id").isNotNull()) &
    (col("user_id").isNotNull()) &
    (col("total_amount") > 0) &
    (col("status").isin(["completed", "failed", "created"]))
)


@dp.expect("order_id_not_null", "order_id IS NOT NULL")
@dp.expect("user_id_not_null", "user_id IS NOT NULL")
@dp.expect("positive_amount", "total_amount > 0")
@dp.expect("valid_status", "status IN ('completed', 'failed', 'created')")

@dp.table(
    name="dev_ecommerce.silver.orders_silver"
)
def orders_silver():
    df = (
        spark.readStream.table(
            "dev_ecommerce.bronze.orders_bronze"
        )
        .withColumn(
            "total_amount",
            col("total_amount").cast("double")
        )
        .withColumn(
            "status",
            lower(col("status"))
        )
    )

    return (
        df
        .filter(valid_conditions)
        .withColumn("order_date", to_date(col("order_timestamp")))
        .withColumn("order_time", date_format("order_timestamp", "HH:mm:ss"))
        .select("order_id", "order_date", "order_time", "status", "total_amount", "user_id")
    )


@dp.table(
    name="dev_ecommerce.silver.orders_quarantine_silver"
)
def orders_quarantine_silver():
    df = (
        spark.readStream.table(
            "dev_ecommerce.bronze.orders_bronze"
        )
        .withColumn(
            "total_amount",
            col("total_amount").cast("double")
        )
        .withColumn(
            "status",
            lower(col("status"))
        )
    )

    return (
        df
        .filter(~valid_conditions | col("_rescued_data").isNotNull())
    )
