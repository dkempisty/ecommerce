from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.materialized_view(
    name = "dev_ecommerce.gold.completed_sales"
)
def completed_sales():
    orders = spark.read.table("dev_ecommerce.silver.orders_silver")
    users =  spark.read.table("dev_ecommerce.silver.users_silver")

    return(
        orders
        .join(users, orders.user_id == users.user_id, "left")
        .groupby(
            orders.order_id,
            orders.order_date,
            users.user_id,
            users.country
        )
        .agg(
            sum("total_amount").alias("total_amount")
        )
    )
