from pyspark import pipelines as dp
from pyspark.sql.functions import *

valid_conditions = (
    (col("product_id").isNotNull()) &
    (col("price") > 0)
)

@dp.expect("product_id_is_not_null", "product_id IS NOT NULL")
@dp.expect("price is positive", "price > 0")

@dp.table(
    name="dev_ecommerce.silver.products_silver"
)
def products_silver():
    df = spark.readStream.table("dev_ecommerce.bronze.products_bronze")

    return(
        df
        .filter(valid_conditions)
        .withColumn("category", lower(col("category")))
        .withColumn("brand", lower(col("brand")))
        .withColumn("price", col("price").cast("double"))
        .select("product_id", "category", "price", "brand")    
    )


@dp.table(
    name="dev_ecommerce.silver.products_quarantine_silver"
)
def products_quarantine_silver():
    df = spark.readStream.table("dev_ecommerce.bronze.products_bronze")

    return(
        df
        .filter(~valid_conditions | col("_rescued_data").isNotNull())
    )