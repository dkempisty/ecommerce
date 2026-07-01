from pyspark import pipelines as dp
from pyspark.sql.functions import *

valid_conditions = (
    col("user_id").isNotNull()
)

@dp.expect("user_id_is_not_null", "user_id IS NOT NULL")

@dp.table(
    name="dev_ecommerce.silver.users_silver"
)
def users_silver():
    df = spark.readStream.table("dev_ecommerce.bronze.users_bronze")

    return(
        df
        .filter(valid_conditions)
        .withColumn("country", upper(col("country")))
        .select("user_id", "country", "signup_date")    
    )


@dp.table(
    name="dev_ecommerce.silver.users_quarantine_silver"
)
def products_quarantine_silver():
    df = spark.readStream.table("dev_ecommerce.bronze.users_bronze")

    return(
        df
        .filter(~valid_conditions | col("_rescued_data").isNotNull())
    )