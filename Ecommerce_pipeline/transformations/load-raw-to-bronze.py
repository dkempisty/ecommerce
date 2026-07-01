from pyspark import pipelines as dp

@dp.table(
    name="orders_bronze"
)
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/dev_ecommerce/bronze/ecommerce_data/orders/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load("/Volumes/dev_ecommerce/bronze/ecommerce_data/orders/raw/")
    )

@dp.table(
    name="orders_items_bronze"
)
def orders_items_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/dev_ecommerce/bronze/ecommerce_data/orders_items/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load("/Volumes/dev_ecommerce/bronze/ecommerce_data/orders_items/raw/")
    )

@dp.table(
    name="products_bronze"
)
def products_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/dev_ecommerce/bronze/ecommerce_data/products/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load("/Volumes/dev_ecommerce/bronze/ecommerce_data/products/raw/")
    )

@dp.table(
    name="users_bronze"
)
def users_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/dev_ecommerce/bronze/ecommerce_data/users/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load("/Volumes/dev_ecommerce/bronze/ecommerce_data/users/raw/")
    )