from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from onetl.connection import Hive
from onetl.db import DBWriter

spark = (
    SparkSession.builder.master("yarn")
    .appName("spark_check_yarn")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("spark.hive.metastore.uris", "thrift://nn:9083")
    .enableHiveSupport()
    .getOrCreate()
)

hive = Hive(spark=spark, cluster="x")
hive.check()

# 1. Создаём данные
data = [
    (1, "2024-01-15", "laptop",  1200.0, "electronics"),
    (2, "2024-01-20", "phone",    800.0, "electronics"),
    (3, "2024-02-10", "t-shirt",   30.0, "clothing"),
    (4, "2024-02-15", "jeans",     50.0, "clothing"),
    (5, "2024-03-10", "tablet",   600.0, "electronics"),
    (6, "2024-03-20", "jacket",   120.0, "clothing"),
]

schema = StructType([
    StructField("id",        IntegerType(), True),
    StructField("sale_date", StringType(),  True),
    StructField("product",   StringType(),  True),
    StructField("amount",    DoubleType(),  True),
    StructField("category",  StringType(),  True),
])

df = spark.createDataFrame(data, schema)

# 2. Трансформации
df_transformed = (
    df
    .withColumn("amount",    F.col("amount").cast("double"))
    .withColumn("sale_date", F.to_date(F.col("sale_date")))
    .withColumn("year",      F.year(F.col("sale_date")).cast("string"))
    .withColumn("month",     F.lpad(F.month(F.col("sale_date")).cast("string"), 2, "0"))
)

df_transformed.show()

# 3. Агрегация
df_agg = (
    df_transformed
    .groupBy("category", "year", "month")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("id").alias("total_sales"),
        F.avg("amount").alias("avg_amount")
    )
)

df_agg.show()

# 4. Сохраняем с партиционированием как таблицу Hive
writer = DBWriter(
    connection=hive,
    table="default.sales_aggregated",
    options=Hive.WriteOptions(
        mode="overwrite",
        partitionBy=["category", "year", "month"]
    )
)
writer.run(df_agg)
