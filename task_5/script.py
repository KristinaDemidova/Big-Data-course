from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)

HDFS_INPUT_PATH  = "/user/hive/warehouse/sales_partitioned"
HIVE_METASTORE   = "thrift://nn:9083"
HIVE_WAREHOUSE   = "/user/hive/warehouse"
OUTPUT_DB        = "prefect_demo"

@task(cache_policy=NO_CACHE)
def start_spark():
    spark = (
        SparkSession.builder
        .master("yarn")
        .appName("sales_partitioned_etl")
        .config("spark.sql.warehouse.dir", HIVE_WAREHOUSE)
        .config("hive.metastore.uris", HIVE_METASTORE)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

@task(cache_policy=NO_CACHE)
def extract(spark):
    schema = StructType([
        StructField("id",        IntegerType(), True),
        StructField("sale_date", StringType(),  True),
        StructField("product",   StringType(),  True),
        StructField("amount",    DoubleType(),  True),
    ])

    df = (
        spark.read
        .option("basePath", HDFS_INPUT_PATH)
        .schema(schema)
        .parquet(f"{HDFS_INPUT_PATH}/category=*/year=*/month=*/")
    )
    print(f"[extract] Прочитано строк: {df.count()}")
    print(f"[extract] Колонки: {df.columns}")
    df.show()
    return df

@task(cache_policy=NO_CACHE)
def transform(df):

    df = (
        df
        .withColumn(
            "sale_date",
            F.to_date(F.col("sale_date"), "yyyy-MM-dd")
        )
        .withColumn(
            "product",
            F.initcap(F.col("product"))
        )
        .withColumn(
            "price_category",
            F.when(F.col("amount") >= 1000, "premium")
             .when(F.col("amount") >= 300,  "mid")
             .otherwise("budget")
        )

        .withColumn("amount", F.round(F.col("amount"), 2))
    )

    print("[transform] Трансформации применены:")
    df.show()
    return df

@task(cache_policy=NO_CACHE)
def load(spark, df):

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {OUTPUT_DB}")
    df.write.mode("overwrite").partitionBy("category", "price_category").saveAsTable(f"{OUTPUT_DB}.sales_transformed")
    print(f"[load]  {OUTPUT_DB}.sales_transformed записана")

    agg_category = (
        df.groupBy("category")
        .agg(
            F.count("*").alias("total_sales"),
            F.round(F.sum("amount"),   2).alias("total_revenue"),
            F.round(F.avg("amount"),   2)        .alias("avg_amount"),
            F.min("amount")                      .alias("min_amount"),
            F.max("amount")                      .alias("max_amount"),
        )
        .orderBy("category")
    )
    (
        agg_category.write
        .mode("overwrite")
        .saveAsTable(f"{OUTPUT_DB}.sales_by_category")
    )
    print(f"[load] ✔ {OUTPUT_DB}.sales_by_category записана")
    agg_category.show()

    agg_month = (
        df.groupBy("year", "month", "category")
        .agg(
            F.count("*")                        .alias("total_sales"),
            F.round(F.sum("amount"),   2)        .alias("total_revenue"),
            F.round(F.avg("amount"),   2)        .alias("avg_amount"),
        )
        .orderBy("year", "month", "category")
    )
    (
        agg_month.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .saveAsTable(f"{OUTPUT_DB}.sales_by_month")
    )
    print(f"[load] ✔ {OUTPUT_DB}.sales_by_month записана")
    agg_month.show()

@task(cache_policy=NO_CACHE)
def stop_spark(spark):
    spark.stop()
    print("✔ Spark-сессия остановлена")

@flow(name="sales_partitioned_etl")
def process_data():
    spark = start_spark()
    df    = extract(spark)
    df    = transform(df)
    load(spark, df)
    stop_spark(spark)


if __name__ == "__main__":
    process_data()