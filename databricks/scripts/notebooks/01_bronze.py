# Databricks notebook: 01_bronze.py
# Run this in the Databricks UI after load_to_databricks.py completes.

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE CATALOG IF NOT EXISTS tlc")
spark.sql("CREATE SCHEMA IF NOT EXISTS tlc.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS tlc.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS tlc.gold")

raw_path = "dbfs:/FileStore/tlc/raw/"
df_raw = spark.read.parquet(raw_path)

df_raw.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("tlc.bronze.yellow_trips_raw")

print(f"Bronze loaded: {df_raw.count():,} rows")
