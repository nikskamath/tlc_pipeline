# Databricks notebook: 02_silver.py

from pyspark.sql.functions import col, to_timestamp, unix_timestamp, coalesce, lit

spark = spark  # noqa — provided by Databricks runtime

df_bronze = spark.table("tlc.bronze.yellow_trips_raw")
df_zones = spark.read.csv("dbfs:/FileStore/tlc/taxi_zone_lookup.csv", header=True)

df_silver = df_bronze \
    .filter(col("fare_amount") > 0) \
    .filter(col("trip_distance") > 0) \
    .filter(col("passenger_count").between(1, 6)) \
    .withColumn("pickup_ts", to_timestamp("tpep_pickup_datetime")) \
    .withColumn("dropoff_ts", to_timestamp("tpep_dropoff_datetime")) \
    .withColumn(
        "trip_duration_mins",
        (unix_timestamp("dropoff_ts") - unix_timestamp("pickup_ts")) / 60
    ) \
    .filter(col("trip_duration_mins").between(1, 180)) \
    .withColumn("airport_fee", coalesce(col("airport_fee"), lit(0.0))) \
    .withColumn("cbd_congestion_fee", coalesce(col("cbd_congestion_fee"), lit(0.0))) \
    .withColumn("congestion_surcharge", coalesce(col("congestion_surcharge"), lit(0.0)))

df_zones_pu = df_zones \
    .withColumnRenamed("LocationID", "PULocationID") \
    .withColumnRenamed("Borough", "pickup_borough") \
    .withColumnRenamed("Zone", "pickup_zone")

df_silver = df_silver.join(df_zones_pu, on="PULocationID", how="left")

from pyspark.sql.functions import current_timestamp
df_silver = df_silver.withColumn("_loaded_at", current_timestamp())

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("tlc.silver.yellow_trips_clean")

print(f"Silver loaded: {df_silver.count():,} rows")
