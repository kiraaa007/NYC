#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F

SILVER = "hdfs://namenode:9000/nyc-taxi/silver/trips"

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Final-Silver-Validation")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(SILVER)

print("\n" + "=" * 72)
print("FINAL SILVER VALIDATION")
print("=" * 72)

total = df.count()
print(f"\nTotal Silver rows: {total:,}")

print("\nRows by ingestion source:")
df.groupBy("ingestion_source").count().orderBy("ingestion_source").show(
    truncate=False
)

print("\nRows by year:")
df.groupBy("pickup_year").count().orderBy("pickup_year").show(
    10, truncate=False
)

print("\nMonth partition count:")
month_count = (
    df.select("pickup_year", "pickup_month")
      .distinct()
      .count()
)
print(f"Distinct year-month partitions: {month_count}")

print("\nTaxi-zone enrichment null counts:")
df.select(
    F.sum(F.col("pickup_zone").isNull().cast("int")).alias("pickup_zone_nulls"),
    F.sum(F.col("dropoff_zone").isNull().cast("int")).alias("dropoff_zone_nulls"),
    F.sum(F.col("pickup_borough").isNull().cast("int")).alias("pickup_borough_nulls"),
    F.sum(F.col("dropoff_borough").isNull().cast("int")).alias("dropoff_borough_nulls"),
).show(truncate=False)

print("\nStreaming partition check:")
(
    df.filter(F.col("ingestion_source") == "streaming")
      .groupBy("pickup_year", "pickup_month")
      .count()
      .orderBy("pickup_year", "pickup_month")
      .show(20, truncate=False)
)

print("\n" + "=" * 72)
print("FINAL SILVER VALIDATION COMPLETE")
print("=" * 72)

spark.stop()
