#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F

BATCH_BRONZE = "hdfs://namenode:9000/nyc-taxi/bronze/batch/year=2025/month=11"
BATCH_SILVER = "hdfs://namenode:9000/nyc-taxi/silver_test/batch_2025_11"

STREAM_BRONZE = "hdfs://namenode:9000/nyc-taxi/bronze/streaming/year=2025/month=12"
STREAM_SILVER = "hdfs://namenode:9000/nyc-taxi/silver_test/streaming_2025_12"

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Silver-Validation")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def validate(label, bronze_path, silver_path):
    print("\n" + "=" * 72)
    print(label)
    print("=" * 72)

    bronze = spark.read.parquet(bronze_path)
    silver = spark.read.parquet(silver_path)

    bronze_count = bronze.count()
    silver_count = silver.count()
    removed = bronze_count - silver_count
    removed_pct = (removed / bronze_count * 100.0) if bronze_count else 0.0

    print(f"Bronze rows : {bronze_count:,}")
    print(f"Silver rows : {silver_count:,}")
    print(f"Removed     : {removed:,} ({removed_pct:.4f}%)")

    print("\nSilver partition values:")
    (
        silver
        .groupBy("pickup_year", "pickup_month")
        .count()
        .orderBy("pickup_year", "pickup_month")
        .show(20, truncate=False)
    )

    print("\nTaxi-zone enrichment null counts:")
    silver.select(
        F.sum(F.col("pickup_zone").isNull().cast("int")).alias("pickup_zone_nulls"),
        F.sum(F.col("dropoff_zone").isNull().cast("int")).alias("dropoff_zone_nulls"),
        F.sum(F.col("pickup_borough").isNull().cast("int")).alias("pickup_borough_nulls"),
        F.sum(F.col("dropoff_borough").isNull().cast("int")).alias("dropoff_borough_nulls"),
    ).show(truncate=False)

    print("\nIngestion source:")
    silver.groupBy("ingestion_source").count().show(truncate=False)

    print("\nSample Silver rows:")
    silver.select(
        "pickup_datetime",
        "dropoff_datetime",
        "trip_distance",
        "pickup_location_id",
        "pickup_zone",
        "pickup_borough",
        "dropoff_location_id",
        "dropoff_zone",
        "dropoff_borough",
        "total_amount",
        "trip_duration_minutes",
        "ingestion_source",
    ).show(5, truncate=False)


validate(
    "BATCH TEST: 2025-11",
    BATCH_BRONZE,
    BATCH_SILVER,
)

validate(
    "STREAMING TEST: 2025-12",
    STREAM_BRONZE,
    STREAM_SILVER,
)

print("\n" + "=" * 72)
print("VALIDATION COMPLETE")
print("=" * 72)

spark.stop()
