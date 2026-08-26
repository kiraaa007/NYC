#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F

SILVER = "hdfs://namenode:9000/nyc-taxi/silver/trips"
GOLD = "hdfs://namenode:9000/nyc-taxi/gold"

TABLES = [
    "daily_summary",
    "hourly_demand",
    "pickup_zone_monthly",
    "payment_monthly",
]

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Final-Gold-Validation")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 78)
print("FINAL GOLD VALIDATION")
print("=" * 78)

# Derive expectations from the CURRENT Silver output instead of hard-coding
# the original project row count. This keeps validation correct if a future
# transformation legitimately changes Silver and triggers a rebuild.
silver = spark.read.parquet(SILVER)
expected_silver_rows = silver.count()
expected_month_count = (
    silver.select(
        "pickup_year",
        F.col("pickup_month").cast("int").alias("pickup_month"),
    )
    .distinct()
    .count()
)

print(f"\nCurrent Silver rows       : {expected_silver_rows:,}")
print(f"Current Silver year-months: {expected_month_count}")

all_ok = True

for name in TABLES:
    df = spark.read.parquet(f"{GOLD}/{name}")
    rows = df.count()

    month_count = (
        df.select(
            "pickup_year",
            F.col("pickup_month").cast("int").alias("pickup_month"),
        )
        .distinct()
        .count()
    )

    trip_sum = df.agg(F.sum("trip_count").alias("trip_sum")).first()["trip_sum"]

    matches_silver = trip_sum == expected_silver_rows
    months_ok = month_count == expected_month_count
    all_ok = all_ok and matches_silver and months_ok

    print(f"\n{name}")
    print("-" * 78)
    print(f"Gold rows                 : {rows:,}")
    print(f"Distinct year-months      : {month_count}")
    print(f"SUM(trip_count)           : {trip_sum:,}")
    print(f"Expected Silver rows      : {expected_silver_rows:,}")
    print(f"Expected Silver months    : {expected_month_count}")
    print(f"Month coverage            : {'PASS' if months_ok else 'FAIL'}")
    print(f"Trip-count reconciliation : {'PASS' if matches_silver else 'FAIL'}")

print("\n" + "=" * 78)
print("OVERALL RESULT:", "PASS" if all_ok else "FAIL")
print("=" * 78)

spark.stop()
