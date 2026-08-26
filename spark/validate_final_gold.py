#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F

GOLD = "hdfs://namenode:9000/nyc-taxi/gold"
EXPECTED_SILVER_ROWS = 198_091_763

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

    trip_sum = (
        df.agg(F.sum("trip_count").alias("trip_sum"))
        .first()["trip_sum"]
    )

    matches_silver = (trip_sum == EXPECTED_SILVER_ROWS)
    months_ok = (month_count == 60)

    all_ok = all_ok and matches_silver and months_ok

    print(f"\n{name}")
    print("-" * 78)
    print(f"Gold rows                 : {rows:,}")
    print(f"Distinct year-months      : {month_count}")
    print(f"SUM(trip_count)           : {trip_sum:,}")
    print(f"Expected Silver rows      : {EXPECTED_SILVER_ROWS:,}")
    print(f"60-month coverage         : {'PASS' if months_ok else 'FAIL'}")
    print(f"Trip-count reconciliation : {'PASS' if matches_silver else 'FAIL'}")

print("\n" + "=" * 78)
print("OVERALL RESULT:", "PASS" if all_ok else "FAIL")
print("=" * 78)

spark.stop()
