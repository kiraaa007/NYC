#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F

BASE = "hdfs://namenode:9000/nyc-taxi/gold_test"

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Gold-Test-Validation")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

tables = [
    "daily_summary",
    "hourly_demand",
    "pickup_zone_monthly",
    "payment_monthly",
]

print("\n" + "=" * 72)
print("GOLD TEST VALIDATION")
print("=" * 72)

for name in tables:
    path = f"{BASE}/{name}"
    df = spark.read.parquet(path)

    print("\n" + "-" * 72)
    print(name)
    print("-" * 72)

    print(f"Rows: {df.count():,}")

    if "pickup_year" in df.columns and "pickup_month" in df.columns:
        print("Partitions represented:")
        (
            df.select(
                "pickup_year",
                F.col("pickup_month").cast("int").alias("pickup_month"),
            )
            .distinct()
            .orderBy("pickup_year", "pickup_month")
            .show(20, truncate=False)
        )

    print("Sample:")
    df.show(10, truncate=False)

print("\n" + "=" * 72)
print("GOLD TEST VALIDATION COMPLETE")
print("=" * 72)

spark.stop()
