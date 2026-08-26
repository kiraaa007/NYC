#!/usr/bin/env python3

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


TABLES = [
    "daily_summary",
    "hourly_demand",
    "pickup_zone_monthly",
    "payment_monthly",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export compact Gold tables for Streamlit."
    )

    parser.add_argument(
        "--gold-base",
        default="hdfs://namenode:9000/nyc-taxi/gold",
    )

    parser.add_argument(
        "--output-base",
        default="file:///opt/dashboard-data",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Export-Dashboard-Data")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("EXPORT GOLD TABLES FOR STREAMLIT")
    print("=" * 70)

    for table in TABLES:
        source = f"{args.gold_base}/{table}"
        target = f"{args.output_base}/{table}"

        print(f"\n{table}")
        print(f"  source: {source}")
        print(f"  target: {target}")

        df = spark.read.parquet(source)

        # Gold tables are compact. A small number of output files keeps
        # Streamlit startup fast while avoiding a giant single-file task.
        df.coalesce(4).write.mode("overwrite").parquet(target)

        print(f"  rows: {df.count():,}")

    print("\nDASHBOARD DATA EXPORT COMPLETE")
    spark.stop()


if __name__ == "__main__":
    main()
