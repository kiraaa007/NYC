#!/usr/bin/env python3
"""
NYC Yellow Taxi - Silver -> Gold analytics tables for Streamlit.

Input
-----
hdfs://namenode:9000/nyc-taxi/silver/trips

Outputs
-------
<gold-base>/daily_summary
<gold-base>/hourly_demand
<gold-base>/pickup_zone_monthly
<gold-base>/payment_monthly

These are intentionally compact serving tables for Streamlit.
ML feature engineering should be built separately from Silver after the
prediction target is selected.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SILVER_PATH = os.environ.get(
    "SILVER_PATH",
    "hdfs://namenode:9000/nyc-taxi/silver/trips",
)

GOLD_BASE_PATH = os.environ.get(
    "GOLD_BASE_PATH",
    "hdfs://namenode:9000/nyc-taxi/gold",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("nyc_taxi_gold")


def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("NYC-Yellow-Taxi-Silver-to-Gold")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(
        os.environ.get("SPARK_LOG_LEVEL", "WARN")
    )
    return spark


def read_silver(
    spark: SparkSession,
    year: int | None,
    month: int | None,
) -> DataFrame:
    df = spark.read.parquet(SILVER_PATH)

    if year is not None:
        df = df.filter(F.col("pickup_year") == F.lit(year))

    if month is not None:
        # pickup_month may be inferred by Spark as int from partition names.
        df = df.filter(
            F.col("pickup_month").cast("int") == F.lit(month)
        )

    return df


def add_common_metrics(grouped):
    return (
        grouped
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.sum(F.coalesce(F.col("total_amount"), F.lit(0.0)))
                .alias("total_revenue"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("trip_duration_minutes").alias(
                "avg_trip_duration_minutes"
            ),
            F.avg("passenger_count").alias("avg_passenger_count"),
        )
    )


def build_daily_summary(df: DataFrame) -> DataFrame:
    return (
        add_common_metrics(
            df.groupBy(
                "pickup_date",
                "pickup_year",
                "pickup_month",
                "pickup_day_of_week",
            )
        )
        .withColumn(
            "day_of_week_number",
            F.dayofweek("pickup_date"),
        )
        .orderBy("pickup_date")
    )


def build_hourly_demand(df: DataFrame) -> DataFrame:
    return (
        add_common_metrics(
            df.groupBy(
                "pickup_year",
                "pickup_month",
                "pickup_day_of_week",
                "pickup_hour",
            )
        )
        .withColumn(
            "day_of_week_number",
            F.when(F.col("pickup_day_of_week") == "Sunday", 1)
             .when(F.col("pickup_day_of_week") == "Monday", 2)
             .when(F.col("pickup_day_of_week") == "Tuesday", 3)
             .when(F.col("pickup_day_of_week") == "Wednesday", 4)
             .when(F.col("pickup_day_of_week") == "Thursday", 5)
             .when(F.col("pickup_day_of_week") == "Friday", 6)
             .when(F.col("pickup_day_of_week") == "Saturday", 7)
        )
        .orderBy(
            "pickup_year",
            "pickup_month",
            "day_of_week_number",
            "pickup_hour",
        )
    )


def build_pickup_zone_monthly(df: DataFrame) -> DataFrame:
    return (
        add_common_metrics(
            df.groupBy(
                "pickup_year",
                "pickup_month",
                "pickup_location_id",
                "pickup_zone",
                "pickup_borough",
                "pickup_service_zone",
            )
        )
        .orderBy(
            "pickup_year",
            "pickup_month",
            F.desc("trip_count"),
        )
    )


def build_payment_monthly(df: DataFrame) -> DataFrame:
    return (
        add_common_metrics(
            df.groupBy(
                "pickup_year",
                "pickup_month",
                "payment_type",
            )
        )
        .orderBy(
            "pickup_year",
            "pickup_month",
            "payment_type",
        )
    )


def write_table(
    df: DataFrame,
    path: str,
    mode: str,
    partition_cols: list[str] | None = None,
) -> None:
    log.info("Writing Gold table -> %s", path)

    writer = (
        df.write
        .mode(mode)
        .option("compression", "snappy")
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build Gold analytics tables from Silver."
    )

    parser.add_argument(
        "--output-base",
        default=GOLD_BASE_PATH,
        help=f"Gold base HDFS path. Default: {GOLD_BASE_PATH}",
    )

    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Optional test/filter year, e.g. 2025.",
    )

    parser.add_argument(
        "--month",
        type=int,
        default=None,
        help="Optional test/filter month 1-12.",
    )

    parser.add_argument(
        "--mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Spark write mode. Default: overwrite.",
    )

    args = parser.parse_args()

    if args.month is not None and not 1 <= args.month <= 12:
        log.error("--month must be between 1 and 12.")
        return 2

    if args.month is not None and args.year is None:
        log.error("--month requires --year.")
        return 2

    spark = None

    try:
        log.info("=" * 72)
        log.info("NYC YELLOW TAXI - SILVER -> GOLD")
        log.info("Silver input : %s", SILVER_PATH)
        log.info("Gold base    : %s", args.output_base)
        log.info("Year filter  : %s", args.year)
        log.info("Month filter : %s", args.month)
        log.info("=" * 72)

        spark = create_spark()
        silver = read_silver(
            spark=spark,
            year=args.year,
            month=args.month,
        )

        tables = [
            (
                "daily_summary",
                build_daily_summary(silver),
                ["pickup_year", "pickup_month"],
            ),
            (
                "hourly_demand",
                build_hourly_demand(silver),
                ["pickup_year", "pickup_month"],
            ),
            (
                "pickup_zone_monthly",
                build_pickup_zone_monthly(silver),
                ["pickup_year", "pickup_month"],
            ),
            (
                "payment_monthly",
                build_payment_monthly(silver),
                ["pickup_year", "pickup_month"],
            ),
        ]

        for name, table_df, partitions in tables:
            write_table(
                df=table_df,
                path=f"{args.output_base.rstrip('/')}/{name}",
                mode=args.mode,
                partition_cols=partitions,
            )

        log.info("=" * 72)
        log.info("SILVER -> GOLD COMPLETE")
        log.info("Gold base: %s", args.output_base)
        log.info("=" * 72)

        return 0

    except Exception as exc:
        log.exception("Silver -> Gold failed: %s", exc)
        return 1

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
