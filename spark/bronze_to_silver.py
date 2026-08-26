#!/usr/bin/env python3
"""
NYC TLC Yellow Taxi - Bronze -> Silver Spark job.

Inputs
------
Batch Bronze:
    hdfs://namenode:9000/nyc-taxi/bronze/batch/year=YYYY/month=MM/

Streaming Bronze:
    hdfs://namenode:9000/nyc-taxi/bronze/streaming/year=YYYY/month=MM/
    - contains raw_json + Kafka metadata

Reference:
    hdfs://namenode:9000/nyc-taxi/reference/taxi_zone_lookup.csv

Output
------
Silver:
    hdfs://namenode:9000/nyc-taxi/silver/trips/
        pickup_year=YYYY/
            pickup_month=MM/
                part-....parquet

Design
------
1. Read each batch month independently so schema drift across TLC files
   does not break one global Parquet schema merge.
2. Normalize batch and streaming records to one canonical schema.
3. Union both ingestion paths.
4. Apply conservative structural cleaning.
5. Enrich pickup and dropoff locations using the Taxi Zone Lookup.
6. Add useful time/duration columns.
7. Write partitioned Silver Parquet.

No Gold aggregations or ML features are produced here.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from functools import reduce
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BATCH_BRONZE_PATH = os.environ.get(
    "BATCH_BRONZE_PATH",
    "hdfs://namenode:9000/nyc-taxi/bronze/batch",
)

STREAM_BRONZE_PATH = os.environ.get(
    "STREAM_BRONZE_PATH",
    "hdfs://namenode:9000/nyc-taxi/bronze/streaming",
)

ZONE_LOOKUP_PATH = os.environ.get(
    "ZONE_LOOKUP_PATH",
    "hdfs://namenode:9000/nyc-taxi/reference/taxi_zone_lookup.csv",
)

SILVER_PATH = os.environ.get(
    "SILVER_PATH",
    "hdfs://namenode:9000/nyc-taxi/silver/trips",
)

YEAR_MONTH_RE = re.compile(r"^(20\d{2})-(0[1-9]|1[0-2])$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("nyc_taxi_bronze_to_silver")


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("NYC-Yellow-Taxi-Bronze-to-Silver")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        os.environ.get("SPARK_LOG_LEVEL", "WARN")
    )
    return spark


# ---------------------------------------------------------------------------
# HDFS discovery helpers
# ---------------------------------------------------------------------------

def _hadoop_path(spark: SparkSession, path: str):
    return spark._jvm.org.apache.hadoop.fs.Path(path)


def _hadoop_fs(spark: SparkSession, path: str):
    """
    Resolve the Hadoop FileSystem from the path URI itself.

    This is important in Docker because Spark's default filesystem may be
    file:///, while our project paths explicitly use hdfs://namenode:9000.
    """
    hadoop_path = _hadoop_path(spark, path)
    return hadoop_path.getFileSystem(
        spark._jsc.hadoopConfiguration()
    )


def hdfs_exists(spark: SparkSession, path: str) -> bool:
    hadoop_path = _hadoop_path(spark, path)
    fs = _hadoop_fs(spark, path)
    return bool(fs.exists(hadoop_path))


def discover_batch_months(
    spark: SparkSession,
    requested_months: set[str] | None = None,
) -> list[tuple[str, str]]:
    """
    Return [(YYYY-MM, hdfs_month_path), ...].

    Batch files were stored as:
        /bronze/batch/year=YYYY/month=MM/
    """
    Path = spark._jvm.org.apache.hadoop.fs.Path
    root = Path(BATCH_BRONZE_PATH)
    fs = _hadoop_fs(spark, BATCH_BRONZE_PATH)

    if not fs.exists(root):
        raise FileNotFoundError(
            f"Batch Bronze path does not exist: {BATCH_BRONZE_PATH}"
        )

    found: list[tuple[str, str]] = []

    for year_status in fs.listStatus(root):
        if not year_status.isDirectory():
            continue

        year_name = year_status.getPath().getName()
        if not year_name.startswith("year="):
            continue

        year = year_name.split("=", 1)[1]

        for month_status in fs.listStatus(year_status.getPath()):
            if not month_status.isDirectory():
                continue

            month_name = month_status.getPath().getName()
            if not month_name.startswith("month="):
                continue

            month = month_name.split("=", 1)[1]
            year_month = f"{year}-{month}"

            if not YEAR_MONTH_RE.fullmatch(year_month):
                continue

            if requested_months and year_month not in requested_months:
                continue

            found.append(
                (year_month, month_status.getPath().toString())
            )

    return sorted(found)


# ---------------------------------------------------------------------------
# Schema normalization helpers
# ---------------------------------------------------------------------------

def find_existing_column(
    df: DataFrame,
    candidates: Iterable[str],
) -> str | None:
    """Find the first candidate column, case-insensitively."""
    exact = {c: c for c in df.columns}
    lower = {}
    for c in df.columns:
        lower.setdefault(c.lower(), c)

    for candidate in candidates:
        if candidate in exact:
            return exact[candidate]

        match = lower.get(candidate.lower())
        if match is not None:
            return match

    return None


def col_or_null(
    df: DataFrame,
    candidates: Iterable[str],
    data_type,
):
    existing = find_existing_column(df, candidates)

    if existing is None:
        return F.lit(None).cast(data_type)

    return F.col(existing).cast(data_type)


def canonicalize_batch_month(
    df: DataFrame,
    year_month: str,
) -> DataFrame:
    """
    Convert one original TLC Parquet month into the canonical Silver input
    schema. Reading month-by-month avoids schema drift problems across years.
    """

    return df.select(
        col_or_null(df, ["VendorID"], LongType()).alias("vendor_id"),

        col_or_null(
            df,
            ["tpep_pickup_datetime"],
            TimestampType(),
        ).alias("pickup_datetime"),

        col_or_null(
            df,
            ["tpep_dropoff_datetime"],
            TimestampType(),
        ).alias("dropoff_datetime"),

        col_or_null(
            df,
            ["passenger_count"],
            LongType(),
        ).alias("passenger_count"),

        col_or_null(
            df,
            ["trip_distance"],
            DoubleType(),
        ).alias("trip_distance"),

        col_or_null(
            df,
            ["RatecodeID"],
            LongType(),
        ).alias("rate_code_id"),

        col_or_null(
            df,
            ["store_and_fwd_flag"],
            StringType(),
        ).alias("store_and_fwd_flag"),

        col_or_null(
            df,
            ["PULocationID"],
            LongType(),
        ).alias("pickup_location_id"),

        col_or_null(
            df,
            ["DOLocationID"],
            LongType(),
        ).alias("dropoff_location_id"),

        col_or_null(
            df,
            ["payment_type"],
            LongType(),
        ).alias("payment_type"),

        col_or_null(df, ["fare_amount"], DoubleType()).alias("fare_amount"),
        col_or_null(df, ["extra"], DoubleType()).alias("extra"),
        col_or_null(df, ["mta_tax"], DoubleType()).alias("mta_tax"),
        col_or_null(df, ["tip_amount"], DoubleType()).alias("tip_amount"),
        col_or_null(df, ["tolls_amount"], DoubleType()).alias("tolls_amount"),

        col_or_null(
            df,
            ["improvement_surcharge"],
            DoubleType(),
        ).alias("improvement_surcharge"),

        col_or_null(
            df,
            ["total_amount"],
            DoubleType(),
        ).alias("total_amount"),

        col_or_null(
            df,
            ["congestion_surcharge"],
            DoubleType(),
        ).alias("congestion_surcharge"),

        col_or_null(
            df,
            ["Airport_fee", "airport_fee"],
            DoubleType(),
        ).alias("airport_fee"),

        col_or_null(
            df,
            ["cbd_congestion_fee"],
            DoubleType(),
        ).alias("cbd_congestion_fee"),

        F.lit("batch").alias("ingestion_source"),
        F.lit(year_month).alias("source_year_month"),

        F.lit(None).cast(LongType()).alias("kafka_partition"),
        F.lit(None).cast(LongType()).alias("kafka_offset"),
        F.lit(None).cast(TimestampType()).alias("kafka_timestamp"),
    )


def json_field(raw_json_col, *paths: str):
    """Return the first non-null JSON field among candidate keys."""
    expressions = [
        F.get_json_object(raw_json_col, f"$.{path}")
        for path in paths
    ]
    return F.coalesce(*expressions)


def canonicalize_streaming(df: DataFrame) -> DataFrame:
    """
    Parse streaming Bronze raw_json into the same canonical schema as batch.
    """

    raw = F.col("raw_json")

    return df.select(
        json_field(raw, "VendorID").cast(LongType()).alias("vendor_id"),

        json_field(
            raw,
            "tpep_pickup_datetime",
        ).cast(TimestampType()).alias("pickup_datetime"),

        json_field(
            raw,
            "tpep_dropoff_datetime",
        ).cast(TimestampType()).alias("dropoff_datetime"),

        json_field(
            raw,
            "passenger_count",
        ).cast(LongType()).alias("passenger_count"),

        json_field(
            raw,
            "trip_distance",
        ).cast(DoubleType()).alias("trip_distance"),

        json_field(
            raw,
            "RatecodeID",
        ).cast(LongType()).alias("rate_code_id"),

        json_field(
            raw,
            "store_and_fwd_flag",
        ).cast(StringType()).alias("store_and_fwd_flag"),

        json_field(
            raw,
            "PULocationID",
        ).cast(LongType()).alias("pickup_location_id"),

        json_field(
            raw,
            "DOLocationID",
        ).cast(LongType()).alias("dropoff_location_id"),

        json_field(
            raw,
            "payment_type",
        ).cast(LongType()).alias("payment_type"),

        json_field(raw, "fare_amount").cast(DoubleType()).alias("fare_amount"),
        json_field(raw, "extra").cast(DoubleType()).alias("extra"),
        json_field(raw, "mta_tax").cast(DoubleType()).alias("mta_tax"),
        json_field(raw, "tip_amount").cast(DoubleType()).alias("tip_amount"),
        json_field(raw, "tolls_amount").cast(DoubleType()).alias("tolls_amount"),

        json_field(
            raw,
            "improvement_surcharge",
        ).cast(DoubleType()).alias("improvement_surcharge"),

        json_field(
            raw,
            "total_amount",
        ).cast(DoubleType()).alias("total_amount"),

        json_field(
            raw,
            "congestion_surcharge",
        ).cast(DoubleType()).alias("congestion_surcharge"),

        json_field(
            raw,
            "Airport_fee",
            "airport_fee",
        ).cast(DoubleType()).alias("airport_fee"),

        json_field(
            raw,
            "cbd_congestion_fee",
        ).cast(DoubleType()).alias("cbd_congestion_fee"),

        F.lit("streaming").alias("ingestion_source"),

        F.coalesce(
            F.col("year_month"),
            F.concat_ws("-", F.col("year"), F.col("month")),
        ).alias("source_year_month"),

        F.col("kafka_partition").cast(LongType()).alias("kafka_partition"),
        F.col("kafka_offset").cast(LongType()).alias("kafka_offset"),
        F.col("kafka_timestamp").cast(TimestampType()).alias("kafka_timestamp"),
    )


# ---------------------------------------------------------------------------
# Read Bronze
# ---------------------------------------------------------------------------

def read_batch(
    spark: SparkSession,
    requested_months: set[str] | None,
) -> DataFrame | None:
    months = discover_batch_months(
        spark=spark,
        requested_months=requested_months,
    )

    if not months:
        log.warning("No matching batch months found.")
        return None

    log.info("Batch months selected: %d", len(months))

    normalized: list[DataFrame] = []

    for year_month, path in months:
        log.info("Reading batch month %s -> %s", year_month, path)

        month_df = spark.read.parquet(path)

        normalized.append(
            canonicalize_batch_month(
                df=month_df,
                year_month=year_month,
            )
        )

    return reduce(
        lambda left, right: left.unionByName(
            right,
            allowMissingColumns=True,
        ),
        normalized,
    )


def read_streaming(spark: SparkSession) -> DataFrame | None:
    if not hdfs_exists(spark, STREAM_BRONZE_PATH):
        log.warning(
            "Streaming Bronze path does not exist: %s",
            STREAM_BRONZE_PATH,
        )
        return None

    log.info("Reading streaming Bronze -> %s", STREAM_BRONZE_PATH)

    stream_bronze = spark.read.parquet(STREAM_BRONZE_PATH)

    if "raw_json" not in stream_bronze.columns:
        raise RuntimeError(
            "Streaming Bronze does not contain required raw_json column."
        )

    return canonicalize_streaming(stream_bronze)


# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------

def clean_trips(df: DataFrame) -> DataFrame:
    """
    Conservative Silver cleaning.

    We remove only records that are structurally unusable:
      - missing pickup/dropoff timestamp
      - pickup timestamp outside its source YYYY-MM
      - dropoff <= pickup
      - missing pickup/dropoff LocationID
      - negative trip distance
      - negative passenger count when passenger_count is present

    We intentionally DO NOT:
      - remove zero passenger counts
      - remove zero trip distances
      - remove negative fares/totals
      - arbitrarily cap long trips
      - drop duplicates without a true trip primary key
    """

    cleaned = (
        df
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(
            F.date_format(F.col("pickup_datetime"), "yyyy-MM")
            == F.col("source_year_month")
        )
        .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
        .filter(F.col("pickup_location_id").isNotNull())
        .filter(F.col("dropoff_location_id").isNotNull())
        .filter(
            F.col("trip_distance").isNull()
            | (F.col("trip_distance") >= F.lit(0.0))
        )
        .filter(
            F.col("passenger_count").isNull()
            | (F.col("passenger_count") >= F.lit(0))
        )
    )

    return (
        cleaned
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("dropoff_datetime")
                - F.unix_timestamp("pickup_datetime")
            ) / F.lit(60.0),
        )
        .withColumn(
            "pickup_date",
            F.to_date("pickup_datetime"),
        )
        .withColumn(
            "pickup_year",
            F.year("pickup_datetime"),
        )
        .withColumn(
            "pickup_month",
            F.format_string(
                "%02d",
                F.month("pickup_datetime"),
            ),
        )
        .withColumn(
            "pickup_day",
            F.dayofmonth("pickup_datetime"),
        )
        .withColumn(
            "pickup_hour",
            F.hour("pickup_datetime"),
        )
        .withColumn(
            "pickup_day_of_week",
            F.date_format("pickup_datetime", "EEEE"),
        )
    )


# ---------------------------------------------------------------------------
# Taxi zone lookup enrichment
# ---------------------------------------------------------------------------

def read_zone_lookup(spark: SparkSession) -> DataFrame:
    if not hdfs_exists(spark, ZONE_LOOKUP_PATH):
        raise FileNotFoundError(
            f"Taxi Zone Lookup does not exist: {ZONE_LOOKUP_PATH}"
        )

    lookup = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(ZONE_LOOKUP_PATH)
    )

    required = {"LocationID", "Borough", "Zone", "service_zone"}
    missing = required.difference(lookup.columns)

    if missing:
        raise RuntimeError(
            "Taxi Zone Lookup missing required columns: "
            + ", ".join(sorted(missing))
        )

    return (
        lookup
        .select(
            F.col("LocationID").cast(LongType()).alias("location_id"),
            F.col("Borough").cast(StringType()).alias("borough"),
            F.col("Zone").cast(StringType()).alias("zone"),
            F.col("service_zone").cast(StringType()).alias("service_zone"),
        )
        .dropDuplicates(["location_id"])
    )


def enrich_with_zones(
    trips: DataFrame,
    lookup: DataFrame,
) -> DataFrame:

    pickup_lookup = lookup.select(
        F.col("location_id").alias("_pickup_lookup_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )

    dropoff_lookup = lookup.select(
        F.col("location_id").alias("_dropoff_lookup_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
        F.col("service_zone").alias("dropoff_service_zone"),
    )

    return (
        trips
        .join(
            F.broadcast(pickup_lookup),
            F.col("pickup_location_id")
            == F.col("_pickup_lookup_id"),
            "left",
        )
        .drop("_pickup_lookup_id")
        .join(
            F.broadcast(dropoff_lookup),
            F.col("dropoff_location_id")
            == F.col("_dropoff_lookup_id"),
            "left",
        )
        .drop("_dropoff_lookup_id")
    )


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_silver(
    df: DataFrame,
    output_path: str,
    mode: str,
) -> None:
    log.info("Writing Silver -> %s", output_path)
    log.info("Write mode      -> %s", mode)

    (
        df.write
        .mode(mode)
        .option("compression", "snappy")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(output_path)
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize, clean, union, and enrich NYC Yellow Taxi "
            "Batch + Streaming Bronze data into HDFS Silver."
        )
    )

    parser.add_argument(
        "--output-path",
        default=SILVER_PATH,
        help=f"Silver output path. Default: {SILVER_PATH}",
    )

    parser.add_argument(
        "--mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Spark write mode. Default: overwrite",
    )

    parser.add_argument(
        "--batch-month",
        action="append",
        default=None,
        help=(
            "Optional YYYY-MM batch month to process. "
            "May be specified multiple times. "
            "If omitted, all discovered batch months are processed."
        ),
    )

    parser.add_argument(
        "--skip-batch",
        action="store_true",
        help="Do not read Batch Bronze.",
    )

    parser.add_argument(
        "--skip-streaming",
        action="store_true",
        help="Do not read Streaming Bronze.",
    )

    args = parser.parse_args()

    if args.skip_batch and args.skip_streaming:
        log.error(
            "Cannot use --skip-batch and --skip-streaming together."
        )
        return 2

    requested_months: set[str] | None = None

    if args.batch_month:
        requested_months = set()

        for year_month in args.batch_month:
            if not YEAR_MONTH_RE.fullmatch(year_month):
                log.error(
                    "Invalid --batch-month %s. Expected YYYY-MM.",
                    year_month,
                )
                return 2

            requested_months.add(year_month)

    spark = None

    try:
        log.info("=" * 72)
        log.info("NYC YELLOW TAXI - BRONZE -> SILVER")
        log.info("Batch Bronze     : %s", BATCH_BRONZE_PATH)
        log.info("Streaming Bronze : %s", STREAM_BRONZE_PATH)
        log.info("Zone Lookup      : %s", ZONE_LOOKUP_PATH)
        log.info("Silver Output    : %s", args.output_path)
        log.info("=" * 72)

        spark = create_spark_session()

        sources: list[DataFrame] = []

        if not args.skip_batch:
            batch_df = read_batch(
                spark=spark,
                requested_months=requested_months,
            )
            if batch_df is not None:
                sources.append(batch_df)

        if not args.skip_streaming:
            streaming_df = read_streaming(spark)
            if streaming_df is not None:
                sources.append(streaming_df)

        if not sources:
            log.error("No Bronze data was available to process.")
            return 1

        unified = reduce(
            lambda left, right: left.unionByName(
                right,
                allowMissingColumns=True,
            ),
            sources,
        )

        cleaned = clean_trips(unified)

        lookup = read_zone_lookup(spark)

        silver = enrich_with_zones(
            trips=cleaned,
            lookup=lookup,
        )

        # Stable column ordering for easier downstream use.
        ordered_columns = [
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "rate_code_id",
            "store_and_fwd_flag",
            "pickup_location_id",
            "pickup_zone",
            "pickup_borough",
            "pickup_service_zone",
            "dropoff_location_id",
            "dropoff_zone",
            "dropoff_borough",
            "dropoff_service_zone",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
            "cbd_congestion_fee",
            "trip_duration_minutes",
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "pickup_day",
            "pickup_hour",
            "pickup_day_of_week",
            "ingestion_source",
            "source_year_month",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        ]

        silver = silver.select(*ordered_columns)

        write_silver(
            df=silver,
            output_path=args.output_path,
            mode=args.mode,
        )

        log.info("=" * 72)
        log.info("BRONZE -> SILVER COMPLETE")
        log.info("Output: %s", args.output_path)
        log.info("=" * 72)

        return 0

    except Exception as exc:
        log.exception("Bronze -> Silver failed: %s", exc)
        return 1

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
