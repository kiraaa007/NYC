#!/usr/bin/env python3

"""
Build the ML dataset for NYC Yellow Taxi trip-duration prediction.

Input:
    hdfs://namenode:9000/nyc-taxi/silver/trips

Temporal split:
    Train: 2021-01 through 2025-11
    Test : 2025-12

Target:
    trip_duration_minutes
"""

from __future__ import annotations

import argparse
import math

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


DEFAULT_SILVER_PATH = "hdfs://namenode:9000/nyc-taxi/silver/trips"
DEFAULT_OUTPUT_BASE = "hdfs://namenode:9000/nyc-taxi/ml/duration_dataset"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build train/test data for trip-duration prediction."
    )

    parser.add_argument(
        "--silver-path",
        default=DEFAULT_SILVER_PATH,
    )

    parser.add_argument(
        "--output-base",
        default=DEFAULT_OUTPUT_BASE,
    )

    parser.add_argument(
        "--train-fraction",
        type=float,
        default=0.005,
        help="Fraction of cleaned historical rows to keep for training.",
    )

    parser.add_argument(
        "--test-fraction",
        type=float,
        default=0.05,
        help="Fraction of cleaned 2025-12 rows to keep for testing.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
    )

    return parser.parse_args()


def prepare_features(df):
    """
    Apply ML-specific cleaning and create prediction-time features.
    """

    # IMPORTANT:
    # Spark Column expressions must be created only after a SparkSession /
    # SparkContext exists. Therefore this map is defined here rather than
    # at module import time.
    day_to_num = F.create_map(
        F.lit("Monday"), F.lit(1),
        F.lit("Tuesday"), F.lit(2),
        F.lit("Wednesday"), F.lit(3),
        F.lit("Thursday"), F.lit(4),
        F.lit("Friday"), F.lit(5),
        F.lit("Saturday"), F.lit(6),
        F.lit("Sunday"), F.lit(7),
    )

    cleaned = (
        df
        .select(
            "pickup_location_id",
            "dropoff_location_id",
            "passenger_count",
            "trip_distance",
            "rate_code_id",
            "pickup_hour",
            "pickup_day_of_week",
            "pickup_month",
            "pickup_year",
            "source_year_month",
            "trip_duration_minutes",
        )
        .filter(
            F.col("trip_duration_minutes").isNotNull()
            & F.col("trip_duration_minutes").between(1.0, 180.0)
        )
        .filter(
            F.col("trip_distance").isNotNull()
            & F.col("trip_distance").between(0.1, 100.0)
        )
        .filter(
            F.col("passenger_count").isNotNull()
            & F.col("passenger_count").between(1, 6)
        )
        .filter(
            F.col("pickup_location_id").isNotNull()
            & F.col("pickup_location_id").between(1, 265)
        )
        .filter(
            F.col("dropoff_location_id").isNotNull()
            & F.col("dropoff_location_id").between(1, 265)
        )
        .filter(
            F.col("rate_code_id").isNotNull()
            & F.col("rate_code_id").between(1, 6)
        )
        .filter(
            F.col("pickup_hour").isNotNull()
            & F.col("pickup_hour").between(0, 23)
        )
        .filter(F.col("pickup_day_of_week").isNotNull())
        .filter(F.col("pickup_month").between(1, 12))
    )

    featured = (
        cleaned
        .withColumn(
            "pickup_day_of_week_num",
            day_to_num[F.col("pickup_day_of_week")].cast(T.IntegerType()),
        )
        .filter(F.col("pickup_day_of_week_num").isNotNull())
        .withColumn(
            "is_weekend",
            F.when(
                F.col("pickup_day_of_week_num").isin(6, 7),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "is_rush_hour",
            F.when(
                F.col("pickup_hour").isin(7, 8, 9, 16, 17, 18, 19),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "pickup_hour_sin",
            F.sin(
                F.col("pickup_hour") * F.lit(2.0 * math.pi / 24.0)
            ),
        )
        .withColumn(
            "pickup_hour_cos",
            F.cos(
                F.col("pickup_hour") * F.lit(2.0 * math.pi / 24.0)
            ),
        )
        .withColumn(
            "pickup_month_sin",
            F.sin(
                (F.col("pickup_month") - F.lit(1))
                * F.lit(2.0 * math.pi / 12.0)
            ),
        )
        .withColumn(
            "pickup_month_cos",
            F.cos(
                (F.col("pickup_month") - F.lit(1))
                * F.lit(2.0 * math.pi / 12.0)
            ),
        )
    )

    return featured.select(
        F.col("pickup_location_id").cast("int"),
        F.col("dropoff_location_id").cast("int"),
        F.col("passenger_count").cast("int"),
        F.col("trip_distance").cast("double"),
        F.col("rate_code_id").cast("int"),
        F.col("pickup_hour").cast("int"),
        F.col("pickup_day_of_week_num").cast("int"),
        F.col("pickup_month").cast("int"),
        F.col("is_weekend").cast("int"),
        F.col("is_rush_hour").cast("int"),
        F.col("pickup_hour_sin").cast("double"),
        F.col("pickup_hour_cos").cast("double"),
        F.col("pickup_month_sin").cast("double"),
        F.col("pickup_month_cos").cast("double"),
        F.col("trip_duration_minutes").cast("double").alias("label"),
        "source_year_month",
    )


def main():
    args = parse_args()

    if not 0 < args.train_fraction <= 1:
        raise ValueError("--train-fraction must be in (0, 1].")

    if not 0 < args.test_fraction <= 1:
        raise ValueError("--test-fraction must be in (0, 1].")

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Duration-ML-Dataset")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("NYC TAXI - BUILD TRIP DURATION ML DATASET")
    print("=" * 70)
    print(f"Silver input   : {args.silver_path}")
    print(f"Output base    : {args.output_base}")
    print(f"Train fraction : {args.train_fraction}")
    print(f"Test fraction  : {args.test_fraction}")
    print(f"Random seed    : {args.seed}")

    silver = spark.read.parquet(args.silver_path)
    features = prepare_features(silver)

    train = (
        features
        .filter(F.col("source_year_month") < F.lit("2025-12"))
        .sample(
            withReplacement=False,
            fraction=args.train_fraction,
            seed=args.seed,
        )
        .drop("source_year_month")
    )

    test = (
        features
        .filter(F.col("source_year_month") == F.lit("2025-12"))
        .sample(
            withReplacement=False,
            fraction=args.test_fraction,
            seed=args.seed,
        )
        .drop("source_year_month")
    )

    train_path = f"{args.output_base}/train"
    test_path = f"{args.output_base}/test"

    print("\nWriting training dataset...")
    train.write.mode("overwrite").parquet(train_path)

    print("Writing test dataset...")
    test.write.mode("overwrite").parquet(test_path)

    saved_train = spark.read.parquet(train_path)
    saved_test = spark.read.parquet(test_path)

    train_count = saved_train.count()
    test_count = saved_test.count()

    print("\n" + "=" * 70)
    print("ML DATASET BUILD COMPLETE")
    print("=" * 70)
    print(f"Training rows : {train_count:,}")
    print(f"Test rows     : {test_count:,}")
    print(f"Training path : {train_path}")
    print(f"Test path     : {test_path}")

    print("\nTraining label statistics:")
    saved_train.select(
        F.min("label").alias("min_duration"),
        F.avg("label").alias("avg_duration"),
        F.expr("percentile_approx(label, 0.5)").alias("median_duration"),
        F.max("label").alias("max_duration"),
    ).show(truncate=False)

    print("\nTest label statistics:")
    saved_test.select(
        F.min("label").alias("min_duration"),
        F.avg("label").alias("avg_duration"),
        F.expr("percentile_approx(label, 0.5)").alias("median_duration"),
        F.max("label").alias("max_duration"),
    ).show(truncate=False)

    print("\nFeature schema:")
    saved_train.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
