#!/usr/bin/env python3

"""
Single-trip inference for the saved NYC Yellow Taxi trip-duration GBT model.

This script mirrors the exact feature engineering used during training so it
can later be reused inside the Streamlit ML prediction page.

Example:
    spark-submit predict_trip_duration.py \
        --pickup-location-id 132 \
        --dropoff-location-id 161 \
        --passenger-count 1 \
        --trip-distance 16.5 \
        --rate-code-id 1 \
        --pickup-hour 18 \
        --pickup-day-of-week Friday \
        --pickup-month 12
"""

from __future__ import annotations

import argparse
import math

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_MODEL_PATH = (
    "hdfs://namenode:9000/nyc-taxi/ml/models/duration/gbt"
)

DAY_TO_NUM = {
    "monday": 1,
    "tuesday": 2,
    "wednesday": 3,
    "thursday": 4,
    "friday": 5,
    "saturday": 6,
    "sunday": 7,
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Predict NYC Yellow Taxi trip duration."
    )

    parser.add_argument("--pickup-location-id", type=int, required=True)
    parser.add_argument("--dropoff-location-id", type=int, required=True)
    parser.add_argument("--passenger-count", type=int, required=True)
    parser.add_argument("--trip-distance", type=float, required=True)
    parser.add_argument("--rate-code-id", type=int, default=1)
    parser.add_argument("--pickup-hour", type=int, required=True)
    parser.add_argument(
        "--pickup-day-of-week",
        type=str,
        required=True,
        choices=[
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ],
    )
    parser.add_argument("--pickup-month", type=int, required=True)
    parser.add_argument("--model-path", default=DEFAULT_MODEL_PATH)

    return parser.parse_args()


def validate_args(args):
    if not 1 <= args.pickup_location_id <= 265:
        raise ValueError("pickup-location-id must be between 1 and 265.")

    if not 1 <= args.dropoff_location_id <= 265:
        raise ValueError("dropoff-location-id must be between 1 and 265.")

    if not 1 <= args.passenger_count <= 6:
        raise ValueError("passenger-count must be between 1 and 6.")

    if not 0.1 <= args.trip_distance <= 100.0:
        raise ValueError("trip-distance must be between 0.1 and 100 miles.")

    if not 1 <= args.rate_code_id <= 6:
        raise ValueError("rate-code-id must be between 1 and 6.")

    if not 0 <= args.pickup_hour <= 23:
        raise ValueError("pickup-hour must be between 0 and 23.")

    if not 1 <= args.pickup_month <= 12:
        raise ValueError("pickup-month must be between 1 and 12.")


def build_input_row(args):
    day_num = DAY_TO_NUM[args.pickup_day_of_week.lower()]

    is_weekend = 1 if day_num in (6, 7) else 0
    is_rush_hour = (
        1
        if args.pickup_hour in (7, 8, 9, 16, 17, 18, 19)
        else 0
    )

    hour_angle = 2.0 * math.pi * args.pickup_hour / 24.0
    month_angle = (
        2.0 * math.pi * (args.pickup_month - 1) / 12.0
    )

    return {
        "pickup_location_id": int(args.pickup_location_id),
        "dropoff_location_id": int(args.dropoff_location_id),
        "passenger_count": int(args.passenger_count),
        "trip_distance": float(args.trip_distance),
        "rate_code_id": int(args.rate_code_id),
        "pickup_hour": int(args.pickup_hour),
        "pickup_day_of_week_num": int(day_num),
        "pickup_month": int(args.pickup_month),
        "is_weekend": int(is_weekend),
        "is_rush_hour": int(is_rush_hour),
        "pickup_hour_sin": float(math.sin(hour_angle)),
        "pickup_hour_cos": float(math.cos(hour_angle)),
        "pickup_month_sin": float(math.sin(month_angle)),
        "pickup_month_cos": float(math.cos(month_angle)),
    }


def add_categorical_strings(df):
    return (
        df
        .withColumn(
            "pickup_location_id_cat",
            F.col("pickup_location_id").cast("string"),
        )
        .withColumn(
            "dropoff_location_id_cat",
            F.col("dropoff_location_id").cast("string"),
        )
        .withColumn(
            "rate_code_id_cat",
            F.col("rate_code_id").cast("string"),
        )
        .withColumn(
            "pickup_day_of_week_num_cat",
            F.col("pickup_day_of_week_num").cast("string"),
        )
    )


def main():
    args = parse_args()
    validate_args(args)

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Trip-Duration-Inference")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    model = PipelineModel.load(args.model_path)

    input_row = build_input_row(args)

    df = spark.createDataFrame([input_row])
    df = add_categorical_strings(df)

    result = (
        model
        .transform(df)
        .select(
            "pickup_location_id",
            "dropoff_location_id",
            "passenger_count",
            "trip_distance",
            "rate_code_id",
            "pickup_hour",
            "pickup_day_of_week_num",
            "pickup_month",
            F.col("prediction").cast("double"),
        )
        .first()
    )

    prediction = float(result["prediction"])

    print("=" * 68)
    print("NYC YELLOW TAXI - TRIP DURATION PREDICTION")
    print("=" * 68)
    print(f"Pickup Location ID : {args.pickup_location_id}")
    print(f"Dropoff Location ID: {args.dropoff_location_id}")
    print(f"Passengers         : {args.passenger_count}")
    print(f"Estimated distance : {args.trip_distance:.2f} miles")
    print(f"Rate code          : {args.rate_code_id}")
    print(f"Pickup day         : {args.pickup_day_of_week}")
    print(f"Pickup hour        : {args.pickup_hour:02d}:00")
    print(f"Pickup month       : {args.pickup_month}")
    print("-" * 68)
    print(f"PREDICTED DURATION : {prediction:.2f} minutes")
    print("=" * 68)

    spark.stop()


if __name__ == "__main__":
    main()
