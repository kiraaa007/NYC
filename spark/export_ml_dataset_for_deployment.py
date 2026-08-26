#!/usr/bin/env python3

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export the sampled duration ML train/test data locally."
    )

    parser.add_argument(
        "--train-source",
        default="hdfs://namenode:9000/nyc-taxi/ml/duration_dataset/train",
    )

    parser.add_argument(
        "--test-source",
        default="hdfs://namenode:9000/nyc-taxi/ml/duration_dataset/test",
    )

    parser.add_argument(
        "--output-base",
        default="file:///opt/ml-artifacts/data",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Export-ML-Deployment-Data")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    train = spark.read.parquet(args.train_source)
    test = spark.read.parquet(args.test_source)

    train_target = f"{args.output_base}/train"
    test_target = f"{args.output_base}/test"

    print("=" * 70)
    print("EXPORT ML DATA FOR DEPLOYMENT TRAINING")
    print("=" * 70)

    print(f"Training source : {args.train_source}")
    print(f"Training target : {train_target}")
    train.coalesce(8).write.mode("overwrite").parquet(train_target)

    print(f"Test source     : {args.test_source}")
    print(f"Test target     : {test_target}")
    test.coalesce(4).write.mode("overwrite").parquet(test_target)

    print(f"Training rows   : {train.count():,}")
    print(f"Test rows       : {test.count():,}")

    print("\nML DEPLOYMENT DATA EXPORT COMPLETE")
    spark.stop()


if __name__ == "__main__":
    main()
