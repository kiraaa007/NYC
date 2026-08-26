#!/usr/bin/env python3

"""
Train and compare Spark ML trip-duration regression models.

Input:
    /nyc-taxi/ml/duration_dataset/train
    /nyc-taxi/ml/duration_dataset/test

Models:
    1. Mean baseline
    2. Linear Regression
    3. Random Forest Regressor
    4. Gradient-Boosted Trees Regressor

Evaluation:
    MAE
    RMSE
    R2

The pickup/dropoff LocationIDs and rate code are treated as categorical
features rather than continuous numeric quantities.
"""

from __future__ import annotations

import argparse
import time

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import (
    OneHotEncoder,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.regression import (
    GBTRegressor,
    LinearRegression,
    RandomForestRegressor,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_TRAIN_PATH = (
    "hdfs://namenode:9000/nyc-taxi/ml/duration_dataset/train"
)
DEFAULT_TEST_PATH = (
    "hdfs://namenode:9000/nyc-taxi/ml/duration_dataset/test"
)
DEFAULT_MODEL_BASE = (
    "hdfs://namenode:9000/nyc-taxi/ml/models/duration"
)
DEFAULT_METRICS_PATH = (
    "hdfs://namenode:9000/nyc-taxi/ml/metrics/duration_model_comparison"
)
DEFAULT_PREDICTIONS_PATH = (
    "hdfs://namenode:9000/nyc-taxi/ml/predictions/duration/2025-12"
)


CATEGORICAL_SOURCE_COLS = [
    "pickup_location_id",
    "dropoff_location_id",
    "rate_code_id",
    "pickup_day_of_week_num",
]

CATEGORICAL_STRING_COLS = [
    "pickup_location_id_cat",
    "dropoff_location_id_cat",
    "rate_code_id_cat",
    "pickup_day_of_week_num_cat",
]

INDEXED_COLS = [
    "pickup_location_id_idx",
    "dropoff_location_id_idx",
    "rate_code_id_idx",
    "pickup_day_of_week_num_idx",
]

OHE_COLS = [
    "pickup_location_id_ohe",
    "dropoff_location_id_ohe",
    "rate_code_id_ohe",
    "pickup_day_of_week_num_ohe",
]

# Features available on the Streamlit prediction page.
NUMERIC_FEATURES = [
    "passenger_count",
    "trip_distance",
    "pickup_hour",
    "pickup_month",
    "is_weekend",
    "is_rush_hour",
    "pickup_hour_sin",
    "pickup_hour_cos",
    "pickup_month_sin",
    "pickup_month_cos",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Train NYC Taxi trip-duration regression models."
    )

    parser.add_argument("--train-path", default=DEFAULT_TRAIN_PATH)
    parser.add_argument("--test-path", default=DEFAULT_TEST_PATH)
    parser.add_argument("--model-base", default=DEFAULT_MODEL_BASE)
    parser.add_argument("--metrics-path", default=DEFAULT_METRICS_PATH)
    parser.add_argument(
        "--predictions-path",
        default=DEFAULT_PREDICTIONS_PATH,
    )
    parser.add_argument(
        "--models",
        default="linear,rf,gbt",
        help=(
            "Comma-separated models to train. "
            "Allowed: linear,rf,gbt"
        ),
    )
    parser.add_argument("--seed", type=int, default=42)

    return parser.parse_args()


def add_categorical_strings(df):
    result = df

    for source, target in zip(
        CATEGORICAL_SOURCE_COLS,
        CATEGORICAL_STRING_COLS,
    ):
        result = result.withColumn(
            target,
            F.col(source).cast("string"),
        )

    return result


def indexer_stages():
    return [
        StringIndexer(
            inputCol=input_col,
            outputCol=output_col,
            handleInvalid="keep",
            stringOrderType="alphabetAsc",
        )
        for input_col, output_col in zip(
            CATEGORICAL_STRING_COLS,
            INDEXED_COLS,
        )
    ]


def evaluate_predictions(predictions):
    mae = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="mae",
    ).evaluate(predictions)

    rmse = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="rmse",
    ).evaluate(predictions)

    r2 = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="r2",
    ).evaluate(predictions)

    return {
        "mae": float(mae),
        "rmse": float(rmse),
        "r2": float(r2),
    }


def build_linear_pipeline():
    indexers = indexer_stages()

    encoder = OneHotEncoder(
        inputCols=INDEXED_COLS,
        outputCols=OHE_COLS,
        handleInvalid="keep",
        dropLast=True,
    )

    assembler = VectorAssembler(
        inputCols=OHE_COLS + NUMERIC_FEATURES,
        outputCol="features",
        handleInvalid="error",
    )

    model = LinearRegression(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        maxIter=50,
        regParam=0.05,
        elasticNetParam=0.0,
        standardization=True,
    )

    return Pipeline(
        stages=indexers + [encoder, assembler, model]
    )


def build_rf_pipeline(seed):
    indexers = indexer_stages()

    assembler = VectorAssembler(
        inputCols=INDEXED_COLS + NUMERIC_FEATURES,
        outputCol="features",
        handleInvalid="error",
    )

    model = RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        numTrees=30,
        maxDepth=8,
        maxBins=300,
        subsamplingRate=0.8,
        featureSubsetStrategy="sqrt",
        seed=seed,
    )

    return Pipeline(
        stages=indexers + [assembler, model]
    )


def build_gbt_pipeline(seed):
    indexers = indexer_stages()

    assembler = VectorAssembler(
        inputCols=INDEXED_COLS + NUMERIC_FEATURES,
        outputCol="features",
        handleInvalid="error",
    )

    model = GBTRegressor(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        maxIter=25,
        maxDepth=5,
        maxBins=300,
        stepSize=0.08,
        subsamplingRate=0.8,
        seed=seed,
        lossType="squared",
    )

    return Pipeline(
        stages=indexers + [assembler, model]
    )


def main():
    args = parse_args()

    requested_models = [
        x.strip().lower()
        for x in args.models.split(",")
        if x.strip()
    ]

    allowed = {"linear", "rf", "gbt"}
    invalid = sorted(set(requested_models) - allowed)

    if invalid:
        raise ValueError(
            "Unsupported model(s): " + ", ".join(invalid)
        )

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Trip-Duration-Models")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 76)
    print("NYC TAXI - TRIP DURATION MODEL TRAINING")
    print("=" * 76)
    print(f"Train path : {args.train_path}")
    print(f"Test path  : {args.test_path}")
    print(f"Models     : {', '.join(requested_models)}")
    print(f"Seed       : {args.seed}")

    train = add_categorical_strings(
        spark.read.parquet(args.train_path)
    ).cache()

    test = add_categorical_strings(
        spark.read.parquet(args.test_path)
    ).cache()

    train_rows = train.count()
    test_rows = test.count()

    print(f"\nTraining rows: {train_rows:,}")
    print(f"Test rows    : {test_rows:,}")

    metrics = []

    # ------------------------------------------------------------------
    # Baseline: predict the historical training mean for every test row.
    # ------------------------------------------------------------------
    train_mean = train.agg(
        F.avg("label").alias("mean_label")
    ).first()["mean_label"]

    baseline_predictions = test.withColumn(
        "prediction",
        F.lit(float(train_mean)),
    )

    baseline_metrics = evaluate_predictions(
        baseline_predictions
    )

    metrics.append(
        {
            "model": "mean_baseline",
            "mae": baseline_metrics["mae"],
            "rmse": baseline_metrics["rmse"],
            "r2": baseline_metrics["r2"],
            "training_seconds": 0.0,
            "model_path": "",
        }
    )

    print("\nMEAN BASELINE")
    print(
        f"MAE={baseline_metrics['mae']:.4f} | "
        f"RMSE={baseline_metrics['rmse']:.4f} | "
        f"R2={baseline_metrics['r2']:.4f}"
    )

    builders = {
        "linear": lambda: build_linear_pipeline(),
        "rf": lambda: build_rf_pipeline(args.seed),
        "gbt": lambda: build_gbt_pipeline(args.seed),
    }

    trained_paths = {}

    for model_name in requested_models:
        print("\n" + "-" * 76)
        print(f"TRAINING: {model_name.upper()}")
        print("-" * 76)

        pipeline = builders[model_name]()

        start_time = time.time()
        fitted = pipeline.fit(train)
        training_seconds = time.time() - start_time

        predictions = fitted.transform(test).cache()
        result = evaluate_predictions(predictions)

        model_path = f"{args.model_base}/{model_name}"

        fitted.write().overwrite().save(model_path)
        trained_paths[model_name] = model_path

        metrics.append(
            {
                "model": model_name,
                "mae": result["mae"],
                "rmse": result["rmse"],
                "r2": result["r2"],
                "training_seconds": float(training_seconds),
                "model_path": model_path,
            }
        )

        print(
            f"{model_name.upper()} RESULTS | "
            f"MAE={result['mae']:.4f} | "
            f"RMSE={result['rmse']:.4f} | "
            f"R2={result['r2']:.4f} | "
            f"TrainSeconds={training_seconds:.1f}"
        )

        predictions.unpersist()

    metrics_df = (
        spark.createDataFrame(metrics)
        .orderBy(F.col("rmse").asc())
    )

    metrics_df.write.mode("overwrite").parquet(
        args.metrics_path
    )

    print("\n" + "=" * 76)
    print("MODEL COMPARISON")
    print("=" * 76)

    metrics_df.select(
        "model",
        F.round("mae", 4).alias("mae"),
        F.round("rmse", 4).alias("rmse"),
        F.round("r2", 4).alias("r2"),
        F.round(
            "training_seconds",
            1,
        ).alias("training_seconds"),
    ).show(truncate=False)

    # Pick the best TRAINED model, not the constant baseline.
    trained_metrics = [
        item
        for item in metrics
        if item["model"] != "mean_baseline"
    ]

    best = min(
        trained_metrics,
        key=lambda item: item["rmse"],
    )

    best_model_name = best["model"]
    best_model_path = best["model_path"]

    print(f"Best model by RMSE : {best_model_name}")
    print(f"Best model path    : {best_model_path}")

    best_info = spark.createDataFrame(
        [
            {
                "best_model": best_model_name,
                "model_path": best_model_path,
                "mae": best["mae"],
                "rmse": best["rmse"],
                "r2": best["r2"],
            }
        ]
    )

    best_info.write.mode("overwrite").json(
        f"{args.model_base}/best_model_info"
    )

    # Reload the saved model to prove that the persisted artifact works,
    # then write December predictions for later Streamlit evaluation.
    best_model = PipelineModel.load(best_model_path)

    best_predictions = (
        best_model
        .transform(test)
        .select(
            "pickup_location_id",
            "dropoff_location_id",
            "passenger_count",
            "trip_distance",
            "rate_code_id",
            "pickup_hour",
            "pickup_day_of_week_num",
            "pickup_month",
            "label",
            F.col("prediction").cast("double"),
        )
    )

    best_predictions.write.mode("overwrite").parquet(
        args.predictions_path
    )

    print(f"Metrics path        : {args.metrics_path}")
    print(f"Predictions path    : {args.predictions_path}")
    print("\nTRIP DURATION MODEL TRAINING COMPLETE")

    train.unpersist()
    test.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
