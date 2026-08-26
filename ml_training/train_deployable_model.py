#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path
import time

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import TargetEncoder


ARTIFACT_DIR = Path("/artifacts")
TRAIN_PATH = ARTIFACT_DIR / "data" / "train"
TEST_PATH = ARTIFACT_DIR / "data" / "test"

MODEL_PATH = ARTIFACT_DIR / "trip_duration_model.pkl"
METRICS_PATH = ARTIFACT_DIR / "trip_duration_model_metrics.json"
FEATURES_PATH = ARTIFACT_DIR / "trip_duration_model_features.json"

TARGET = "label"

CATEGORICAL_FEATURES = [
    "pickup_location_id",
    "dropoff_location_id",
    "rate_code_id",
    "pickup_day_of_week_num",
]

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

FEATURE_COLUMNS = CATEGORICAL_FEATURES + NUMERIC_FEATURES


def read_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Dataset not found: {path}. "
            "Export it from HDFS before training."
        )

    df = pd.read_parquet(path)

    missing = sorted(
        set(FEATURE_COLUMNS + [TARGET]) - set(df.columns)
    )

    if missing:
        raise ValueError(
            "Missing required columns: " + ", ".join(missing)
        )

    return df[FEATURE_COLUMNS + [TARGET]].copy()


def prepare_x(df: pd.DataFrame) -> pd.DataFrame:
    x = df[FEATURE_COLUMNS].copy()

    # IDs are categories, not continuous quantities.
    for col in CATEGORICAL_FEATURES:
        x[col] = x[col].astype("string")

    return x


def main():
    print("=" * 76)
    print("NYC TAXI - PORTABLE TRIP DURATION MODEL")
    print("=" * 76)

    train = read_dataset(TRAIN_PATH)
    test = read_dataset(TEST_PATH)

    x_train = prepare_x(train)
    y_train = train[TARGET].astype(float)

    x_test = prepare_x(test)
    y_test = test[TARGET].astype(float)

    print(f"Training rows : {len(train):,}")
    print(f"Test rows     : {len(test):,}")

    categorical_encoder = TargetEncoder(
        target_type="continuous",
        smooth="auto",
        cv=5,
        shuffle=True,
        random_state=42,
    )

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "categorical",
                categorical_encoder,
                CATEGORICAL_FEATURES,
            ),
            (
                "numeric",
                "passthrough",
                NUMERIC_FEATURES,
            ),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    regressor = HistGradientBoostingRegressor(
        loss="squared_error",
        learning_rate=0.08,
        max_iter=250,
        max_leaf_nodes=31,
        min_samples_leaf=40,
        l2_regularization=0.15,
        early_stopping=True,
        validation_fraction=0.1,
        n_iter_no_change=20,
        random_state=42,
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("regressor", regressor),
        ]
    )

    print("\nTraining portable gradient-boosting model...")
    started = time.time()
    model.fit(x_train, y_train)
    training_seconds = time.time() - started

    print("Evaluating on unseen 2025-12 test data...")
    predictions = model.predict(x_test)

    mae = mean_absolute_error(y_test, predictions)
    rmse = mean_squared_error(
        y_test,
        predictions,
        squared=False,
    )
    r2 = r2_score(y_test, predictions)

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    joblib.dump(
        model,
        MODEL_PATH,
        compress=3,
    )

    metrics = {
        "model_type": "HistGradientBoostingRegressor",
        "artifact": MODEL_PATH.name,
        "training_rows": int(len(train)),
        "test_rows": int(len(test)),
        "mae_minutes": float(mae),
        "rmse_minutes": float(rmse),
        "r2": float(r2),
        "training_seconds": float(training_seconds),
        "test_period": "2025-12",
    }

    METRICS_PATH.write_text(
        json.dumps(metrics, indent=2),
        encoding="utf-8",
    )

    feature_metadata = {
        "target": "trip_duration_minutes",
        "categorical_features": CATEGORICAL_FEATURES,
        "numeric_features": NUMERIC_FEATURES,
        "prediction_inputs": [
            "pickup_location_id",
            "dropoff_location_id",
            "passenger_count",
            "estimated_trip_distance",
            "rate_code_id",
            "pickup_datetime",
        ],
    }

    FEATURES_PATH.write_text(
        json.dumps(feature_metadata, indent=2),
        encoding="utf-8",
    )

    # Prove that the serialized artifact can be reloaded.
    reloaded = joblib.load(MODEL_PATH)
    smoke = reloaded.predict(x_test.head(1))

    print("\n" + "=" * 76)
    print("DEPLOYMENT MODEL RESULT")
    print("=" * 76)
    print(f"MAE  : {mae:.4f} minutes")
    print(f"RMSE : {rmse:.4f} minutes")
    print(f"R2   : {r2:.4f}")
    print(f"Train: {training_seconds:.1f} seconds")
    print(f"Model: {MODEL_PATH}")
    print(
        "Reload smoke-test prediction: "
        f"{float(smoke[0]):.2f} minutes"
    )
    print("\nPORTABLE MODEL TRAINING COMPLETE")


if __name__ == "__main__":
    main()
