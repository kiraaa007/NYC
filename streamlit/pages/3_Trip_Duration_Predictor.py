from datetime import date, datetime, time
import json
import math
import os
from pathlib import Path

import joblib
import pandas as pd
import streamlit as st

from utils import apply_page_style, load_zone_lookup


st.set_page_config(
    page_title="Trip Duration Predictor",
    page_icon="🤖",
    layout="wide",
)

apply_page_style()

st.title("🤖 Trip Duration Predictor")
st.caption(
    "This page loads the trained model artifact directly and predicts "
    "the duration of a planned Yellow Taxi trip."
)

# Resolve paths relative to the GitHub project.
# pages/ -> streamlit/ -> project root
PAGE_DIR = Path(__file__).resolve().parent
STREAMLIT_DIR = PAGE_DIR.parent
PROJECT_ROOT = STREAMLIT_DIR.parent

MODEL_PATH = Path(
    os.getenv(
        "TRIP_DURATION_MODEL_PATH",
        str(
            PROJECT_ROOT
            / "ml_artifacts"
            / "trip_duration_model.pkl"
        ),
    )
)

METRICS_PATH = Path(
    os.getenv(
        "TRIP_DURATION_METRICS_PATH",
        str(
            PROJECT_ROOT
            / "ml_artifacts"
            / "trip_duration_model_metrics.json"
        ),
    )
)


@st.cache_resource(show_spinner="Loading trip-duration model...")
def load_model():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model artifact not found: {MODEL_PATH}. "
            "Train the deployment model first."
        )

    return joblib.load(MODEL_PATH)


@st.cache_data(show_spinner=False)
def load_model_metrics():
    if not METRICS_PATH.exists():
        return None

    return json.loads(
        METRICS_PATH.read_text(encoding="utf-8")
    )


def create_feature_row(
    pickup_location_id: int,
    dropoff_location_id: int,
    passenger_count: int,
    trip_distance: float,
    rate_code_id: int,
    pickup_dt: datetime,
) -> pd.DataFrame:
    day_num = pickup_dt.weekday() + 1

    is_weekend = 1 if day_num in (6, 7) else 0

    is_rush_hour = (
        1
        if pickup_dt.hour in (7, 8, 9, 16, 17, 18, 19)
        else 0
    )

    hour_angle = 2.0 * math.pi * pickup_dt.hour / 24.0

    month_angle = (
        2.0 * math.pi * (pickup_dt.month - 1) / 12.0
    )

    row = {
        "pickup_location_id": str(int(pickup_location_id)),
        "dropoff_location_id": str(int(dropoff_location_id)),
        "rate_code_id": str(int(rate_code_id)),
        "pickup_day_of_week_num": str(int(day_num)),
        "passenger_count": int(passenger_count),
        "trip_distance": float(trip_distance),
        "pickup_hour": int(pickup_dt.hour),
        "pickup_month": int(pickup_dt.month),
        "is_weekend": int(is_weekend),
        "is_rush_hour": int(is_rush_hour),
        "pickup_hour_sin": math.sin(hour_angle),
        "pickup_hour_cos": math.cos(hour_angle),
        "pickup_month_sin": math.sin(month_angle),
        "pickup_month_cos": math.cos(month_angle),
    }

    return pd.DataFrame([row])


try:
    lookup = load_zone_lookup()
except Exception as exc:
    st.error(str(exc))
    st.stop()

column_map = {}

for col in lookup.columns:
    low = col.lower()

    if low == "locationid":
        column_map[col] = "LocationID"
    elif low == "borough":
        column_map[col] = "Borough"
    elif low == "zone":
        column_map[col] = "Zone"

lookup = lookup.rename(columns=column_map)

if not {"LocationID", "Zone"}.issubset(lookup.columns):
    st.error(
        "Taxi lookup must contain LocationID and Zone columns."
    )
    st.stop()

lookup["LocationID"] = pd.to_numeric(
    lookup["LocationID"],
    errors="coerce",
).astype("Int64")

lookup = lookup.dropna(
    subset=["LocationID", "Zone"]
).copy()

if "Borough" not in lookup.columns:
    lookup["Borough"] = ""

lookup["display_name"] = (
    lookup["Borough"].fillna("").astype(str)
    + " — "
    + lookup["Zone"].astype(str)
)

lookup = (
    lookup
    .sort_values(["Borough", "Zone", "LocationID"])
    .reset_index(drop=True)
)

display_to_id = dict(
    zip(
        lookup["display_name"],
        lookup["LocationID"].astype(int),
    )
)

metrics = load_model_metrics()

if metrics:
    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Test MAE",
        f"{metrics['mae_minutes']:.2f} min",
    )
    c2.metric(
        "Test RMSE",
        f"{metrics['rmse_minutes']:.2f} min",
    )
    c3.metric(
        "Test R²",
        f"{metrics['r2']:.3f}",
    )

st.divider()

left, right = st.columns(2)

with left:
    pickup_name = st.selectbox(
        "Pickup zone",
        lookup["display_name"].tolist(),
    )

    dropoff_name = st.selectbox(
        "Dropoff zone",
        lookup["display_name"].tolist(),
        index=min(1, len(lookup) - 1),
    )

    passenger_count = st.slider(
        "Passengers",
        min_value=1,
        max_value=6,
        value=1,
    )

    trip_distance = st.number_input(
        "Estimated trip distance (miles)",
        min_value=0.1,
        max_value=100.0,
        value=3.0,
        step=0.1,
    )

with right:
    pickup_date = st.date_input(
        "Pickup date",
        value=date.today(),
    )

    pickup_time = st.time_input(
        "Pickup time",
        value=time(hour=12, minute=0),
    )

    rate_codes = {
        "Standard rate": 1,
        "JFK": 2,
        "Newark": 3,
        "Nassau / Westchester": 4,
        "Negotiated fare": 5,
        "Group ride": 6,
    }

    rate_label = st.selectbox(
        "Rate code",
        list(rate_codes.keys()),
    )

st.divider()

if st.button(
    "Predict trip duration",
    type="primary",
    use_container_width=True,
):
    try:
        model = load_model()

        pickup_dt = datetime.combine(
            pickup_date,
            pickup_time,
        )

        model_input = create_feature_row(
            pickup_location_id=display_to_id[pickup_name],
            dropoff_location_id=display_to_id[dropoff_name],
            passenger_count=passenger_count,
            trip_distance=trip_distance,
            rate_code_id=rate_codes[rate_label],
            pickup_dt=pickup_dt,
        )

        prediction = float(
            model.predict(model_input)[0]
        )

        st.success("Prediction complete")

        st.metric(
            "Estimated trip duration",
            f"{prediction:.1f} minutes",
        )

        st.caption(
            "The model uses the selected zones, estimated distance, "
            "passenger count, rate code, and pickup-time features."
        )

    except Exception as exc:
        st.error(
            "Prediction failed. "
            f"Details: {exc}"
        )
