import pandas as pd
import plotly.express as px
import streamlit as st

from utils import apply_page_style, load_gold_table


st.set_page_config(
    page_title="Demand Patterns",
    page_icon="📈",
    layout="wide",
)

apply_page_style()
st.title("📈 Demand Patterns")

try:
    hourly = load_gold_table("hourly_demand")
except Exception as exc:
    st.error(str(exc))
    st.stop()

hourly["pickup_year"] = pd.to_numeric(
    hourly["pickup_year"], errors="coerce"
).astype("Int64")
hourly["pickup_month"] = pd.to_numeric(
    hourly["pickup_month"], errors="coerce"
).astype("Int64")

years = sorted(hourly["pickup_year"].dropna().unique().tolist())
year = st.selectbox("Year", years, index=len(years) - 1)

months = sorted(
    hourly.loc[hourly["pickup_year"] == year, "pickup_month"]
    .dropna()
    .unique()
    .tolist()
)
month = st.selectbox("Month", months, index=len(months) - 1)

df = hourly[
    (hourly["pickup_year"] == year)
    & (hourly["pickup_month"] == month)
].copy()

day_order = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

if "pickup_day_of_week" in df.columns:
    heat = df.pivot_table(
        index="pickup_day_of_week",
        columns="pickup_hour",
        values="trip_count",
        aggfunc="sum",
        fill_value=0,
    ).reindex(day_order)

    fig = px.imshow(
        heat,
        aspect="auto",
        labels={
            "x": "Pickup hour",
            "y": "Day of week",
            "color": "Trips",
        },
        title="Trips by Day of Week and Hour",
    )
    st.plotly_chart(fig, use_container_width=True)

hourly_curve = (
    df.groupby("pickup_hour", as_index=False)["trip_count"]
    .sum()
    .sort_values("pickup_hour")
)

fig = px.line(
    hourly_curve,
    x="pickup_hour",
    y="trip_count",
    markers=True,
    title="Hourly Pickup Demand",
)
fig.update_layout(xaxis_title="Hour", yaxis_title="Trips")
st.plotly_chart(fig, use_container_width=True)
