import pandas as pd
import plotly.express as px
import streamlit as st

from utils import apply_page_style, load_gold_table


st.set_page_config(
    page_title="NYC Yellow Taxi Analytics",
    page_icon="🚕",
    layout="wide",
)

apply_page_style()

st.title("🚕 NYC Yellow Taxi Analytics")
st.caption(
    "Historical analytics from the Gold layer. "
    "Use the sidebar pages for maps, demand patterns, and ML prediction."
)

try:
    daily = load_gold_table("daily_summary")
except Exception as exc:
    st.error(str(exc))
    st.stop()

daily["pickup_date"] = pd.to_datetime(daily["pickup_date"])
daily = daily.sort_values("pickup_date")

min_date = daily["pickup_date"].min().date()
max_date = daily["pickup_date"].max().date()

date_range = st.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

filtered = daily[
    (daily["pickup_date"].dt.date >= start_date)
    & (daily["pickup_date"].dt.date <= end_date)
].copy()

if filtered.empty:
    st.warning("No data for the selected date range.")
    st.stop()

total_trips = int(filtered["trip_count"].sum())
total_revenue = float(filtered["total_revenue"].sum())

weighted_duration = (
    (filtered["avg_trip_duration_minutes"] * filtered["trip_count"]).sum()
    / filtered["trip_count"].sum()
)

weighted_distance = (
    (filtered["avg_trip_distance"] * filtered["trip_count"]).sum()
    / filtered["trip_count"].sum()
)

def format_currency(value):
    if value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value / 1_000:.2f}K"
    else:
        return f"${value:,.0f}"


c1, c2, c3, c4 = st.columns(4)

c1.metric(
    "Trips",
    f"{total_trips:,.0f}"
)

c2.metric(
    "Revenue",
    format_currency(total_revenue)
)

c3.metric(
    "Avg trip duration",
    f"{weighted_duration:.1f} min"
)

c4.metric(
    "Avg trip distance",
    f"{weighted_distance:.2f} mi"
)

left, right = st.columns(2)

with left:
    fig = px.line(
        filtered,
        x="pickup_date",
        y="trip_count",
        title="Daily Trip Volume",
    )
    fig.update_layout(xaxis_title="", yaxis_title="Trips")
    st.plotly_chart(fig, use_container_width=True)

with right:
    fig = px.line(
        filtered,
        x="pickup_date",
        y="total_revenue",
        title="Daily Revenue",
    )
    fig.update_layout(xaxis_title="", yaxis_title="Revenue ($)")
    st.plotly_chart(fig, use_container_width=True)

st.info(
    "Open **Zone Map** from the sidebar for geographic analysis, "
    "or **Trip Duration Predictor** for the ML feature."
)
