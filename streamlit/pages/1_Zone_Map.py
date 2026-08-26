import pandas as pd
import plotly.express as px
import streamlit as st

from utils import (
    apply_page_style,
    load_gold_table,
    load_taxi_zones,
    month_name,
)


st.set_page_config(
    page_title="NYC Taxi Zone Map",
    page_icon="🗺️",
    layout="wide",
)

apply_page_style()
st.title("🗺️ Taxi Zone Map")
st.caption(
    "Interactive choropleth built from the NYC TLC taxi-zone shapefile "
    "and the Gold pickup-zone monthly aggregates."
)

try:
    zone_monthly = load_gold_table("pickup_zone_monthly")
    zones = load_taxi_zones()
except Exception as exc:
    st.error(str(exc))
    st.stop()

zone_monthly["pickup_year"] = pd.to_numeric(
    zone_monthly["pickup_year"], errors="coerce"
).astype("Int64")
zone_monthly["pickup_month"] = pd.to_numeric(
    zone_monthly["pickup_month"], errors="coerce"
).astype("Int64")
zone_monthly["pickup_location_id"] = pd.to_numeric(
    zone_monthly["pickup_location_id"], errors="coerce"
).astype("Int64")

years = sorted(zone_monthly["pickup_year"].dropna().unique().tolist())
selected_year = st.selectbox("Year", years, index=len(years) - 1)

months_available = sorted(
    zone_monthly.loc[
        zone_monthly["pickup_year"] == selected_year,
        "pickup_month",
    ]
    .dropna()
    .unique()
    .tolist()
)

selected_month = st.selectbox(
    "Month",
    months_available,
    index=len(months_available) - 1,
    format_func=month_name,
)

metric_options = {
    "Trip count": "trip_count",
    "Total revenue": "total_revenue",
    "Average trip duration": "avg_trip_duration_minutes",
    "Average trip distance": "avg_trip_distance",
    "Average tip": "avg_tip_amount",
}

metric_label = st.selectbox(
    "Map metric",
    list(metric_options.keys()),
)
metric = metric_options[metric_label]

month_df = zone_monthly[
    (zone_monthly["pickup_year"] == selected_year)
    & (zone_monthly["pickup_month"] == selected_month)
].copy()

if month_df.empty:
    st.warning("No data for this year/month.")
    st.stop()

# A defensive aggregate in case the Gold table contains more than one row
# for a location due to additional grouping columns.
agg_map = {
    "trip_count": "sum",
    "total_revenue": "sum",
    "avg_trip_duration_minutes": "mean",
    "avg_trip_distance": "mean",
    "avg_tip_amount": "mean",
}

group_cols = ["pickup_location_id"]

for optional in ["pickup_zone", "pickup_borough"]:
    if optional in month_df.columns:
        group_cols.append(optional)

month_df = (
    month_df.groupby(group_cols, dropna=False, as_index=False)
    .agg(agg_map)
)

merged = zones.merge(
    month_df,
    left_on="LocationID",
    right_on="pickup_location_id",
    how="left",
)

merged[metric] = pd.to_numeric(
    merged[metric], errors="coerce"
).fillna(0)

if "pickup_zone" not in merged.columns:
    merged["pickup_zone"] = "Taxi Zone"

if "pickup_borough" not in merged.columns:
    merged["pickup_borough"] = ""

# Plotly will key geometries using LocationID stored in the GeoJSON properties.
geojson = merged.__geo_interface__

fig = px.choropleth_mapbox(
    merged,
    geojson=geojson,
    locations="LocationID",
    featureidkey="properties.LocationID",
    color=metric,
    hover_name="pickup_zone",
    hover_data={
        "pickup_borough": True,
        "LocationID": True,
        metric: ":,.2f",
    },
    mapbox_style="carto-positron",
    center={"lat": 40.7128, "lon": -74.0060},
    zoom=9.2,
    opacity=0.70,
    title=f"{metric_label} — {month_name(selected_month)} {selected_year}",
)

fig.update_layout(
    margin={"r": 0, "t": 45, "l": 0, "b": 0},
    height=720,
)

st.plotly_chart(fig, use_container_width=True)

top = (
    month_df.sort_values(metric, ascending=False)
    .head(10)
    .copy()
)

display_cols = [
    c
    for c in [
        "pickup_zone",
        "pickup_borough",
        "pickup_location_id",
        metric,
    ]
    if c in top.columns
]

st.subheader(f"Top zones by {metric_label.lower()}")
st.dataframe(
    top[display_cols],
    use_container_width=True,
    hide_index=True,
)
