from __future__ import annotations

import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import streamlit as st


DATA_DIR = Path(os.getenv("DASHBOARD_DATA_DIR", "/app/data"))
NYC_DATA_DIR = Path(os.getenv("NYC_DATA_DIR", "/data"))


@st.cache_data(show_spinner=False)
def load_gold_table(name: str) -> pd.DataFrame:
    path = DATA_DIR / name
    if not path.exists():
        raise FileNotFoundError(
            f"Dashboard data not found: {path}. "
            "Run export_dashboard_data.py first."
        )
    return pd.read_parquet(path)


@st.cache_data(show_spinner=False)
def load_zone_lookup() -> pd.DataFrame:
    candidates = [
        NYC_DATA_DIR / "taxi_zones" / "taxi_zone_lookup.csv",
        NYC_DATA_DIR / "taxi_zone_lookup.csv",
    ]

    for path in candidates:
        if path.exists():
            df = pd.read_csv(path)
            df.columns = [c.strip() for c in df.columns]
            return df

    raise FileNotFoundError(
        "taxi_zone_lookup.csv was not found under the mounted NYC data folder."
    )


@st.cache_data(show_spinner=False)
def load_taxi_zones() -> gpd.GeoDataFrame:
    candidates = [
        NYC_DATA_DIR / "taxi_zones" / "taxi_zones.shp",
        NYC_DATA_DIR / "taxi_zones" / "taxi_zone.shp",
    ]

    # Fallback: use any .shp in taxi_zones.
    zone_dir = NYC_DATA_DIR / "taxi_zones"
    if zone_dir.exists():
        candidates.extend(sorted(zone_dir.glob("*.shp")))

    shp = next((p for p in candidates if p.exists()), None)

    if shp is None:
        raise FileNotFoundError(
            "Taxi-zone shapefile was not found under /data/taxi_zones."
        )

    gdf = gpd.read_file(shp)

    id_col = None
    for candidate in ["LocationID", "locationid", "LOCATIONID", "OBJECTID"]:
        if candidate in gdf.columns:
            id_col = candidate
            break

    if id_col is None:
        raise ValueError(
            "Could not identify the taxi-zone ID column in the shapefile. "
            f"Columns: {list(gdf.columns)}"
        )

    gdf = gdf.rename(columns={id_col: "LocationID"})
    gdf["LocationID"] = pd.to_numeric(
        gdf["LocationID"], errors="coerce"
    ).astype("Int64")

    # Plotly works best with WGS84 longitude/latitude.
    if gdf.crs is not None:
        gdf = gdf.to_crs(epsg=4326)

    return gdf


def apply_page_style():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 1.4rem;
            padding-bottom: 2rem;
            max-width: 1500px;
        }
        div[data-testid="stMetric"] {
            border: 1px solid rgba(128,128,128,.22);
            border-radius: 14px;
            padding: 12px 14px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def month_name(month: int) -> str:
    return pd.Timestamp(year=2000, month=int(month), day=1).strftime("%B")
