import os
import subprocess
import tempfile
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


st.set_page_config(page_title="Orbital Conjunction Dashboard", layout="wide", page_icon="🛰️")


@st.cache_data(ttl=300)
def load_data(local_source: str) -> pd.DataFrame:
    if local_source:
        path = Path(local_source).expanduser()
        if path.is_dir():
            files = sorted(path.glob("*.parquet"))
            if not files:
                raise FileNotFoundError(f"No Parquet files found in {path}")
            return pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path)
        return pd.read_parquet(path)

    latest_marker = "hdfs:///user/bda/tle/latest.txt"
    result = subprocess.run(
        ["hdfs", "dfs", "-cat", latest_marker],
        capture_output=True,
        text=True,
        check=True,
    )
    latest_dir = result.stdout.strip()
    if not latest_dir:
        raise RuntimeError(f"{latest_marker} is empty")

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / "conjunctions"
        subprocess.run(["hdfs", "dfs", "-get", latest_dir, str(local_path)], check=True)
        files = sorted(local_path.glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No Parquet files found in {latest_dir}")
        return pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)


try:
    df = load_data(os.environ.get("CONJUNCTION_DATA_PATH", "").strip())
except Exception as exc:
    st.error(f"Unable to load conjunction data: {exc}")
    st.stop()

rename_map = {
    "obj1": "Object A ID",
    "obj2": "Object B ID",
    "obj1_name": "Object A",
    "obj2_name": "Object B",
    "obj1_type": "Object A Type",
    "obj2_type": "Object B Type",
    "obj1_country": "Object A Country",
    "obj2_country": "Object B Country",
    "closest_approach_time": "Closest Approach Time",
    "min_distance_km": "Min Distance (km)",
    "max_relative_velocity_km_s": "Max Relative Velocity (km/s)",
    "severity_rank": "Severity Rank",
}
df.rename(columns={key: value for key, value in rename_map.items() if key in df.columns}, inplace=True)

required_columns = [
    "Object A",
    "Object B",
    "Object A Type",
    "Object B Type",
    "Object A Country",
    "Object B Country",
    "Closest Approach Time",
    "Min Distance (km)",
    "Max Relative Velocity (km/s)",
    "Severity Rank",
]
missing_columns = [column for column in required_columns if column not in df.columns]
if missing_columns:
    st.error(f"Missing required columns: {missing_columns}")
    st.stop()

df["Object A Type"] = df["Object A Type"].astype(str).str.strip().str.upper()
df["Object B Type"] = df["Object B Type"].astype(str).str.strip().str.upper()
df["Object A Country"] = df["Object A Country"].fillna("").astype(str).str.strip().str.upper()
df["Object B Country"] = df["Object B Country"].fillna("").astype(str).str.strip().str.upper()
df["Closest Approach Time"] = pd.to_datetime(df["Closest Approach Time"], utc=True, errors="coerce")
df.dropna(subset=["Closest Approach Time", "Min Distance (km)", "Max Relative Velocity (km/s)"], inplace=True)
df["Combined Type"] = df.apply(
    lambda row: " & ".join(sorted([row["Object A Type"], row["Object B Type"]])),
    axis=1,
)
df["Risk Score"] = df["Severity Rank"].max() + 1 - df["Severity Rank"]

st.title("🛰️ Orbital Conjunction Dashboard")
st.sidebar.header("Filters")

object_types = sorted(set(df["Object A Type"]) | set(df["Object B Type"]))
selected_types = st.sidebar.multiselect("Object types", object_types, default=object_types)
search_text = st.sidebar.text_input("Search object")

minimum_date = df["Closest Approach Time"].min().date()
maximum_date = df["Closest Approach Time"].max().date()
selected_dates = st.sidebar.date_input(
    "Date range",
    value=(minimum_date, maximum_date),
    min_value=minimum_date,
    max_value=maximum_date,
)

maximum_distance = float(df["Min Distance (km)"].max())
distance_limit = st.sidebar.slider("Maximum distance (km)", 0.0, maximum_distance, maximum_distance)
maximum_velocity = float(df["Max Relative Velocity (km/s)"].max())
velocity_limit = st.sidebar.slider("Maximum relative velocity (km/s)", 0.0, maximum_velocity, maximum_velocity)
maximum_rank = int(df["Severity Rank"].max())
rank_limit = st.sidebar.slider("Maximum severity rank", 1, maximum_rank, maximum_rank)

countries = sorted(
    country
    for country in set(df["Object A Country"]) | set(df["Object B Country"])
    if country
)
selected_countries = st.sidebar.multiselect("Countries", countries, default=countries)
top_n = st.sidebar.selectbox("Display limit", ["All", 5, 10, 20, 50], index=0)

filtered = df[
    df["Object A Type"].isin(selected_types) | df["Object B Type"].isin(selected_types)
].copy()

if search_text:
    query = search_text.strip().lower()
    filtered = filtered[
        filtered["Object A"].str.lower().str.contains(query, na=False)
        | filtered["Object B"].str.lower().str.contains(query, na=False)
    ]

if isinstance(selected_dates, (tuple, list)) and len(selected_dates) == 2:
    start_date, end_date = selected_dates
else:
    start_date = end_date = selected_dates

filtered = filtered[
    (filtered["Closest Approach Time"].dt.date >= start_date)
    & (filtered["Closest Approach Time"].dt.date <= end_date)
    & (filtered["Min Distance (km)"] <= distance_limit)
    & (filtered["Max Relative Velocity (km/s)"] <= velocity_limit)
    & (filtered["Severity Rank"] <= rank_limit)
]

if selected_countries:
    filtered = filtered[
        filtered["Object A Country"].isin(selected_countries)
        | filtered["Object B Country"].isin(selected_countries)
    ]
else:
    filtered = filtered.iloc[0:0]

filtered.sort_values(["Severity Rank", "Min Distance (km)"], inplace=True)
if top_n != "All":
    filtered = filtered.head(int(top_n))

now = pd.Timestamp.now(tz="UTC")
upcoming = filtered[
    (filtered["Closest Approach Time"] >= now)
    & (filtered["Closest Approach Time"] <= now + pd.Timedelta(days=1))
]
satellites = pd.concat(
    [
        filtered.loc[filtered["Object A Type"] == "SATELLITE", "Object A"],
        filtered.loc[filtered["Object B Type"] == "SATELLITE", "Object B"],
    ]
).nunique()

metric_columns = st.columns(6)
metric_columns[0].metric("Conjunctions", len(filtered))
metric_columns[1].metric("Average distance", f"{filtered['Min Distance (km)'].mean():.3f} km" if not filtered.empty else "N/A")
metric_columns[2].metric("Average velocity", f"{filtered['Max Relative Velocity (km/s)'].mean():.3f} km/s" if not filtered.empty else "N/A")
metric_columns[3].metric("Minimum distance", f"{filtered['Min Distance (km)'].min():.3f} km" if not filtered.empty else "N/A")
metric_columns[4].metric("Upcoming in 24h", len(upcoming))
metric_columns[5].metric("Unique satellites", satellites)

st.dataframe(filtered.drop(columns=["Risk Score"]), use_container_width=True, hide_index=True)

if filtered.empty:
    st.info("No conjunctions match the selected filters.")
    st.stop()

st.plotly_chart(
    px.scatter(
        filtered,
        x="Min Distance (km)",
        y="Max Relative Velocity (km/s)",
        color="Combined Type",
        size="Risk Score",
        hover_data=["Object A", "Object B", "Closest Approach Time", "Severity Rank"],
        title="Distance vs relative velocity",
    ),
    use_container_width=True,
)

chart_columns = st.columns(2)
chart_columns[0].plotly_chart(
    px.histogram(filtered, x="Min Distance (km)", color="Combined Type", title="Minimum-distance distribution"),
    use_container_width=True,
)
chart_columns[1].plotly_chart(
    px.histogram(
        filtered,
        x="Max Relative Velocity (km/s)",
        color="Combined Type",
        title="Relative-velocity distribution",
    ),
    use_container_width=True,
)

object_counts = pd.concat(
    [
        filtered[["Object A"]].rename(columns={"Object A": "Object"}),
        filtered[["Object B"]].rename(columns={"Object B": "Object"}),
    ]
)["Object"].value_counts().head(20).rename_axis("Object").reset_index(name="Conjunction Count")
st.plotly_chart(
    px.bar(object_counts, x="Object", y="Conjunction Count", title="Most frequently involved objects"),
    use_container_width=True,
)
