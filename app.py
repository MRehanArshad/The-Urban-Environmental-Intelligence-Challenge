import streamlit as st
import polars as pl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import contextily as ctx

from config.config import DATADIR

# -------------------------------
# Page config
# -------------------------------
st.set_page_config(layout="wide", page_title="Urban Environmental Intelligence Dashboard")

st.title("🌆 Urban Environmental Intelligence Dashboard")

# -------------------------------
# Load Data
# -------------------------------
wide_df = pl.read_parquet(DATADIR / "pca_scores.parquet")
loadings = pd.read_csv(DATADIR / "pca_loadings.csv")
explained = pd.read_csv(DATADIR / "pca_explained_variance.csv")
heat_map = pl.read_csv(DATADIR / "heatmap.csv")
df = pl.read_csv(DATADIR / "openaq_2025.csv")

pdf = wide_df.to_pandas()

# -------------------------------
# Sidebar Filters
# -------------------------------
st.sidebar.header("Filters")
unique_tz = pdf["timezone"].unique()
selected_tz = st.sidebar.multiselect(
    "Select Timezone", options=unique_tz, default=unique_tz
)
filtered = pdf[pdf["timezone"].isin(selected_tz)]

# -------------------------------
# Tabs for sections
# -------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "Dimensionality Reduction (PCA)", 
    "Temporal Analysis", 
    "Distribution Modeling", 
    "Visual Integrity Audit"
])

# -------------------------------
# Tab 1: PCA Analysis
# -------------------------------
with tab1:
    st.subheader("PCA Scatter Plot")
    fig, ax = plt.subplots()
    for tz in filtered["timezone"].unique():
        subset = filtered[filtered["timezone"] == tz]
        ax.scatter(subset["PC1"], subset["PC2"], label=tz, alpha=0.7)
    ax.set_xlabel("PC1")
    ax.set_ylabel("PC2")
    ax.set_title("Environmental Variables Projection")
    ax.legend()
    st.pyplot(fig)

    st.subheader("Component Loadings")
    fig2, ax2 = plt.subplots(figsize=(8,5))
    x = np.arange(len(loadings))
    width = 0.35
    ax2.bar(x - width/2, loadings["PC1"].round(2), width, label="PC1")
    ax2.bar(x + width/2, loadings["PC2"].round(2), width, label="PC2")
    ax2.set_xticks(x)
    ax2.set_xticklabels(loadings.index, rotation=45, ha='right')
    ax2.set_ylabel("Loading Value")
    ax2.set_title("Feature Contribution to Components")
    ax2.legend()
    st.pyplot(fig2)

    st.subheader("Explained Variance")
    fig3, ax3 = plt.subplots()
    ax3.bar(explained["component"], explained["explained_variance_ratio"])
    ax3.set_ylabel("Explained Variance Ratio")
    ax3.set_title("Variance Captured by Each Component")
    st.pyplot(fig3)

# -------------------------------
# Tab 2: High-Density Temporal Analysis
# -------------------------------
with tab2:
    st.subheader("PM2.5 Health Threshold Violation Heatmap")
    heat_map = heat_map.fill_null(value=0)
    matrix = heat_map.select(pl.exclude("location_id")).to_numpy()
    fig4, ax4 = plt.subplots(figsize=(12,6))
    im = ax4.imshow(matrix, aspect="auto")
    fig4.colorbar(im, ax=ax4, label="Violation Intensity")
    ax4.set_xlabel("Time")
    ax4.set_ylabel("Sensor Index")
    ax4.set_title("PM2.5 Health Threshold Violation Heatmap")
    st.pyplot(fig4)

# -------------------------------
# Tab 3: Distribution Modeling & Tail Integrity
# -------------------------------
with tab3:
    idf = df.filter(pl.col("parameter") == "pm25").select("value").drop_nulls()
    values = idf["value"].to_numpy()

    # Columns for hist & CCDF side-by-side
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Log Frequency Distribution")
        fig5, ax5 = plt.subplots()
        ax5.hist(values, bins=100)
        ax5.set_yscale("log")
        ax5.set_xlabel("PM2.5")
        ax5.set_ylabel("Log Frequency")
        ax5.set_title("PM2.5 Distribution")
        st.pyplot(fig5)

    with col2:
        st.subheader("Complementary Cumulative Distribution Function")
        fig6, ax6 = plt.subplots()
        sorted_vals = np.sort(values)
        ccdf = 1.0 - np.arange(1, len(sorted_vals)+1) / len(sorted_vals)
        ax6.plot(sorted_vals, ccdf)
        ax6.set_yscale("log")
        ax6.set_xlabel("PM2.5")
        ax6.set_ylabel("P(X > x)")
        ax6.set_title("CCDF of PM2.5 (Tail Integrity)")
        st.pyplot(fig6)

    # Show key metrics
    p99 = np.percentile(values, 99)
    extreme_prob = np.mean(values > 200)
    st.metric("99th Percentile PM2.5", f"{p99:.2f}")
    st.metric("Probability PM2.5 > 200", f"{extreme_prob:.2%}")

# -------------------------------
# Tab 4: Visual Integrity Audit
# -------------------------------
with tab4:
    # --- Generate Random Data ---
    np.random.seed(42)
    regions = [f"Region {i}" for i in range(1, 11)]
    population_density = np.random.randint(50, 1000, size=10)
    pollution_index = np.random.randint(10, 200, size=10)

    df2 = pd.DataFrame({
        "Region": regions,
        "Population_Density": population_density,
        "Pollution_Index": pollution_index
    })

    # --- Plotting Small Multiples ---
    fig, axes = plt.subplots(2, 5, figsize=(20, 8), sharey=True)

    for ax, (_, row) in zip(axes.flatten(), df2.iterrows()):
        values = [row["Population_Density"], row["Pollution_Index"]]
        variables = ["Population Density", "Pollution Index"]
        
        # Normalize values for color mapping
        norm = plt.Normalize(min(values), max(values))
        
        ax.bar(variables, values)
        ax.set_title(row["Region"])
        ax.set_ylabel("")

    # Common labels
    fig.text(0.5, 0.04, 'Variables', ha='center', fontsize=14)
    fig.text(0.04, 0.5, 'Value', va='center', rotation='vertical', fontsize=14)
    fig.suptitle("Small Multiples: Population Density vs Pollution Index by Region", fontsize=16)
    plt.tight_layout(rect=[0.03, 0.03, 1, 0.95])

    st.pyplot(fig)

    # --- Show Data Table ---
    st.subheader("Data Table")
    st.dataframe(df2)

    st.subheader("PM2.5 levels by location")
    df_pol = (
        df.filter(pl.col("parameter") == "pm25")
        .group_by("location_id")
        .agg(
            pl.col("value").last().alias("pm25"),
            pl.col("longitude").first().alias("longitude"),
            pl.col("latitude").first().alias("latitude")
        )
    )
    pdf = pd.DataFrame(df_pol.to_dicts())
    geometry = [Point(xy) for xy in zip(pdf["longitude"], pdf["latitude"])]
    gdf = gpd.GeoDataFrame(pdf, geometry=geometry)
    gdf.set_crs(epsg=4326, inplace=True)
    gdf_web = gdf.to_crs(epsg=3857)

    fig7, ax7 = plt.subplots(figsize=(12,8))
    gdf_web.plot(column="pm25", cmap="viridis", legend=True, markersize=50, alpha=0.8, ax=ax7)
    ctx.add_basemap(ax7)
    ax7.set_title("PM2.5 levels by location")
    ax7.set_axis_off()
    st.pyplot(fig7)