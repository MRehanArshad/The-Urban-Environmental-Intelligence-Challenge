import streamlit as st
import polars as pl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from config.config import DATADIR

st.set_page_config(layout="wide")

st.title("Urban Environmental Intelligence Dashboard")


st.header("Dimensioality Reduction Analysis (PCA)")

wide_df = pl.read_parquet(DATADIR / "pca_scores.parquet")
loadings = pd.read_csv(DATADIR / "pca_loadings.csv")
explained = pd.read_csv(DATADIR / "pca_explained_variance.csv")

pdf = wide_df.to_pandas()

st.sidebar.header("Filters")

unique_tz = pdf["timezone"].unique()
selected_tz = st.sidebar.multiselect(
    "Select Timezone",
    options=unique_tz,
    default=unique_tz
)

filtered = pdf[pdf["timezone"].isin(selected_tz)]

st.subheader("PCA Scatter Plot")

fig, ax = plt.subplots()

for tz in filtered["timezone"].unique():
    subset=filtered[filtered["timezone"] == tz]
    ax.scatter(
        subset["PC1"],
        subset["PC2"],
        label=tz,
        alpha=0.7
    )

ax.set_xlabel("PC1")
ax.set_ylabel("PC2")
ax.set_title("Environmental Variables Projection")
ax.legend()

st.pyplot(fig)

st.subheader("Explained Variance")

fig3, ax3 = plt.subplots()

ax3.bar(
    explained["component"],
    explained["explained_variance_ratio"]
)

ax3.set_ylabel("Explained Variance Ratio")
ax3.set_title("Variance Captured by Each Component")

st.pyplot(fig3)

