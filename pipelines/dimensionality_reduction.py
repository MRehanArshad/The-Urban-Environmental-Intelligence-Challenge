import polars as pl
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


from config.config import (
    DATADIR
)

def reduce(df: pl.DataFrame):
    wide_df = (
        df.lazy()
        .collect()
        .pivot(index=["datetimeUtc", "location_id", "timezone"], on="parameter", values="value", aggregate_function="first")
    )

    wide_df = wide_df.select(["datetimeUtc", "location_id", "timezone", "pm25", "pm10", "no2", "o3", "temperature", "relativehumidity"])

    wide_df = wide_df.fill_null(strategy="mean")

    X = wide_df.select(pl.exclude(["location_id", "datetimeUtc", "timezone"])).to_numpy()
    X_scaled = StandardScaler().fit_transform(X)

    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)

    wide_df = wide_df.with_columns([
        pl.Series("PC1", X_pca[:, 0]),
        pl.Series("PC2", X_pca[:, 1])
    ])

    PCA_SCORE_PATH = DATADIR / "pca_scores.parquet"
    wide_df.write_parquet(PCA_SCORE_PATH)

    loadings = pd.DataFrame(pca.components_.T,
                            index=["pm25", "pm10", "no2", "o3", "temperature", "relativehumidity"],
                            columns=["PC1", "PC2"])

    LOADING_DIR = DATADIR / "pca_loadings.csv"

    loadings.to_csv(
        LOADING_DIR,
        index = False
    )

    explained = pd.DataFrame({
        "component": ["PC1", "PC2"],
        "explained_variance_ratio": pca.explained_variance_ratio_
    })

    EXPLAINED_PATH = DATADIR / "pca_explained_variance.csv"
    explained.to_csv(EXPLAINED_PATH, index=False)

    return wide_df