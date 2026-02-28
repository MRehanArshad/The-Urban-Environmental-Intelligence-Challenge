import polars as pl

from  config.config import DATADIR

def analyze(df: pl.DataFrame):
    pm25_df = df.filter(pl.col("parameter") == "pm25")
    pm25_df = pm25_df.with_columns(
        (pl.col("value") > 35).cast(pl.Int8).alias("voilation")
    )

    pm25_df = (
        pm25_df
        .with_columns(pl.col("datetimeUtc").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ"))
        .sort(["location_id", "datetimeUtc"])
        .group_by_dynamic("datetimeUtc", every="1h", by="location_id")
        .agg(pl.col("voilation").mean().alias("voilation_rate"))
    )

    heatmap_df = pm25_df.pivot(
        values="voilation_rate",
        index="location_id",
        columns="datetimeUtc",
        aggregate_function="mean"
    )

    HEATMAP_DF = DATADIR / "heatmap.csv"

    heatmap_df.write_csv(HEATMAP_DF)

