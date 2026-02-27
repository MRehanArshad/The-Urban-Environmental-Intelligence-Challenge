import polars as pl

from config.config import (
    OUTPUT_FILE
)

def clean_data():
    lf = pl.read_csv(OUTPUT_FILE)

    lf = lf.filter(pl.col('value')>=0)

    lf = lf.select(['location_id', 'location_name', 'parameter', 'value', 'unit', 'datetimeUtc', 'timezone', 'latitude', 'longitude'])

    return lf