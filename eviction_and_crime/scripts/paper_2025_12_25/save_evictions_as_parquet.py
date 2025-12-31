"""
Save evictions data as parquet
"""

import polars as pl
from eviction_and_crime.constants import (
    CLEAN_GEOCODED_EVICTIONS_WITH_STANDARDIZED_ADDRESSES_PARQUET,
    CLEAN_GEOCODED_EVICTIONS_WITH_STANDARDIZED_ADDRESSES,
)

df = pl.read_csv(
    CLEAN_GEOCODED_EVICTIONS_WITH_STANDARDIZED_ADDRESSES,
    schema_overrides={
        "plaintiff_atty_address_zip": pl.String,
        "Geocodio Postal Code": pl.String,
    },
)


df_with_correct_dtypes = df.with_columns(
    pl.col("file_date").str.to_date(format="%m/%d/%Y"),
    pl.col("latest_docket_date").str.to_date(format="%m/%d/%Y"),
    pl.col("Geocodio Postal Code").str.zfill(5),
)

df_with_correct_dtypes.write_parquet(
    CLEAN_GEOCODED_EVICTIONS_WITH_STANDARDIZED_ADDRESSES_PARQUET
)
