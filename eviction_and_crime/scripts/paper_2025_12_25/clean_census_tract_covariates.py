"""
clean_census_tract_covariates.py

Cleans census tract level data from Opportunity Insights.
"""

import polars as pl
from eviction_and_crime.constants import (
    RAW_CENSUS_TRACT_COVARIATES,
    CLEAN_CENSUS_TRACT_COVARIATES,
)


# Load data.
tracts_df = pl.read_csv(RAW_CENSUS_TRACT_COVARIATES)

# Keep only rows corresponding to Massachusetts.
tracts_df = tracts_df.filter(pl.col("state") == 25)


# Generate a GEOID column.
tracts_df = tracts_df.with_columns(
    pl.col("county").cast(str).str.zfill(3).alias("county"),
    pl.col("tract").cast(str).str.zfill(6).alias("tract"),
).with_columns(
    (pl.col("state").cast(str) + pl.col("county") + pl.col("tract")).alias(
        "tract_geoid"
    )
)


# Save data.
columns_to_keep = [
    "tract_geoid",
    "med_hhinc2016",
    "popdensity2010",
    "share_white2010",
    "frac_coll_plus2010",
    "job_density_2013",
    "poor_share2010",
    "traveltime15_2010",
    "rent_twobed2015",
    "czname",
]
tracts_df.select(pl.col(columns_to_keep)).write_parquet(CLEAN_CENSUS_TRACT_COVARIATES)
