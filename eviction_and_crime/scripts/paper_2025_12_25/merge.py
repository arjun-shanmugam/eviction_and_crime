"""
Create merged dataset for analysis.
"""

import polars as pl
from eviction_and_crime.constants import (
    TREATMENT_MONTH_VARIABLE,
    CLEAN_GEOCODED_EVICTIONS_WITH_STANDARDIZED_ADDRESSES_PARQUET,
)

# INFO: Prepare eviction data
evictions_df = pl.read_parquet(
    CLEAN_GEOCODED_EVICTIONS_WITH_STANDARDIZED_ADDRESSES_PARQUET
)


# Generate a variable indicating judgment in favor of defendant.
# Start by flagging all cases where the disposition indicates a judgment for the defendant as such,
# and assume otherwise that the judgment is for the plaintiff.
evictions_df = evictions_df.with_columns(
    (
        (pl.col("disposition_found") == "Dismissed").fill_null(False)
        | (pl.col("judgment_for_pdu") == "Defendant").fill_null(False)
        | (
            pl.col("disposition").str.contains("R 41(a)(1) Voluntary Dismissal")
        ).fill_null(False)
    )
    .cast(pl.Int8)
    .alias("judgment_for_defendant")
)


# Now, set judgment_for_defendant to missing for certain cases.
mediated_case = pl.col("disposition_found") == "Mediated"
disposition_found_other = pl.col("disposition_found") == "Other"
disposition_found_heard_but_no_judgment = (
    (pl.col("disposition_found") == "Heard") & (pl.col("judgment_for_pdu") == "Unknown")
) | ((pl.col("disposition_found") == "Heard") & (pl.col("judgment_for_pdu").is_null()))
missing_disposition_found = pl.col("disposition_found").is_null()
disposition_judgment_mismatch = (
    (pl.col("disposition_found") == "Defaulted")
    & (pl.col("judgment_for_pdu") == "Defendant")
) | (
    (pl.col("disposition_found") == "Dismissed")
    & (pl.col("judgment_for_pdu") == "Plaintiff")
)
set_to_missing_condition = pl.any_horizontal(
    [
        mediated_case,
        disposition_found_other,
        disposition_found_heard_but_no_judgment,
        missing_disposition_found,
        disposition_judgment_mismatch,
    ]
)
evictions_df = evictions_df.with_columns(
    pl.when(set_to_missing_condition)
    .then(None)
    .otherwise(pl.col("judgment_for_defendant"))
    .alias("judgment_for_defendant")
)


evictions_df = evictions_df.with_columns(
    [(1 - pl.col("judgment_for_defendant")).alias("judgment_for_plaintiff")]
)

evictions_df.select(pl.col(TREATMENT_MONTH_VARIABLE)).unique().sort()
