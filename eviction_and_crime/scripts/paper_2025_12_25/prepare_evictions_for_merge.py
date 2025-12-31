"""
Create merged dataset for analysis.
"""

import polars as pl
import os
from eviction_and_crime.figure_utilities import add_column_numbers
from eviction_and_crime.constants import (
    COVID_TREATMENT_DATES,
    VERBOSE,
    CLEAN_GEOCODED_EVICTIONS_WITH_STANDARDIZED_ADDRESSES_PARQUET,
    OUTPUT_PAPER_2025_12_25_TABLES,
    EVICTIONS_PRE_MERGE,
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


# Restrict sample
sample_restriction_table_index = []
sample_restriction_table_observations = []
sample_restriction_table_forced_moves = []

# Restrict to cases in Boston with non-missing initiating action.
boston_mask = (pl.col("Geocodio County") == "Suffolk County") & pl.col(
    "Geocodio City"
).is_in(["Chelsea", "Revere", "Winthrop"]).not_()
evictions_df = evictions_df.filter(boston_mask)
original_N = len(evictions_df)

if VERBOSE:
    print(f"Beginning with {original_N} observations.")


sample_restriction_table_index.append(
    "Case Concluded in Boston with non-missing initiating action"
)
sample_restriction_table_observations.append(original_N)
sample_restriction_table_forced_moves.append(
    evictions_df.select(pl.col("judgment_for_plaintiff").sum()).item()
)


# Add standardized address column
evictions_df = evictions_df.with_columns(
    (
        pl.col("Geocodio House Number").cast(pl.String)
        + " "
        + pl.col("Geocodio Street")
        + " "
        + pl.col("Geocodio Postal Code").cast(pl.String)
    )
    .str.to_lowercase()
    .alias("standardized_address")
)

evictions_df_all_boston_cases = evictions_df


mask = pl.col("initiating_action") == "SP Summons and Complaint - Foreclosure"
if VERBOSE:
    print(
        f"Dropping {evictions_df.select(mask.sum()).item()} cases for which the initiating action is foreclosure"
    )
evictions_df = evictions_df.filter(mask.not_())

sample_restriction_table_index.append(
    "Case initiated for reason other than foreclosure"
)
sample_restriction_table_observations.append(len(evictions_df))
sample_restriction_table_forced_moves.append(
    evictions_df.select(pl.col("judgment_for_plaintiff").sum()).item()
)


# Drop cases where disposition found is other.
disposition_found_other_mask = (pl.col("disposition_found") == "Other") | (
    pl.col("disposition_found").is_null()
)
if VERBOSE:
    print(
        f'Dropping {evictions_df.select(disposition_found_other_mask.sum()).item()} cases where disposition_found is "Other" or missing.'
    )
evictions_df = evictions_df.filter(disposition_found_other_mask.not_())
sample_restriction_table_index.append("Cases for which disposition could be scraped")
sample_restriction_table_observations.append(len(evictions_df))
sample_restriction_table_forced_moves.append(
    evictions_df.select(pl.col("judgment_for_plaintiff").sum()).item()
)


# Drop cases resolved via mediation.
mediated_mask = pl.col("disposition_found") == "Mediated"
if VERBOSE:
    print(
        f"Dropping {evictions_df.select(mediated_mask.sum()).item()} cases resolved through mediation."
    )
evictions_df = evictions_df.filter(mediated_mask.not_())
sample_restriction_table_index.append("Case not resolved through mediation")
sample_restriction_table_observations.append(len(evictions_df))
sample_restriction_table_forced_moves.append(
    evictions_df.select(pl.col("judgment_for_plaintiff").sum()).item()
)


# Drop cases where defendant is an entity.
defendant_is_entity_mask = pl.col("isEntityD") == 1
if VERBOSE:
    print(
        f"Dropping {evictions_df.select(defendant_is_entity_mask.sum()).item()} cases which were filed against defendants who are entities"
    )
evictions_df = evictions_df.filter(defendant_is_entity_mask.not_())
sample_restriction_table_index.append("Defendant is an individual and not an entity")
sample_restriction_table_observations.append(len(evictions_df))
sample_restriction_table_forced_moves.append(
    evictions_df.select(pl.col("judgment_for_plaintiff").sum()).item()
)


# Drop cases where defendant has an attorney.
defendant_has_attorney_mask = evictions_df["hasAttyD"] == 1
evictions_df = evictions_df.filter(defendant_has_attorney_mask.not_())
if VERBOSE:
    print(
        f"Dropping {evictions_df.select(defendant_has_attorney_mask.sum()).item()} cases which were filed against defendants with attorneys"
    )
sample_restriction_table_index.append("Defendant has no attorney")
sample_restriction_table_observations.append(len(evictions_df))
sample_restriction_table_forced_moves.append(
    evictions_df.select(pl.col("judgment_for_plaintiff").sum()).item()
)


sample_restriction_table = pl.DataFrame(
    {
        "Restriction": sample_restriction_table_index,
        "Observations": sample_restriction_table_observations,
        "Forced Move-Outs": sample_restriction_table_forced_moves,
    }
)
with pl.Config(fmt_str_lengths=1000):
    print(sample_restriction_table)


sample_restriction_table_pandas = sample_restriction_table.to_pandas()

sample_restriction_table_pandas = sample_restriction_table_pandas.set_index(
    "Restriction"
)

# Add column numbers
sample_restriction_table = add_column_numbers(sample_restriction_table_pandas)

# Export to LaTeX.
filename = os.path.join(OUTPUT_PAPER_2025_12_25_TABLES, "sample_restriction.tex")
sample_restriction_table.style.format(formatter="{:,.0f}").to_latex(
    filename,
    column_format="lcc",
    multicol_align="c",
    hrules=True,
    clines="skip-last;data",
)
sample_restriction_table


# TODO:
# Add flags indicating what sample each record corresponds to
evictions_df_all_boston_cases = evictions_df_all_boston_cases.with_columns(
    pl.when(
        pl.col("case_number").is_in(
            evictions_df.select(pl.col("case_number")).to_series().to_list()
        )
    )
    .then(pl.lit(1))
    .otherwise(0)
    .alias("main_analysis_sample"),
    pl.when(pl.col("disposition_found") == "Mediated")
    .then(pl.lit(1))
    .otherwise(0)
    .alias("mediated_sample"),
    pl.when(pl.col("latest_docket_month").is_in(COVID_TREATMENT_DATES))
    .then(1)
    .otherwise(0)
    .alias(
        "covid_sample",
    ),
)
evictions_df_all_boston_cases.write_parquet(EVICTIONS_PRE_MERGE)
