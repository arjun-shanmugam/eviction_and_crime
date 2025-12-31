"""
Clean raw evictions data
"""

from eviction_and_crime.constants import CLEAN_EVICTIONS, RAW_EVICTIONS

import polars as pl

schema_overrides = {
    "defendant_atty_address_zip": pl.String,
    "plaintiff_atty_address_zip": pl.String,
    "judgment": pl.String,
}
evictions_df = pl.read_csv(
    RAW_EVICTIONS, schema_overrides=schema_overrides, encoding="unicode_escape"
)


# Clean judgement column
evictions_df = evictions_df.with_columns(
    pl.col("judgment")
    .str.replace("$", "", literal=True)
    .str.replace("....edited", "", literal=True)
    .cast(pl.Float64)
)

# Clean court region column
court_division_replacement_dict = {
    "central": "Central",
    "eastern": "Eastern",
    "metro_south": "Metro South",
    "northeast": "Northeast",
    "southeast": "Southeast",
    "western": "Western",
}
evictions_df = evictions_df.with_columns(
    pl.col("court_division")
    .replace(court_division_replacement_dict)
    .str.replace("_", " ", literal=True)
)

# Clean initiating action column
initiating_action_replacement_dict = {
    "Efiled SP Summons and Complaint - Cause": "SP Summons and Complaint - Cause",
    "Efiled SP Summons and Complaint - Foreclosure": "SP Summons and Complaint - Foreclosure",
    "SP Summons and Complaint - Non-payment": "SP Summons and Complaint - Non-payment of Rent",
    "Efiled SP Summons and Complaint - Non-payment": "SP Summons and Complaint - Non-payment of Rent",
    "Efiled SP Summons and Complaint - Non-payment of Rent": "SP Summons and Complaint - Non-payment of Rent",
    "Efiled SP Summons and Complaint - No Cause": "SP Summons and Complaint - No Cause",
    "Poah Communities, Managing Agent For Poah Central Annex Preservation Associates, Lp": None,
    "SP Summons and Complaint - No Caus": "SP Summons and Complaint - No Cause",
    "SP Transfer - Foreclosure": "SP Summons and Complaint - Foreclosure",
    "Residential - Cause other than Non-payment of Rent": "SP Summons and Complaint - Cause",
    "SP Transfer - Non-payment of Rent": "SP Summons and Complaint - Non-payment of Rent",
    "SP Transfer - Cause": "SP Summons and Complaint - Cause",
    "Residential Non-Payment of Rent": "SP Summons and Complaint - Non-payment of Rent",
    "Summary Process Residential Non-payment of Rent": "SP Summons and Complaint - Non-payment of Rent",
    "Summary Process - Residential-Cause other than Non payment of rent.": "SP Summons and Complaint - Cause",
    "P Summons and Complaint - Cause": "SP Summons and Complaint - Cause",
    "Summary Process - Residential (c239)": "SP Summons and Complaint - Cause",
    "SP Transfer- No Cause": "SP Summons and Complaint - No Cause",
    "Tenant Illegal Activity - Declaratory Judgment c.139 s. 19": "SP Summons and Complaint - Cause",
    "Public Housing Tenant Illegal Activity Declaratory Judgment (c139 &#167;19)": "SP Summons and Complaint - Cause",
    "Public Housing Tenant Illegal Activity Declaratory Judgment (c139 §19)": "SP Summons and Complaint - Cause",
    "Public Housing Tenant Illegal Activity Declaratory Judgment (c139 Â§19)": "SP Summons and Complaint - Cause",
}
# TODO: Finish cleaning up initiating action column
initating_actions_to_exclude = [
    "Supplementary Process (c224 &#167;&#167; 14-21)",
    "Replevin (c247)",
    "Lien Enforcement Action (c254 &#167;5;255 &#167;26)",
    "Barnes, Esq., Vesper Gibbs",
    "Consumer Revolving Credit - M.R.C.P Rule 8.1",
    "Money Action - District Court Filing (c231 &#167;&#167; 103-104)",
    "Money Action - District Court Filing (c231 §§ 103-104)",
    "Case transferred from another court or has prior manual docket - no fee due",
]
evictions_df = evictions_df.with_columns(
    pl.col("initiating_action").replace(initiating_action_replacement_dict)
)
evictions_df = evictions_df.filter(
    ~pl.col("initiating_action").is_in(initating_actions_to_exclude)
)

evictions_df = evictions_df.with_columns(
    # Apostrophes represented as mojibake.
    pl.col("court_person").str.replace("&#039;", "", literal=True)
)


# Clean court person names
# TODO : Finish cleaning court person names
name_replacement_dict = {
    "David D Kerman": "David Kerman",
    "Del ": "Gustavo del Puerto",
    "Diana H": "Diana Horan",
    "Diana H Horan": "Diana Horan",
    "Fairlie A Dalton": "Fairlie Dalton",
    "Gustavo A": "Gustavo del Puerto",
    "Gustavo A Del Puerto": "Gustavo del Puerto",
    "III Joseph ": "Joseph Kelleher III",
    "III Kelleher": "Joseph Kelleher III",
    "Laura J Fenn": "Laura Fenn",
    "Laura J. Fenn": "Laura Fenn",
    "Michae Malamut": "Michael Malamut",
    "Michael J Doherty": "Michael Doherty",
    "Nickolas W Moudios": "Nickolas Moudios",
    "Nickolas W. Moudios": "Nickolas Moudios",
    "Robert G Fields": "Robert Fields",
    "Sergio E Carvajal": "Sergio Carvajal",
    "Timothy F Sullivan": "Timothy Sullivan",
    "on. Donna Salvidio": "Donna Salvidio",
    "Donna  Salvidio": "Donna Salvidio",
    "null": None,
}
evictions_df = evictions_df.with_columns(
    pl.col("court_person").replace(name_replacement_dict)
)


# Drop missing addresses
evictions_df = evictions_df.filter(pl.col("property_address_full").is_not_null())


# Add file month and year to dataset.
date_format = "%m/%d/%Y"
evictions_df = evictions_df.with_columns(
    pl.col("file_date")
    .str.strptime(pl.Date, date_format, strict=True)
    .dt.strftime("%Y-%m")
    .alias("file_month"),
    pl.col("file_date")
    .str.strptime(pl.Date, date_format, strict=True)
    .dt.year()
    .alias("file_year"),
    pl.col("latest_docket_date")
    .str.strptime(pl.Date, date_format, strict=True)
    .dt.strftime("%Y-%m")
    .alias("latest_docket_month"),
    pl.col("latest_docket_date")
    .str.strptime(pl.Date, date_format, strict=True)
    .dt.year()
    .alias("latest_docket_year"),
)


# Clean the values in the judgment_for_pdu variable.
judgment_for_pdu_replacement_dict = {
    "unknown": "Unknown",
    "plaintiff": "Plaintiff",
    "defendant": "Defendant",
}
evictions_df = evictions_df.with_columns(
    pl.col("judgment_for_pdu")
    .replace(judgment_for_pdu_replacement_dict)
    .alias("judgment_for_pdu")
)

# Replace missing values in money judgment column with zeroes.
evictions_df = evictions_df.with_columns(
    pl.col("judgment").fill_null(0).alias("judgment")
)

# After this point, ran through geocodio to get 2010 census tract and
# rooftop lat long for each property
evictions_df.write_csv(CLEAN_EVICTIONS)
