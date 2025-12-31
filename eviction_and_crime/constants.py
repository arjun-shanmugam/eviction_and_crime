"""
Constants
"""

from pathlib import Path

DATA = Path("data")


RAW_EVICTIONS = DATA / "raw" / "2025-04-27_aug.csv"
RAW_CENSUS_TRACT_COVARIATES = DATA / "raw" / "tract_covariates.csv"
RAW_PERMITS = DATA / "raw" / "tmph067vrvl.csv"
RAW_CRIME = DATA / "raw" / "crime" / "*.csv"
OFFENSE_CODES = DATA / "raw" / "crime" / "rmsoffensecodes.xlsx"
CLEAN_CENSUS_TRACT_COVARIATES = DATA / "intermediate" / "tract_covariates.parquet"
CLEAN_EVICTIONS = DATA / "intermediate" / "evictions.csv"
CLEAN_GEOCODED_EVICTIONS = (
    DATA
    / "intermediate"
    / "evictions_geocodio_18898599e93760f523ca446cde00872f2bd82740.csv"
)
CLEAN_GEOCODED_EVICTIONS_WITH_STANDARDIZED_ADDRESSES = (
    DATA / "intermediate" / "evictions_geocoded_and_standardized.csv"
)
CLEAN_GEOCODED_EVICTIONS_WITH_STANDARDIZED_ADDRESSES_PARQUET = (
    DATA / "intermediate" / "evictions_geocoded_and_standardized.parquet"
)
EVICTIONS_PRE_MERGE = DATA / "intermediate" / "evictions_pre_merge.parquet"


OUTPUT = Path("output")
OUTPUT_PAPER_2025_12_25 = OUTPUT / "paper_2025_12_25"
OUTPUT_PAPER_2025_12_25_TABLES = OUTPUT_PAPER_2025_12_25 / "tables"

TREATMENT_MONTH_VARIABLE = "latest_docket_month"

TREATMENT_MONTHS_TO_KEEP = [
    "2017-11",
    "2017-12",
    "2018-01",
    "2018-02",
    "2018-03",
    "2018-04",
    "2018-05",
    "2018-06",
    "2018-07",
    "2018-08",
    "2018-09",
    "2018-10",
    "2018-11",
    "2018-12",
    "2019-01",
    "2019-02",
    "2019-03",
    "2019-04",
    "2019-05",
    "2019-06",
    "2019-07",
    "2019-08",
    "2019-09",
    "2019-10",
    "2019-11",
    "2019-12",
    "2020-01",
    "2020-02",
    "2020-03",
    "2020-04",
    "2020-05",
    "2020-06",
    "2020-07",
    "2020-08",
    "2020-09",
    "2020-10",
    "2020-11",
    "2020-12",
    "2021-01",
    "2021-02",
    "2021-03",
    "2021-04",
    "2021-05",
    "2021-06",
    "2021-07",
    "2021-08",
    "2021-09",
    "2021-10",
    "2021-11",
    "2021-12",
    "2022-01",
    "2022-02",
    "2022-03",
    "2022-04",
    "2022-05",
    "2022-06",
    "2022-07",
    "2022-08",
    "2022-09",
    "2022-10",
    "2022-11",
    "2022-12",
    "2023-01",
    "2023-02",
    "2023-03",
    "2023-04",
    "2023-05",
    "2023-06",
    "2023-07",
    "2023-08",
    "2023-09",
    "2023-10",
    "2023-11",
    "2023-12",
    "2024-01",
    "2024-02",
    "2024-03",
    "2024-04",
    "2024-05",
    "2024-06",
    "2024-07",
    "2024-08",
    "2024-09",
    "2024-10",
    "2024-11",
    "2024-12",
    "2025-01",
    "2025-02",
    "2025-03",
    "2025-04",
]
COVID_TREATMENT_DATES = [
    "2020-04",
    "2020-05",
    "2020-06",
    "2020-07",
    "2020-08",
    "2020-09",
    "2020-10",
    "2020-11",
    "2020-12",
    "2021-01",
    "2021-02",
    "2021-03",
    "2021-04",
    "2021-05",
    "2021-06",
    "2021-07",
    "2021-08",
    "2021-09",
    "2021-10",
    "2021-11",
    "2021-12",
    "2022-01",
    "2022-02",
]


VERBOSE = True
