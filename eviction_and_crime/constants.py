"""
Constants
"""

from pathlib import Path

DATA = Path("data")
RAW_EVICTIONS_DATA = DATA / "raw" / "2025-04-27_aug.csv"
RAW_CENSUS_TRACT_COVARIATES = DATA / "raw" / "tract_covariates.csv"
CLEAN_CENSUS_TRACT_COVARIATES = DATA / "intermediate" / "tract_covariates.csv"


INTERMEDIATE_EVICTIONS_DATA = DATA / "intermediate" / "evictions.csv"
