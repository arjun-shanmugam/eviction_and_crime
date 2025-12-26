"""
Clean raw evictions data
"""

from eviction_and_crime.constants import RAW_EVICTIONS_DATA
import polars as pl


evictions_df = pl.read_csv(RAW_EVICTIONS_DATA)
