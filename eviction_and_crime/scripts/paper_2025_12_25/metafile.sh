python3 -m evictions_and_crime.scripts.paper_2025_12_25.clean_evictions
# manually run output through geocodio
python3 -m eviction_and_crime.scripts.clean_census_tract_covariates
python3 -m eviction_and_crime.scripts.standardize_addresses
python3 -m eviction_and_crime.scripts.save_evictions_as_parquet
python3 -m eviction_and_crime.scripts.prepare_evictions_for_merge
python3 -m eviction_and_crime.scripts.merge

