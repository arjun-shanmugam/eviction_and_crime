from eviction_and_crime.constants import (
    RAW_PERMITS_DATA,
    GEOCODED_EVICTIONS_DATA,
)
# TODO: Write this script
# Needs to take raw addresses pre-standardization and use a geocoder to standardize them


# def extract_clean_address(address: str):
#     print(f"geocoding address: {address}")
#     try:
#         location = geolocator.geocode(address)
#         return location.address
#     except:
#         return ""
#
#
# evictions_df = pd.read_csv(INPUT_DATA_EVICTIONS)
# permits_df = pd.read_csv(INPUT_DATA_PERMITS)
#
# permits_df["address_std"] = permits_df.apply(
#     lambda x: extract_clean_address(
#         str(x["Raw Address"]) + str(x["city"]) + str(x["state"]) + str(x["zip"])
#     ),
#     axis=1,
# )
#
# evictions_df["address_std"] = evictions_df.apply(
#     lambda x: extract_clean_address(
#         str(x["Number"])
#         + str(x["Street"])
#         + str(x["City"])
#         + str(x["State"])
#         + str(x["Zip"])
#     ),
#     axis=1,
# )
# permits_df.to_csv(INPUT_DATA_PERMITS)
# evictions_df.to_csv(INPUT_DATA_EVICTIONS)
