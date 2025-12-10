import pandas as pd

#---------------------------------------
# LOADING DATA
#---------------------------------------
# Load raw data from Seattle that has NIBRS info
spd_df = pd.read_csv("SPD_Crime_Data__2008-Present.csv")
#---------------------------------------
# CREATING MAPPING FOR NIBRS
#---------------------------------------
# Create NIBRS_CD to NIBRS_DESC Dictionary
nibrs_to_desc = {}
unique_pairs = spd_df[["NIBRS_offense_code", "NIBRS Offense Code Description"]].drop_duplicates().values.tolist()

for nibrs_cd, nibrs_desc in unique_pairs:
    if nibrs_cd not in nibrs_to_desc:
        nibrs_to_desc[nibrs_cd] = nibrs_desc

# Create NIBRS_DESC to NIBRS_CAT Dictionary
nibrs_desc_to_cat = {}
unique_pairs = spd_df[["NIBRS Offense Code Description", "Offense Category"]].drop_duplicates().values.tolist()

for nibrs_desc, nibrs_category in unique_pairs:
    if nibrs_desc not in nibrs_desc_to_cat:
        nibrs_desc_to_cat[nibrs_desc] = nibrs_category