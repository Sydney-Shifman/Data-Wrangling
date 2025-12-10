import pandas as pd

# ---------------------------------------
# LOADING DATA
# ---------------------------------------
# Load raw data from Hospital
print("Beginning loading of Hospital data...")
hospital_df = pd.read_csv("us_hospital_locations.csv")
print(f"Finished loading Hospital data containing {hospital_df.shape[0]} rows")
# ---------------------------------------
# CREATING MAPPING FOR DATA
# ---------------------------------------
print("Beginning creating mapping for Hospital data...")
# City Dictionary for NY
areas_in_ny = {
    'BROOKLYN': 'NEW YORK',
    'BRONX': 'NEW YORK',
    'STATEN ISLAND': 'NEW YORK',
    'WARDS ISLAND': 'NEW YORK'
}
print(f"\t- Created City Dictionary for NY")

# City Dictionary for LA
areas_in_la = {
    'ENCINO': 'LOS ANGELES',
    'HOLLYWOOD': 'LOS ANGELES',
    'RESEDA': 'LOS ANGELES',
    'SAN PEDRO': 'LOS ANGELES',
    'SYLMAR': 'LOS ANGELES',
    'TARZANA': 'LOS ANGELES',
    'WEST HILLS': 'LOS ANGELES',
    'WOODLAND HILLS': 'LOS ANGELES'
}
print(f"\t- Created City Dictionary for LA")
print("Finished creating mapping for data")
# ---------------------------------------
# PROCESSING MAPPING
# ---------------------------------------
print("Beginning mapping of data...")
# Create a copy for clean version
clean_hospital_df = hospital_df.copy()

# Filter only WA, CA, NY state data
clean_hospital_df = clean_hospital_df[clean_hospital_df['STATE'].isin(['WA', 'CA', 'NY'])]
print(f"\t- Filtered hospitals in WA, CA, NY")

# Map each area to city for NY
clean_hospital_df['CITY'] = clean_hospital_df['CITY'].map(areas_in_ny).fillna(clean_hospital_df['CITY'])
print(f"\t- Mapped to include area in NY")

# Map each area to city for LA
clean_hospital_df['CITY'] = clean_hospital_df['CITY'].map(areas_in_la).fillna(clean_hospital_df['CITY'])
print(f"\t- Mapped to include area in LA")
print("Finished mapping for data")
# ---------------------------------------
# FILTER OUT CLEANED DATA FOR COMBINING
# ---------------------------------------
print("Beginning filtering data...")
# Filter only Seattle, Los Angeles, New York city data
clean_hospital_df = clean_hospital_df[clean_hospital_df['CITY'].isin(['SEATTLE', 'LOS ANGELES', 'NEW YORK'])]
print(f"\t- Filtered hospitals in Seattle, Los Angeles, New York")

# Drop unnecessary columns
num_columns = len(clean_hospital_df.columns)
clean_hospital_df.drop(columns=['X', 'Y', 'FID', 'ID', 'STATE', 'ZIP', 'ZIP4', 'TELEPHONE', 'TYPE',
                                'STATUS', 'POPULATION', 'COUNTY', 'COUNTYFIPS', 'COUNTRY', 'NAICS_CODE',
                                'NAICS_DESC', 'SOURCE', 'SOURCEDATE', 'VAL_METHOD', 'VAL_DATE',
                                'WEBSITE', 'STATE_ID', 'ALT_NAME', 'ST_FIPS', 'OWNER', 'TTL_STAFF',
                                'BEDS', 'TRAUMA', 'HELIPAD'], inplace=True)
print(f"\t- Dropped {num_columns - len(clean_hospital_df.columns)} unnecessary columns")

# Rename columns for continuity
clean_hospital_df.rename(columns={'NAME': 'Hospital Name'}, inplace=True)
clean_hospital_df.rename(columns={'LATITUDE': 'Latitude'}, inplace=True)
clean_hospital_df.rename(columns={'LONGITUDE': 'Longitude'}, inplace=True)
clean_hospital_df.rename(columns={'CITY': 'City'}, inplace=True)
clean_hospital_df.rename(columns={'ADDRESS': 'Address'}, inplace=True)
print(f"\t- Renamed columns for continuity across .csv files")

# Reorder the columns
clean_hospital_df = clean_hospital_df[['City', 'Hospital Name', 'Address', 'Latitude', 'Longitude']]
print(f"\t- Reordered columns to be more organized")
print("Finished filtering data")
print("Finished processing of Hospital data")
# ---------------------------------------
# EXPORTING CLEANED DATA
# ---------------------------------------
print(f"Beginning exporting Hospital data containing {clean_hospital_df.shape[0]} rows...") #109 rows
# Create .csv file for cleaned version of data
clean_hospital_df.to_csv("clean_hospital.csv", index=False)
print("Finished exporting cleaned and filtered data to clean_hospital.csv\n")