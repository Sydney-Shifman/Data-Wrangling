import pandas as pd

#---------------------------------------
# LOADING DATA
#---------------------------------------
# Load raw data from Hosptial
print("Beginning loading of Hospital data...")
hospital_df = pd.read_csv("us_hospital_locations.csv")
print(f"Finished loading Hospital data containing {hospital_df.shape[0]} rows")
#---------------------------------------
# FILTER OUT CLEANED DATA FOR COMBINING
#---------------------------------------
print("Beginning filtering data...")
# Filter only Seattle, Los Angeles, New York city data
clean_hospital_df  = hospital_df[hospital_df['CITY'].isin(['SEATTLE', 'LOS ANGELES', 'NEW YORK'])].copy()
print(f"\t- Filtered hospitals in Seattle, Los Angeles, New York")

# Drop unnecessary columns
num_columns = len(clean_hospital_df.columns)
clean_hospital_df.drop(columns=['X', 'Y', 'FID', 'ID', 'STATE', 'ZIP','ZIP4', 'TELEPHONE', 'TYPE',
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
#---------------------------------------
# EXPORTING CLEANED DATA
#---------------------------------------
print(f"Beginning exporting Hospital data containing {clean_hospital_df.shape[0]} rows...")
# Create .csv file for cleaned version of data
clean_hospital_df.to_csv("clean_hospital.csv", index=False)
print("Finished exporting cleaned and filtered data to clean_hospital.csv\n")
