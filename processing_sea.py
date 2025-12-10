import pandas as pd
import numpy as np
import re

#---------------------------------------
# LOADING DATA
#---------------------------------------
print("\nBeginning processing of Seattle data...")
print("Beginning loading of Seattle data...")
# Load raw Seattle data
spd_df = pd.read_csv("SPD_Crime_Data__2008-Present.csv")
print(f"Finished loading Seattle data containing {spd_df.shape[0]} rows")
#---------------------------------------
# CLEANING MAPPED OUT DATA
#---------------------------------------
print("Beginning cleaning of data...")
# Create a copy for clean version
clean_spd_df = spd_df.copy()

# Remove rows that aren't a crime
clean_spd_df = spd_df[~spd_df['NIBRS Crime Against Category'].isin(['NOT_A_CRIME', '-'])].copy()
print(f"\t- Removed {spd_df.shape[0] - clean_spd_df.shape[0]} rows that represent a non-criminal report")

# Make null value (-1.0) np.nan for lat and lon
clean_spd_df.loc[:, ['Latitude', 'Longitude']] = clean_spd_df[['Latitude', 'Longitude']].replace('-1.0', np.nan)
print(f"\t- Converted {(spd_df['Latitude'].count() - clean_spd_df['Latitude'].count()) + (spd_df['Longitude'].count() - clean_spd_df['Longitude'].count())} values converted to np.nan in latitude and longitude")

# Make null values across dataset into np.nan
print(f"\t- Converted {clean_spd_df.isin(['-', 'UNKNOWN', 'REDACTED', 'OOJ', '99']).sum().sum()} null/unknown values to np.nan")
clean_spd_df.replace(['-', 'UNKNOWN', 'REDACTED', 'OOJ', '99'], np.nan, inplace=True)

# Fix missing street label in block adress
def fix_block_address(addr):
    if not addr or pd.isna(addr):
        return addr

    match = re.search(r'(\d+)XX', addr)
    if not match:
        return addr

    num = int(match.group(1))

    if 10 <= num % 100 <= 20:
        suffix = "TH"
    else:
        last = num % 10
        if last == 1:
            suffix = "ST"
        elif last == 2:
            suffix = "ND"
        elif last == 3:
            suffix = "RD"
        else:
            suffix = "TH"

    corrected = re.sub(r'\d+XX', f"{num}{suffix}", addr)
    return corrected

# Apply fix to block address on data
clean_spd_df['Block Address'] = clean_spd_df['Block Address'].apply(fix_block_address)
print(f"\t- Fixed {spd_df['Block Address'].str.contains(r'\d+XX', na=False).sum()} missing street labels for block address")

# Separate date and time
date = []
time = []

for row in clean_spd_df['Report DateTime']:
    dateTime = row.split(maxsplit=1)
    date.append(dateTime[0])
    time.append(dateTime[1])

clean_spd_df.drop(columns=['Report DateTime'], inplace=True)
clean_spd_df['Report Date'] = date
clean_spd_df['Report Time'] = time
print(f"\t- Separated DateTime column into Report Date and Report Time")
print("Finished cleaning data")
#---------------------------------------
# ADDING HOSPITAL LOCATIONS TO DATA
#---------------------------------------
print("Beginning adding nearest hospital locations to data...")
# Load cleaned hospital data for use
clean_hospital_df = pd.read_csv('clean_hospital.csv')
print(f"\t- Loaded cleaned hospital data")

# Prepare hospital arrays
hosp_lats = clean_hospital_df['Latitude'].to_numpy(dtype=float)
hosp_lons = clean_hospital_df['Longitude'].to_numpy(dtype=float)
hosp_names = clean_hospital_df['Name'].to_numpy(dtype=object)
hosp_addrs = clean_hospital_df['Address'].to_numpy(dtype=object)

# Prepare result columns with default np.nan
nearest_names = np.full(len(clean_spd_df), np.nan, dtype=object)
nearest_addrs = np.full(len(clean_spd_df), np.nan, dtype=object)

# Mask of rows with valid coordinates (non-null and not NaN)
valid_mask = clean_spd_df['Latitude'].notna() & clean_spd_df['Longitude'].notna()

if valid_mask.any():
    crime_lats = clean_spd_df.loc[valid_mask, 'Latitude'].to_numpy(dtype=float)
    crime_lons = clean_spd_df.loc[valid_mask, 'Longitude'].to_numpy(dtype=float)

    # Compute squared Euclidean distances in a vectorized way:
    # shape -> (n_crimes, n_hospitals)
    dlat = crime_lats[:, None] - hosp_lats[None, :]
    dlon = crime_lons[:, None] - hosp_lons[None, :]
    d2 = dlat * dlat + dlon * dlon

    # For each crime, find index of nearest hospital
    nearest_idx = np.argmin(d2, axis=1)

    # Map back to names and addresses
    nearest_names[valid_mask.to_numpy()] = hosp_names[nearest_idx]
    nearest_addrs[valid_mask.to_numpy()] = hosp_addrs[nearest_idx]

# Assign to two separate columns
clean_spd_df['Nearest Hospital'] = nearest_names
clean_spd_df['Hospital Address'] = nearest_addrs
print(f"\t- Added column to identify nearest hospital")
print(f"\t- Added column to identify hospital address")
print("Finished adding nearest hospital locations to data")
#---------------------------------------
# FILTER OUT CLEANED DATA FOR COMBINING
#---------------------------------------
print("Beginning filtering data...")
# Drop columns not shared by datasets
num_columns = len(clean_spd_df.columns)
clean_spd_df.drop(columns=['Offense ID', 'Offense Date', 'NIBRS Group AB', 'NIBRS Crime Against Category', 'Offense Sub Category',
                           'Shooting Type Group', 'Beat', 'Precinct', 'Sector', 'Reporting Area', 'Report Time'], inplace=True)
print(f"\t- Dropped {num_columns - len(clean_spd_df.columns)} columns that aren't shared between data")

# Rename columns for continuity between datasets
clean_spd_df.rename(columns={'Block Address': 'Reported Location'}, inplace=True)
clean_spd_df.rename(columns={'Neighborhood': 'Reported Area'}, inplace=True)
clean_spd_df.rename(columns={'Offense Category': 'NIBRS Category'}, inplace=True)
clean_spd_df.rename(columns={'NIBRS Offense Code Description': 'NIBRS Description'}, inplace=True)
clean_spd_df.rename(columns={'NIBRS_offense_code': 'NIBRS Code'}, inplace=True)
print(f"\t- Renamed columns for continuity when combining data")

# Create column to identify city of crime
clean_spd_df['City'] = 'Seattle'
print(f"\t- Added column to identify city of crime")

# Reorder the columns
clean_spd_df = clean_spd_df[['City', 'Report Number', 'Report Date', 'NIBRS Code', 'NIBRS Description', 'NIBRS Category', 'Reported Area',
                             'Reported Location', 'Nearest Hospital', 'Hospital Address', 'Latitude', 'Longitude']]
print(f"\t- Reordered columns to be more organized when combining data")
print("Finished filtering data")
print("Finished processing of Seattle data")
#---------------------------------------
# EXPORTING CLEANED DATA
#---------------------------------------
print(f"Beginning exporting Seattle data containing {clean_spd_df.shape[0]} rows...")
# Create .csv file for cleaned version of data
clean_spd_df.to_csv('clean_sea.csv', index=False)
print("Finished exporting cleaned and filtered data to clean_sea.csv\n")