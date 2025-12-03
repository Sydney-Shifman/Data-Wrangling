import pandas as pd
import numpy as np
import re

#---------------------------------------
# LOADING DATA
#---------------------------------------
print("\nBeginning processing of Seattle data...")
# Load raw data
spd_df = pd.read_csv("SPD_Crime_Data__2008-Present.csv")

print("Finished loading data")
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
# FILTER OUT CLEANED DATA FOR COMBINING
#---------------------------------------
print("Beginning filtering data...")
# Drop columns not shared by datasets
num_columns = len(clean_spd_df.columns)
clean_spd_df.drop(columns=['Offense ID', 'Offense Date', 'NIBRS Group AB', 'NIBRS Crime Against Category', 'Offense Sub Category',
                           'Shooting Type Group', 'Beat', 'Precinct', 'Sector', 'Reporting Area', 'Report Time'], inplace=True)
print(f"\t- Dropped {num_columns - len(clean_spd_df.columns)} columns")

# Rename columns for continuity between datasets
clean_spd_df.rename(columns={'Block Address': 'Reported Location'}, inplace=True)
clean_spd_df.rename(columns={'Neighborhood': 'Reported Area'}, inplace=True)
clean_spd_df.rename(columns={'Offense Category': 'NIBRS Category'}, inplace=True)
clean_spd_df.rename(columns={'NIBRS Offense Code Description': 'NIBRS Desc'}, inplace=True)
clean_spd_df.rename(columns={'NIBRS_offense_code': 'NIBRS Code'}, inplace=True)
print(f"\t- Renamed columns for continuity")

# Create column to identify city of crime
clean_spd_df['City'] = 'Seattle'
print(f"\t- Added column to identify city of crime")

# Reorder the columns
clean_spd_df = clean_spd_df[['City', 'Report Number', 'Report Date', 'NIBRS Code', 'NIBRS Desc', 'NIBRS Category', 'Reported Area',
                             'Reported Location', 'Latitude', 'Longitude']]
print("Finished filtering data")
print("Finished processing of Seattle data")
#---------------------------------------
# EXPORTING CLEANED DATA
#---------------------------------------
print("Beginning export of Seattle data...")
# Create .csv file for cleaned version of data
clean_spd_df.to_csv('clean_sea.csv', index=False)
print("Finished exporting cleaned and filtered data to clean_sea.csv\n")
