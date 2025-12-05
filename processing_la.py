import pandas as pd
import numpy as np
import nibrs_mapping

#---------------------------------------
# LOADING DATA
#---------------------------------------
print("\nBeginning processing of LA data...")
print("Beginning loading of LA data...")
# Load raw LA data
la_df = pd.read_csv("Crime_Data_from_2020_to_Present.csv")
print(f"Finished loading LA data containing {la_df.shape[0]} rows")

print("Beginning loading of Hospital data...")
# Load raw Hospital data
hospitals = pd.read_csv("hospital_coordinates.csv")
print(f"Finished loading Hospital data containing {hospitals.shape[0]} rows")
#---------------------------------------
# CREATING MAPPING FOR DATA
#---------------------------------------
print("Beginning creating mapping for LA data...")
# CRM_CD to NIBRS_CD Dictionary
crmcd_to_nibrs = {
    # Kidnapping/Abduction
    "434": "100", "910": "100", "920": "100", "922": "100",

    # Robbery
    "210": "120", "220": "120",

    # Arson
    "648": "200",

    # Extortion/Blackmail
    "940": "210",

    # Burglary/Breaking & Entering
    "310": "220", "320": "220",

    # Motor Vehicle Theft
    "433": "240", "510": "240", "520": "240", "522": "240",

    # Counterfeiting/Forgery
    "649": "250", "660": "250",

    # Embezzlement
    "668": "270", "670": "270",

    # Destruction/Damage/Vandalism of Property
    "740": "290", "745": "290", "924": "290",

    # Pornography/Obscene Material
    "814": "370",

    # Violation of No Contact Orders
    "900": "500", "901": "500", "902": "500", "904": "500", "906": "500",

    # Bribery
    "942": "510",

    # Weapon Law Violations
    "753": "520", "756": "520", "931": "520",

    # Animal Cruelty
    "943": "720",

    # Murder & Nonnegligent Manslaughter
    "110": "999",

    # Negligent Manslaughter
    "113": "999",

    # Rape
    "121": "11A", "122": "11A",

    # Sodomy
    "820": "11B", "821": "11B",

    # Sexual Assault With An Object
    "815": "11C",

    # Fondling
    "760": "11D", "813": "11D", "860": "11D",

    # Aggravated Assault
    "230": "13A", "231": "13A", "235": "13A", "236": "13A", "250": "13A", "251": "13A",
    "622": "13A", "647": "13A",

    # Simple Assault
    "623": "13B", "624": "13B", "625": "13B", "626": "13B", "627": "13B",

    # Intimidation
    "755": "13C", "761": "13C", "763": "13C", "928": "13C", "930": "13C",

    # Pocket-picking
    "352": "23A", "353": "23A", "452": "23A", "453": "23A",

    # Purse-snatching
    "451": "23B", "351": "23B",

    # Shoplifting
    "343": "23C", "442": "23C", "443": "23C",

    # Theft From Building
    "470": "23D", "471": "23D",

    # Theft From Coin-Operated Machine or Device
    "473": "23E", "474": "23E", "475": "23E",

    # Theft From Motor Vehicle
    "330": "23F", "331": "23F", "410": "23F", "420": "23F", "421": "23F",

    # Theft of Motor Vehicle Parts or Accessories
    "349": "23G", "446": "23G",

    # All Other Larceny
    "341": "23H", "345": "23H", "350": "23H", "440": "23H", "441": "23H", "445": "23H",
    "450": "23H", "480": "23H", "485": "23H", "487": "23H", "444": "23H",

    # False Pretenses/Swindle/Confidence Game
    "347": "26A", "662": "26A", "664": "26A", "666": "26A", "950": "26A", "951": "26A",

    # Credit Card/Automated Teller Machine Fraud
    "653": "26B", "654": "26B",

    # Identity Theft
    "354": "26F",

    # Hacking/Computer Invasion
    "661": "26G",

    # Drug/Narcotic Violations
    "865": "35A",

    # Incest
    "830": "36A",

    # Statutory Rape
    "810": "36B", "812": "36B",

    # Assisting or Promoting Prostitution
    "805": "40B", "806": "40B",

    # Human Trafficking, Commercial Sex Acts
    "822": "64A",

    # Human Trafficking, Involuntary Servitude
    "921": "64B",

    # Bad Checks
    "651": "90A", "652": "90A",

    # Curfew/Loitering/Vagrancy Violations
    "933": "90B",

    # Disorderly Conduct
    "438": "90C", "762": "90C", "880": "90C", "882": "90C", "884": "90C", "886": "90C",

    # Family Offenses, Nonviolent
    "237": "90F", "870": "90F",

    # Peeping Tom
    "932": "90H",

    # Trespass of Real
    "888": "90J",

    # All Other Offenses
    "432": "90Z", "435": "90Z", "436": "90Z", "437": "90Z", "439": "90Z", "840": "90Z",
    "845": "90Z", "850": "90Z", "890": "90Z", "903": "90Z", "926": "90Z", "944": "90Z",
    "946": "90Z", "948": "90Z", "949": "90Z", "954": "90Z", "956": "90Z"
}
print(f"\t- Created CRM_CD to NIBRS_CD Dictionary")
print("Finished creating mapping for data")
#---------------------------------------
# PROCESSING MAPPING
#---------------------------------------
print("Beginning mapping of data...")
# Create a copy for clean version
clean_la_df = la_df.copy()

# Convert Crm_Cd to string to match mapping dictionary
clean_la_df['Crm Cd'] = clean_la_df['Crm Cd'].astype(str)

# Map each CRM_CD to its NIBRS_offense_code
clean_la_df['NIBRS_offense_code'] = clean_la_df['Crm Cd'].map(crmcd_to_nibrs)
print(f"\t- Mapped NIBRS_offense_code")

# Map each NIBRS_offense_code to its NIBRS Offense Code Description
clean_la_df['NIBRS Offense Code Description'] = clean_la_df['NIBRS_offense_code'].map(nibrs_mapping.nibrs_to_desc)
print(f"\t- Mapped NIBRS Offense Code Description")

# Map each NIBRS Offense Code Description to its Offense Category
clean_la_df['Offense Category'] = clean_la_df['NIBRS Offense Code Description'].map(nibrs_mapping.nibrs_desc_to_cat)
print(f"\t- Mapped Offense Category")
print("Finished mapping for data")
#---------------------------------------
# CLEANING MAPPED OUT DATA
#---------------------------------------
print("Beginning cleaning of data...")
# Make null value (0.0) np.nan for lat and lon
clean_la_df.loc[:, ['LAT', 'LON']] = clean_la_df[['LAT', 'LON']].replace(0.0, np.nan)
print(f"\t- Converted {(la_df['LAT'].count() - clean_la_df['LAT'].count()) + (la_df['LON'].count() - clean_la_df['LON'].count())} values to np.nan in latitude and longitude")

# Fix spacing between words for Location
clean_la_df['LOCATION'] = clean_la_df['LOCATION'].str.strip().str.split().str.join(" ")
print(f"\t- Removed extra spacing in Location")


# Separate date and time for Date Rptd
date = []
time = []

for row in clean_la_df['Date Rptd']:
    dateTime = row.split(maxsplit=1)
    date.append(dateTime[0])
    time.append(dateTime[1])

clean_la_df.drop(columns=['Date Rptd'], inplace=True)
clean_la_df['Report Date'] = date
clean_la_df['Report Time'] = time
print(f"\t- Separated Date Rptd column into Report Date and Report Time")

# Separate date and time for DATE OCC
date = []

for row in clean_la_df['DATE OCC']:
    dateTime = row.split(maxsplit=1)
    date.append(dateTime[0])

clean_la_df.drop(columns=['DATE OCC'], inplace=True)
clean_la_df['OCC Date'] = date
clean_la_df['OCC Time'] = clean_la_df['TIME OCC']
print(f"\t- Separated Date OCC column into Report Date and Report Time")

# Convert formatting for timestamp into standard time (include seconds)
clean_la_df['OCC Time'] = pd.to_datetime(clean_la_df['OCC Time'], format='%H%M',errors='coerce')
clean_la_df['OCC Time'] = clean_la_df['OCC Time'].dt.strftime('%I:%M:%S')
print(f"\t- Fixed formatting of OCC Time to be in standard time")
print("Finished cleaning data")
#---------------------------------------
# ADDING HOSPITAL LOCATIONS TO DATA
#---------------------------------------
print("Beginning adding nearest hospital locations to data...")

# Prepare hospital arrays
hosp_lats = hospitals['LATITUDE'].to_numpy(dtype=float)
hosp_lons = hospitals['LONGITUDE'].to_numpy(dtype=float)
hosp_names = hospitals['HOSPITAL NAME'].to_numpy(dtype=object)
hosp_addrs = hospitals['ADDRESS'].to_numpy(dtype=object)

# Prepare result columns with default np.nan
nearest_names = np.full(len(clean_la_df), np.nan, dtype=object)
nearest_addrs = np.full(len(clean_la_df), np.nan, dtype=object)

# Mask of rows with valid coordinates (non-null and not NaN)
valid_mask = clean_la_df['LAT'].notna() & clean_la_df['LON'].notna()

if valid_mask.any():
    crime_lats = clean_la_df.loc[valid_mask, 'LAT'].to_numpy(dtype=float)
    crime_lons = clean_la_df.loc[valid_mask, 'LON'].to_numpy(dtype=float)

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
clean_la_df['Nearest Hospital'] = nearest_names
clean_la_df['Hospital Address'] = nearest_addrs
print(f"\t- Added column to identify nearest hospital")
print(f"\t- Added column to identify hospital address")
print("Finished adding nearest hospital locations to data")
#---------------------------------------
# FILTER OUT CLEANED DATA FOR COMBINING
#---------------------------------------
print("Beginning filtering data...")
# Drop columns not shared by datasets
num_columns = len(clean_la_df.columns)
clean_la_df.drop(columns=['Report Time', 'OCC Date', 'OCC Time', 'AREA', 'Rpt Dist No', 'Part 1-2', 'Crm Cd',
                           'Crm Cd Desc', 'Mocodes', 'Vict Age', 'Vict Sex', 'Vict Descent',
                          'Premis Cd', 'Premis Desc', 'Weapon Used Cd', 'Weapon Desc', 'Status',
                          'Status Desc', 'Crm Cd 1', 'Crm Cd 2', 'Crm Cd 3', 'Crm Cd 4', 'Cross Street'], inplace=True)
print(f"\t- Dropped {num_columns - len(clean_la_df.columns)} columns that aren't shared between data")

# Rename columns for continuity between datasets
clean_la_df.rename(columns={'DR_NO': 'Report Number'}, inplace=True)
clean_la_df.rename(columns={'LOCATION': 'Reported Location'}, inplace=True)
clean_la_df.rename(columns={'AREA NAME': 'Reported Area'}, inplace=True)
clean_la_df.rename(columns={'LAT': 'Latitude'}, inplace=True)
clean_la_df.rename(columns={'LON': 'Longitude'}, inplace=True)
clean_la_df.rename(columns={'Offense Category': 'NIBRS Category'}, inplace=True)
clean_la_df.rename(columns={'NIBRS Offense Code Description': 'NIBRS Desc'}, inplace=True)
clean_la_df.rename(columns={'NIBRS_offense_code': 'NIBRS Code'}, inplace=True)
print(f"\t- Renamed columns for continuity when combining data")

# Create column to identify city of crime
clean_la_df['City'] = 'Los Angeles'
print(f"\t- Added column to identify city of crime")

# Reorder the columns
clean_la_df = clean_la_df[['City', 'Report Number', 'Report Date', 'NIBRS Code', 'NIBRS Desc', 'NIBRS Category', 'Reported Area',
                             'Reported Location', 'Latitude', 'Longitude', 'Nearest Hospital', 'Hospital Address']]
print(f"\t- Reordered columns to be more organized when combining data")

print("Finished filtering data")
print("Finished processing of LA data")
#---------------------------------------
# EXPORTING CLEANED DATA
#---------------------------------------
print(f"Beginning exporting LA data containing {clean_la_df.shape[0]} rows...")
# Create .csv file for cleaned version of data
clean_la_df.to_csv("clean_la.csv", index=False)
print("Finished exporting cleaned and filtered data to clean_la.csv\n")
