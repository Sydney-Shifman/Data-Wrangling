import subprocess
import duckdb

#---------------------------------------
# RUN PIPELINE FOR SCRIPTS
#---------------------------------------
# Configuration
CLEAN_HOSPITAL = 'clean_hospital.csv'
CLEAN_SEA = 'clean_sea.csv'
CLEAN_NY = 'clean_ny.csv'
CLEAN_LA = 'clean_la.csv'
DB_FILE = 'crimes_analysis.duckdb'

# Run the Hospital Processing Script
subprocess.run(['python3', 'processing_hospital.py', '/us_hospital_locations.csv', CLEAN_HOSPITAL], check=True)

# Run the SEA Processing Script
subprocess.run(['python3', 'processing_sea.py', '/SPD_Crime_Data__2008-Present.csv', CLEAN_SEA], check=True)

# Run the NY Processing Script
subprocess.run(['python3', 'processing_ny.py', '/NYPD_Arrests_Data__Historic_.csv', '/NYPD_Arrest_Data__Year_to_Date_.csv', CLEAN_NY], check=True)

# Run the LA Processing Script
subprocess.run(['python3', 'processing_la.py', '/Crime_Data_from_2020_to_Present.csv', CLEAN_LA], check=True)
#---------------------------------------
# INGEST DATA INTO DUCKDB
#---------------------------------------
# Loading DuckDB
conn = duckdb.connect(database=DB_FILE)

query = f"""
    CREATE OR REPLACE TABLE Crimes AS
    SELECT * FROM read_csv_auto('{CLEAN_SEA}')
    UNION ALL
    SELECT * FROM read_csv_auto('{CLEAN_NY}')
    UNION ALL
    SELECT * FROM read_csv_auto('{CLEAN_LA}');
"""

print("\nBeginning ingesting data into duckdb...")
conn.execute(query)
print("Finished ingesting data into duckdb")

row_count = conn.sql("SELECT COUNT(*) FROM Crimes").fetchone()[0]

export_query = """
               COPY Crimes TO 'combined_crime_data.csv' (HEADER, DELIMITER ',');
"""

print("Beginning exporting combined data...")
conn.execute(export_query)
print(f"Finished exporting combined data containing {row_count} rows to combined_crime_data.csv\n")

conn.close()