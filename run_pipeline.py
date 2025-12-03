import subprocess
import duckdb

# Configuration
CLEAN_SEA = 'clean_sea.csv'
CLEAN_NY = 'clean_ny.csv'
CLEAN_LA = 'clean_la.csv'
DB_FILE = 'crimes_analysis.duckdb'

# Run the SEA Processing Script
subprocess.run(['python3', 'processing_sea.py', '/SPD_Crime_Data__2008-Present.csv.csv', CLEAN_SEA], check=True)

# Run the NY Processing Script
subprocess.run(['python3', 'processing_ny.py', '/NYPD_Arrests_Data__Historic_.csv', '/NYPD_Arrest_Data__Year_to_Date_.csv', CLEAN_NY], check=True)

# Run the LA Processing Script
subprocess.run(['python3', 'processing_la.py', '/Crime_Data_from_2020_to_Present.csv', CLEAN_NY], check=True)


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

conn.execute(query)

conn.close()