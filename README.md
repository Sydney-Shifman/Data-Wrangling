# Data-Wrangling
## Instructions
#### Using Terminal
1. Clone the repository
   ```
   git clone https://github.com/Sydney-Shifman/Data-Wrangling.git
   ```
2. Navigate to the repository
   ```
   cd Data-Wrangling
   ```
3. Download .csv files:
      * [LA Crime Dataset](https://drive.google.com/file/d/1p-DbgMBolLshQX-bOKQ6ftenFRGSJNF-/view?usp=share_link)
      * [Seattle Crime Dataset](https://drive.google.com/file/d/1TbS0z8C-5gvb1gBNnI0R2DBDXqw5ik1N/view?usp=share_link)
      * [NY Present Crime Dataset](https://drive.google.com/file/d/1gTVBIecJ-j8aeueo43Mj1yqzrdqDBIQW/view?usp=share_link)
      * [NY Historical Crime Dataset](https://drive.google.com/file/d/1y1s4PohdHsOZFcB-QLHjVkkBEoKvDXbV/view?usp=share_link)
      * [Hospitals in the US Dataset](https://drive.google.com/file/d/1PD0nUwEjm-Ew8Olt8-DqMXss9ZzDHom4/view?usp=share_link)
        
4. Move downloaded .csv files into repository (commands if cloned to Desktop)
   ```
   mv ~/Downloads/NYPD_Arrest_Data__Year_to_Date_.csv ~/Data-Wrangling
   ```
   ```
   mv ~/Downloads/NYPD_Arrests_Data__Historic_.csv ~/Data-Wrangling
   ```
   ```
   mv ~/Downloads/Crime_Data_from_2020_to_Present.csv ~/Data-Wrangling
   ```
   ```
   mv ~/Downloads/SPD_Crime_Data__2008-Present.csv ~/Data-Wrangling
   ```
   ```
   mv ~/Downloads/hospital_coordinates.csv ~/Data-Wrangling
   ```
6. Install pandas, numpy, duckdb (if needed)
   ```
   pip list
   ```
   ```
   pip install pandas numpy duckdb
   ```
7. Run the script run_pipeline.py
   ```
   python3 run_pipeline.py
   ```

#### Using IDE (PyCharm)
1. Clone and open the repository
   ```
   https://github.com/Sydney-Shifman/Data-Wrangling.git
   ```
2. Download .csv files:
      * [LA Crime Dataset](https://drive.google.com/file/d/1p-DbgMBolLshQX-bOKQ6ftenFRGSJNF-/view?usp=share_link)
      * [Seattle Crime Dataset](https://drive.google.com/file/d/1TbS0z8C-5gvb1gBNnI0R2DBDXqw5ik1N/view?usp=share_link)
      * [NY Present Crime Dataset](https://drive.google.com/file/d/1gTVBIecJ-j8aeueo43Mj1yqzrdqDBIQW/view?usp=share_link)
      * [NY Historical Crime Dataset](https://drive.google.com/file/d/1y1s4PohdHsOZFcB-QLHjVkkBEoKvDXbV/view?usp=share_link)
      * [Hospitals in the US Dataset](https://drive.google.com/file/d/1PD0nUwEjm-Ew8Olt8-DqMXss9ZzDHom4/view?usp=share_link)
        
3. Move downloaded .csv files into repository
4. Install pandas, numpy, duckdb packages in Python Interpreter (if needed)
5. Run the script run_pipeline.py

