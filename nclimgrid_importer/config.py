# config.py - Configuration constants for agricultural weather analysis

# --- Date Window Definitions ---
# Growing Season (GS)
GS_START_MONTH = 4  # April
GS_START_DAY = 1
GS_END_MONTH = 9    # September
GS_END_DAY = 30

# Silking-Grain-Fill (SGF)
SGF_START_MONTH = 7  # July
SGF_START_DAY = 1
SGF_END_MONTH = 8    # August
SGF_END_DAY = 15

# --- Threshold Definitions ---
# For GDD (Growing Degree Days)
GDD_BASE_TEMP_C = 10.0  # Base temperature in Celsius
GDD_MAX_TEMP_C = 29.0   # Max temperature cap for T_avg in GDD calculation

# For KDD (Killing Degree Days / Extreme Heat Days)
KDD_TEMP_THRESHOLD_C = 29.0 # T_max above this contributes to KDD

# For CHD (Concurrent Hot-Dry Days)
CHD_TEMP_THRESHOLD_C = 30.0 # T_max must be > this value
CHD_PRECIP_THRESHOLD_MM = 1.0 # Precipitation must be < this value

# --- Data Specific Constants ---
# Placeholder for missing values in the raw daily weather data
RAW_DATA_MISSING_VALUE_PLACEHOLDER = -999.99

# Variables to fetch from the daily weather data source
# These should match the column names in the Parquet files (case-sensitive)
TARGET_DAILY_WEATHER_VARIABLES = ["tmin", "tmax", "tavg", "prcp"]

# --- Input/Output File Names ---
# (You can add filenames here if you want to configure them centrally)
# INPUT_YIELD_CSV = "df_yield_weather.csv"
# OUTPUT_CSV_NAME = "df_yield_with_calculated_weather.csv"

# --- Other Settings ---
# First year for which NClimGrid data is available and should be processed
DATA_START_YEAR = 1951
