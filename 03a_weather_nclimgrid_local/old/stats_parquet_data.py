import polars as pl
import os
# import glob # Not used when using os.walk

# --- Define Parameters ---
HARRIS_COUNTY_FIPS = "48201" # Harris County, Texas FIPS code (string)
TARGET_STATE_NAME = "Texas"  # Full state name as in the data
# Specify variables you want stats for (ensure these are actual column names in your Parquet files)
TARGET_VARIABLES = ["tmin", "tmax", "tavg", "prcp"] 
YEAR_FOLDERS = ["1951", "1952", "1953", "1954", "1955", "1956", "1957", "1958", "1959", "1960"] # Folders to process
MISSING_VALUE_PLACEHOLDER = -999.99 # Placeholder for missing data

def analyze_local_data():
    """
    Reads Parquet files from local year-based folders, filters for a specific
    county and state, and prints descriptive statistics.
    """
    all_file_paths = []
    
    # Get the directory where the script itself is located
    # This makes the script find folders relative to its own position
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script directory (base path for data folders): {script_dir}")

    for year_folder in YEAR_FOLDERS:
        # Construct the path to the year folder relative to the script's directory
        folder_path = os.path.join(script_dir, year_folder)
        print(f"Checking for folder: {folder_path}") # Debug print
        if os.path.isdir(folder_path):
            # More robust way using os.walk to find all .parquet files 
            # in the year_folder and its subdirectories
            for root, _, files in os.walk(folder_path):
                for file in files:
                    if file.endswith(".parquet"):
                        all_file_paths.append(os.path.join(root, file))
        else:
            print(f"Warning: Folder not found - {folder_path}")

    if not all_file_paths:
        print("No Parquet files found in the specified year folders. Exiting.")
        return

    print(f"Found {len(all_file_paths)} Parquet files to process.")
    print(f"First few file paths: {all_file_paths[:3] if all_file_paths else 'None'}")

    try:
        print("\nLoading all Parquet files...")
        lazy_df = pl.scan_parquet(all_file_paths)
        
        print(f"\nFiltering for state_name: '{TARGET_STATE_NAME}' and fips: '{HARRIS_COUNTY_FIPS}'...")
        filtered_lazy_df = lazy_df.filter(
            (pl.col("state_name") == TARGET_STATE_NAME) & (pl.col("fips") == HARRIS_COUNTY_FIPS)
        )
        
        df_harris = filtered_lazy_df.collect()
        
        if df_harris.height == 0:
            print(f"\nNo data found for state '{TARGET_STATE_NAME}' and FIPS '{HARRIS_COUNTY_FIPS}' in the specified files.")
            return
            
        print(f"\nSuccessfully loaded and filtered {df_harris.height} rows for Harris County, TX.")
        
        print("\nSchema of loaded data:")
        for col_name, dtype in df_harris.schema.items():
            print(f"  - {col_name}: {dtype}")

        # --- Cast and Clean Numeric Columns ---
        numeric_cols_to_process = [var for var in TARGET_VARIABLES if var in df_harris.columns]
        
        if numeric_cols_to_process:
            print(f"\n--- Preparing columns for statistics: {numeric_cols_to_process} ---")
            cast_and_clean_expressions = []

            for col_name in numeric_cols_to_process:
                if df_harris[col_name].dtype == pl.String:
                    print(f"  Processing column '{col_name}': Stripping chars, casting to Float64, then replacing {MISSING_VALUE_PLACEHOLDER} with null.")
                    cast_and_clean_expressions.append(
                        pl.when(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False) == MISSING_VALUE_PLACEHOLDER)
                        .then(None) 
                        .otherwise(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False))
                        .alias(col_name)
                    )
                elif df_harris[col_name].dtype == pl.Float64: 
                    print(f"  Processing column '{col_name}': Already Float64, replacing {MISSING_VALUE_PLACEHOLDER} with null.")
                    cast_and_clean_expressions.append(
                        pl.when(pl.col(col_name) == MISSING_VALUE_PLACEHOLDER)
                        .then(None)
                        .otherwise(pl.col(col_name))
                        .alias(col_name)
                    )
                elif df_harris[col_name].dtype.is_numeric(): 
                     print(f"  Column '{col_name}' is numeric ({df_harris[col_name].dtype}), replacing {MISSING_VALUE_PLACEHOLDER} with null.")
                     cast_and_clean_expressions.append(
                        pl.when(pl.col(col_name).cast(pl.Float64, strict=False) == MISSING_VALUE_PLACEHOLDER) 
                        .then(None)
                        .otherwise(pl.col(col_name)) 
                        .alias(col_name)
                    )
                else:
                    print(f"  Warning: Column '{col_name}' is {df_harris[col_name].dtype}. Skipping processing for stats.")
            
            if cast_and_clean_expressions:
                df_harris = df_harris.with_columns(cast_and_clean_expressions)

            final_numeric_cols_for_stats = [
                col for col in numeric_cols_to_process 
                if col in df_harris.columns and df_harris[col].dtype == pl.Float64 
            ]
            
            if final_numeric_cols_for_stats:
                df_for_stats = df_harris.select(final_numeric_cols_for_stats)
                print("\nDescriptive Statistics for numeric variables (after handling missing values):")
                print(df_for_stats.describe())
            else:
                print("No numeric columns were successfully prepared for statistics after casting and cleaning attempts.")
        else:
            print("No variables specified in TARGET_VARIABLES were found in the loaded data for statistics.")

    except Exception as e:
        print(f"\nAn error occurred during data processing: {type(e).__name__} - {e}")

    print("\nScript finished.")

if __name__ == "__main__":
    analyze_local_data()
