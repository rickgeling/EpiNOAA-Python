import polars as pl
import pyarrow.parquet as pq # For direct PyArrow read if needed for comparison
import os

# --- Define Parameters (mirroring what your main script would use) ---
# Variables you were trying to select and process
TARGET_VARIABLES = ["tmin", "tmax", "tavg", "prcp"] 
MISSING_VALUE_PLACEHOLDER = -999.99

def debug_single_local_parquet(file_path: str, current_target_fips: str):
    """
    Reads a single local Parquet file, attempts to filter it for a specific FIPS,
    clean numeric columns, and prints information to help debug read/processing errors.
    
    Args:
        file_path (str): The full path to the .parquet file.
        current_target_fips (str): The 5-digit FIPS code to filter for.
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found at '{file_path}'")
        return

    print(f"--- Debugging Local Parquet File: {file_path} ---")
    print(f"--- Targeting FIPS: {current_target_fips} ---")

    try:
        # --- Method 1: Read with Polars (uses PyArrow backend) ---
        print("\nAttempting to read with Polars (pl.read_parquet)...")
        df = pl.read_parquet(file_path)

        print("\nFull Schema of the local file:")
        for col_name, dtype in df.schema.items():
            print(f"  - {col_name}: {dtype}")
        print(f"Shape (rows, columns): {df.shape}")
        print("\nFirst 3 rows (overall):")
        print(df.head(3))

        # Check if required 'fips' column exists for filtering
        if "fips" not in df.columns:
            print("\nError: 'fips' column not found. Cannot apply FIPS filter.")
        else:
            print(f"\nFiltering for fips: '{current_target_fips}'...")
            df_filtered = df.filter(pl.col("fips") == current_target_fips)
            print(f"  Number of rows after filtering for FIPS '{current_target_fips}': {df_filtered.height}")
            if df_filtered.height > 0:
                print("  First 3 rows of FIPS-filtered data:")
                print(df_filtered.head(3))
            else:
                print(f"  No data found for FIPS {current_target_fips} in this file.")
                # We can still try to cast on the unfiltered df to check for casting issues generally.

        # --- Attempt to cast and clean TARGET_VARIABLES on the original DataFrame (df) ---
        # This helps see if casting itself is an issue with this file's data
        print(f"\nAttempting to cast and clean TARGET_VARIABLES: {TARGET_VARIABLES} on the full unfiltered data...")
        cast_and_clean_expressions = []
        columns_successfully_prepared = []

        for col_name in TARGET_VARIABLES:
            if col_name in df.columns:
                original_dtype = df[col_name].dtype
                print(f"  Processing column '{col_name}' (original dtype: {original_dtype}):")
                if original_dtype == pl.String:
                    expr = (
                        pl.when(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False) == MISSING_VALUE_PLACEHOLDER)
                        .then(None) 
                        .otherwise(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False))
                        .alias(col_name)
                    )
                    cast_and_clean_expressions.append(expr)
                    columns_successfully_prepared.append(col_name)
                    print(f"    Will attempt strip, cast to Float64, and replace {MISSING_VALUE_PLACEHOLDER}.")
                elif original_dtype == pl.Float64:
                    expr = (
                        pl.when(pl.col(col_name) == MISSING_VALUE_PLACEHOLDER)
                        .then(None)
                        .otherwise(pl.col(col_name))
                        .alias(col_name)
                    )
                    cast_and_clean_expressions.append(expr)
                    columns_successfully_prepared.append(col_name)
                    print(f"    Already Float64, will replace {MISSING_VALUE_PLACEHOLDER}.")
                elif original_dtype.is_numeric():
                     expr = (
                        pl.when(pl.col(col_name).cast(pl.Float64, strict=False) == MISSING_VALUE_PLACEHOLDER) 
                        .then(None)
                        .otherwise(pl.col(col_name).cast(pl.Float64, strict=False)) # Ensure it's float for describe
                        .alias(col_name)
                    )
                     cast_and_clean_expressions.append(expr)
                     columns_successfully_prepared.append(col_name)
                     print(f"    Is numeric ({original_dtype}), will cast to Float64 and replace {MISSING_VALUE_PLACEHOLDER}.")
                else:
                    print(f"    Warning: Column '{col_name}' is type {original_dtype}. Skipping cleaning for stats.")
            else:
                print(f"  Warning: Target variable column '{col_name}' not found in this file.")
        
        if cast_and_clean_expressions:
            df_processed = df.with_columns(cast_and_clean_expressions)
            print("\n  Schema after attempting to process TARGET_VARIABLES:")
            for col_name, dtype in df_processed.schema.items():
                if col_name in TARGET_VARIABLES: print(f"    - {col_name}: {dtype}")
            
            if columns_successfully_prepared:
                print("\n  Descriptive statistics for processed TARGET_VARIABLES (from full unfiltered file):")
                # Ensure we only select columns that were successfully prepared and are in df_processed
                final_cols_for_stats = [col for col in columns_successfully_prepared if col in df_processed.columns]
                if final_cols_for_stats:
                    print(df_processed.select(final_cols_for_stats).describe())
                else:
                    print("    No columns available for describe after processing.")
            else:
                print("\n  No target variables were successfully prepared for statistics.")
        else:
            print("\n  No cast/clean expressions generated for TARGET_VARIABLES.")

        # --- Method 2: Try direct PyArrow read (for comparison if Polars had issues) ---
        print("\nAttempting direct read with pyarrow.parquet.read_table()...")
        arrow_table = pq.read_table(file_path)
        print(f"  Successfully read with PyArrow! Schema: {arrow_table.schema}")
        print(f"  Number of rows: {arrow_table.num_rows}")

    except Exception as e:
        print(f"\nCRITICAL ERROR processing file {file_path}: {type(e).__name__} - {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # --- IMPORTANT: Set this to the path of your downloaded Parquet file ---
    local_parquet_file = "202307.parquet" # Assuming it's in the same directory as this script
    # Example: local_parquet_file = "C:/path/to/your/downloaded/202307.parquet"
    
    # --- Set the FIPS code that was causing an issue in your main script ---
    fips_to_debug = "31109" 

    print(f"Debugging file: {local_parquet_file}")
    print(f"For FIPS: {fips_to_debug}")
    
    debug_single_local_parquet(local_parquet_file, fips_to_debug)
