import polars as pl
import numpy as np # For NaN comparison and isclose
import os

# --- Configuration ---
# Define the paths to your CSV files
# Ensure these filenames match exactly what was output by your previous scripts
LOCAL_CSV_PATH = "df_yield_with_local_weather_aggs.csv"
S3_CSV_PATH = "df_yield_with_weather_aggs_state_batched_v3.csv" # The one from S3 processing

# Key columns for joining and identifying rows
KEY_COLUMNS = ["fips_full", "year"]

# Weather variable columns to compare
WEATHER_VARIABLES = [
    "GDD_GS", "KDD_GS", "TMAX_AVG_GS", "PREC_GS", "CHD_GS",
    "GDD_SGF", "KDD_SGF", "TMAX_AVG_SGF", "PREC_SGF", "CHD_SGF"
]

# Tolerance for floating point comparisons
# If abs(value_local - value_s3) <= NUMERIC_TOLERANCE, they are considered "close enough"
NUMERIC_TOLERANCE = 0.01 # e.g., difference of 0.01 is acceptable

def compare_weather_csvs():
    """
    Loads two CSV files containing aggregated weather data, compares them,
    reports significant differences, notes values that are close but not exact,
    and reports the biggest observed difference for each variable.
    """
    print("--- Starting Comparison of Local vs. S3 Aggregated Weather Data ---")

    # --- Load Data ---
    if not os.path.exists(LOCAL_CSV_PATH):
        print(f"Error: Local CSV file not found at '{LOCAL_CSV_PATH}'")
        return
    if not os.path.exists(S3_CSV_PATH):
        print(f"Error: S3 CSV file not found at '{S3_CSV_PATH}'")
        return

    try:
        print(f"Loading local data: {LOCAL_CSV_PATH}")
        df_local = pl.read_csv(LOCAL_CSV_PATH, infer_schema_length=1000) 
        print(f"Loading S3 data: {S3_CSV_PATH}")
        df_s3 = pl.read_csv(S3_CSV_PATH, infer_schema_length=1000)
    except Exception as e:
        print(f"Error loading CSV files: {e}")
        return

    print(f"\nLocal DataFrame shape: {df_local.shape}")
    print(f"S3 DataFrame shape: {df_s3.shape}")

    # --- Select relevant columns and ensure correct types for join keys ---
    columns_to_keep = KEY_COLUMNS + WEATHER_VARIABLES
    
    missing_local_cols = [col for col in columns_to_keep if col not in df_local.columns]
    if missing_local_cols:
        print(f"Error: Missing expected columns in local CSV: {missing_local_cols}")
        return
        
    missing_s3_cols = [col for col in columns_to_keep if col not in df_s3.columns]
    if missing_s3_cols:
        print(f"Error: Missing expected columns in S3 CSV: {missing_s3_cols}")
        return

    df_local_selected = df_local.select(columns_to_keep).with_columns([
        pl.col("fips_full").cast(pl.String),
        pl.col("year").cast(pl.Int64)
    ])
    df_s3_selected = df_s3.select(columns_to_keep).with_columns([
        pl.col("fips_full").cast(pl.String),
        pl.col("year").cast(pl.Int64)
    ])
    
    for var in WEATHER_VARIABLES:
        df_local_selected = df_local_selected.with_columns(
            pl.col(var).cast(pl.Float64, strict=False)
        )
        df_s3_selected = df_s3_selected.with_columns(
            pl.col(var).cast(pl.Float64, strict=False)
        )

    # --- Merge DataFrames ---
    print("\nMerging local and S3 dataframes on 'fips_full' and 'year'...")
    try:
        df_merged = df_local_selected.join(
            df_s3_selected, on=KEY_COLUMNS, how="outer", suffix="_s3" 
        )
        print(f"Merged DataFrame shape: {df_merged.shape}")
    except Exception as e:
        print(f"Error during merge: {e}")
        return

    # --- Compare Weather Variables ---
    significant_differences = []
    close_but_not_exact_matches = []
    comparison_summary = {
        "total_comparisons": 0, "exact_matches": 0, 
        "matches_within_tolerance_not_exact": 0, "both_nan": 0,
        "missing_in_local": 0, "missing_in_s3": 0,
        "significant_numeric_diff": 0, "type_mismatch_errors": 0
    }
    
    # To store the biggest difference found for each variable
    max_differences_observed = {}

    print("\nComparing weather variables...")
    for row in df_merged.iter_rows(named=True):
        fips = row["fips_full"]
        year = row["year"]
        
        for var_base_name in WEATHER_VARIABLES:
            comparison_summary["total_comparisons"] += 1
            
            val_local_col = var_base_name 
            val_s3_col = f"{var_base_name}_s3"
            val_local = row.get(val_local_col)
            val_s3 = row.get(val_s3_col)

            local_is_null = val_local is None or (isinstance(val_local, float) and np.isnan(val_local))
            s3_is_null = val_s3 is None or (isinstance(val_s3, float) and np.isnan(val_s3))

            if local_is_null and s3_is_null:
                comparison_summary["both_nan"] += 1
            elif local_is_null:
                comparison_summary["missing_in_local"] += 1
                significant_differences.append({
                    "fips_full": fips, "year": year, "variable": var_base_name,
                    "value_local": "NaN/Missing", "value_s3": val_s3, "reason": "Missing in Local"
                })
            elif s3_is_null:
                comparison_summary["missing_in_s3"] += 1
                significant_differences.append({
                    "fips_full": fips, "year": year, "variable": var_base_name,
                    "value_local": val_local, "value_s3": "NaN/Missing", "reason": "Missing in S3"
                })
            else:
                try:
                    num_local = float(val_local) 
                    num_s3 = float(val_s3)     
                    current_abs_diff = abs(num_local - num_s3)

                    # Track maximum difference
                    if var_base_name not in max_differences_observed or current_abs_diff > max_differences_observed[var_base_name]["abs_difference"]:
                        max_differences_observed[var_base_name] = {
                            "fips_full": fips, "year": year, 
                            "value_local": num_local, "value_s3": num_s3,
                            "difference": num_local - num_s3,
                            "abs_difference": current_abs_diff
                        }

                    if num_local == num_s3: 
                        comparison_summary["exact_matches"] += 1
                    elif np.isclose(num_local, num_s3, atol=NUMERIC_TOLERANCE):
                        comparison_summary["matches_within_tolerance_not_exact"] += 1
                        close_but_not_exact_matches.append({
                            "fips_full": fips, "year": year, "variable": var_base_name,
                            "value_local": num_local, "value_s3": num_s3, 
                            "difference": num_local - num_s3
                        })
                    else: # Exceeds tolerance
                        comparison_summary["significant_numeric_diff"] += 1
                        significant_differences.append({
                            "fips_full": fips, "year": year, "variable": var_base_name,
                            "value_local": num_local, "value_s3": num_s3, 
                            "difference": num_local - num_s3, "reason": "Exceeds Tolerance"
                        })
                except (ValueError, TypeError): 
                     comparison_summary["type_mismatch_errors"] += 1
                     significant_differences.append({
                        "fips_full": fips, "year": year, "variable": var_base_name,
                        "value_local": val_local, "value_s3": val_s3, "reason": "Type Mismatch/Cannot Compare Numerically"
                    })

    # --- Report Results ---
    print("\n--- Comparison Summary ---")
    print(f"Total FIPS-year-variable combinations compared: {comparison_summary['total_comparisons']}")
    print(f"  Exact matches: {comparison_summary['exact_matches']}")
    print(f"  Matches within tolerance {NUMERIC_TOLERANCE} (but not exact): {comparison_summary['matches_within_tolerance_not_exact']}")
    # ... (rest of summary prints remain the same) ...
    print(f"  Both values NaN/Missing: {comparison_summary['both_nan']}")
    print(f"  Value missing in Local CSV only: {comparison_summary['missing_in_local']}")
    print(f"  Value missing in S3 CSV only: {comparison_summary['missing_in_s3']}")
    print(f"  Significant numeric differences (exceeding tolerance): {comparison_summary['significant_numeric_diff']}")
    if comparison_summary['type_mismatch_errors'] > 0:
        print(f"  Type mismatch/comparison errors: {comparison_summary['type_mismatch_errors']}")

    if close_but_not_exact_matches:
        print(f"\n--- Found {len(close_but_not_exact_matches)} Close (Within Tolerance {NUMERIC_TOLERANCE}, Not Exact) Matches ---")
        # ... (printing logic for close_matches remains the same) ...
        df_close_matches = pl.DataFrame(close_but_not_exact_matches)
        if df_close_matches.height > 20:
            print(df_close_matches.head(10))
            print(f"... and {df_close_matches.height - 10} more.")
        else:
            print(df_close_matches)

    if significant_differences:
        print(f"\n--- Found {len(significant_differences)} Significant Differences (Missing values or Exceeding Tolerance) ---")
        # ... (printing logic for significant_differences remains the same) ...
        df_differences = pl.DataFrame(significant_differences)
        if df_differences.height > 20:
            print(df_differences.head(10))
            print(f"... and {df_differences.height - 10} more.")
        else:
            print(df_differences)
    
    # --- Report Biggest Observed Differences ---
    if max_differences_observed:
        print("\n--- Biggest Observed Differences for Each Variable (Non-Null Comparisons) ---")
        for var_name, details in max_differences_observed.items():
            print(f"  Variable: {var_name}")
            print(f"    Max Abs Difference: {details['abs_difference']:.4f}")
            print(f"    Occurred at FIPS: {details['fips_full']}, Year: {details['year']}")
            print(f"    Value Local: {details['value_local']:.4f}, Value S3: {details['value_s3']:.4f}")
    else:
        print("\nNo numeric differences were observed to report maximums (all values might have been missing or identical).")


    # Final conclusion message
    if not significant_differences and not close_but_not_exact_matches and comparison_summary['missing_in_local'] == 0 and comparison_summary['missing_in_s3'] == 0:
        print("\nNo discrepancies (significant, close, or missing) found between the two datasets. Excellent!")
    elif not significant_differences:
         print("\nNo significant differences (exceeding tolerance) found, though there might be missing values or close matches noted above.")

    print("\n--- Comparison Script Finished ---")

if __name__ == "__main__":
    compare_weather_csvs()