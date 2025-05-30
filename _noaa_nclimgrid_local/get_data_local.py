# get_data_local.py

# --- Standard Library Imports ---
import os
import time 
from typing import List, Dict, Optional, Any, Tuple 
from collections import defaultdict

# --- Third-party Library Imports ---
import polars as pl
from polars.exceptions import ColumnNotFoundError
import numpy as np # For NaN

# --- Custom Module Imports ---
# We are not using nclimgrid_importer for local files
import sys
# Add parent directory to Python path
sys.path.append(os.path.abspath(".."))
# Now import config
import config


# --- Chunk 1: Load and Prepare Input CSV ---
def load_and_prepare_yield_data(csv_path: str) -> Optional[Tuple[pl.DataFrame, List[str], int, int]]:
    """
    Loads the yield data CSV, prepares FIPS codes, and extracts unique FIPS and year range.
    """
    if not os.path.exists(csv_path):
        print(f"Error: Input CSV file not found at '{csv_path}'")
        return None
    try:
        yield_column_dtypes = {
            "state": pl.String, "county": pl.String, "year": pl.Int64
        }
        df_yield = pl.read_csv(csv_path, schema_overrides=yield_column_dtypes, null_values=["NA", "N/A", "", " "])
        print(f"\nSuccessfully loaded input CSV: '{csv_path}', Shape: {df_yield.shape}")
        required_cols = ["state", "county", "year"]
        for col_name_req in required_cols:
            if col_name_req not in df_yield.columns:
                print(f"Error: CSV must contain a '{col_name_req}' column.")
                return None
        df_yield = df_yield.drop_nulls(subset=["state", "county", "year"])
        if df_yield.height == 0:
            print("Error: No valid rows in CSV after dropping nulls in state, county, or year.")
            return None
        df_yield = df_yield.with_columns(
            pl.col("county").str.zfill(3).alias("county_padded"),
            (pl.col("state") + pl.col("county").str.zfill(3)).alias("fips_full")
        )
        unique_fips_to_process = df_yield.get_column("fips_full").unique().sort().to_list()
        print(f"\nFound {len(unique_fips_to_process)} unique FIPS codes to process from yield data.")
        if not unique_fips_to_process:
            print("Error: No unique FIPS codes found.")
            return None
        min_year_csv = df_yield.select(pl.col("year").min()).item()
        max_year_csv = df_yield.select(pl.col("year").max()).item()
        print(f"Year range in CSV: {min_year_csv} - {max_year_csv}")
        return df_yield, unique_fips_to_process, min_year_csv, max_year_csv
    except Exception as e:
        print(f"Error loading or processing input CSV: {type(e).__name__} - {e}")
        return None

# --- Chunk 2 (Modified): Local Daily Weather Data Fetching and Cleaning ---
def load_and_clean_local_daily_weather(
    fips_codes_to_filter: List[str], 
    base_data_path: str, 
    start_year_to_scan: int, 
    end_year_to_scan: int
) -> Tuple[Optional[pl.DataFrame], List[Dict[str, str]]]:
    """
    Scans local Parquet files within year folders for a range of years,
    filters for specified FIPS codes, and performs initial cleaning.
    """
    print(f"\nScanning local daily weather for FIPS list (count: {len(fips_codes_to_filter)}), Years: {start_year_to_scan}-{end_year_to_scan}")
    
    all_parquet_files = []
    file_scan_errors: List[Dict[str, str]] = []

    for year_val in range(start_year_to_scan, end_year_to_scan + 1):
        year_folder = os.path.join(base_data_path, str(year_val))
        if os.path.isdir(year_folder):
            for root, _, files in os.walk(year_folder):
                for file in files:
                    if file.endswith(".parquet"):
                        all_parquet_files.append(os.path.join(root, file))
        else:
            # print(f"  Warning: Year folder not found: {year_folder}") # Less verbose
            for month_num in range(1,13):
                 yyyymm_str = f"{year_val}{month_num:02d}" 
                 file_scan_errors.append({
                     'yyyymm': yyyymm_str, 'file_path': year_folder, 
                     'error_type': 'YearFolderNotFound', 'error_message': f"Year folder {year_folder} not found."
                 })

    if not all_parquet_files:
        print(f"  No Parquet files found in local directories for years {start_year_to_scan}-{end_year_to_scan}.")
        return None, file_scan_errors
    
    # print(f"  Found {len(all_parquet_files)} total Parquet files to scan in the specified year range.") # Less verbose

    try:
        # print("  Scanning all found Parquet files and filtering by FIPS...") # Less verbose
        df_daily_weather = pl.scan_parquet(all_parquet_files).filter(
            pl.col("fips").is_in(fips_codes_to_filter)
        ).collect()
        
        if df_daily_weather.height == 0:
            # print(f"  No data found for the FIPS list {fips_codes_to_filter} in the scanned files.") # Less verbose
            return None, file_scan_errors

        # print(f"  Successfully loaded {df_daily_weather.height} daily records for the FIPS list.") # Less verbose

    except Exception as e:
        print(f"  Error during pl.scan_parquet or initial filter for FIPS list {fips_codes_to_filter}: {type(e).__name__} - {e}")
        file_scan_errors.append({
            'yyyymm': 'BATCH_READ_ERROR', 'file_path': 'Multiple local files',
            'error_type': type(e).__name__, 'error_message': str(e)
        })
        return None, file_scan_errors

    if "date" not in df_daily_weather.columns:
        print(f"  Error: 'date' column missing. Returning None & errors.")
        return None, file_scan_errors
    if df_daily_weather["date"].dtype != pl.Datetime:
        try:
            df_daily_weather = df_daily_weather.with_columns(pl.col("date").cast(pl.Datetime))
        except Exception as e:
            print(f"  Error casting 'date' column for FIPS list: {e}. Returning raw table & errors.")
            return df_daily_weather, file_scan_errors 
    
    cast_and_clean_expressions = []
    for col_name in config.TARGET_DAILY_WEATHER_VARIABLES:
        if col_name in df_daily_weather.columns:
            if df_daily_weather[col_name].dtype == pl.String:
                cast_and_clean_expressions.append(
                    pl.when(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
                    .then(None).otherwise(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False)).alias(col_name)
                )
            elif df_daily_weather[col_name].dtype == pl.Float64:
                cast_and_clean_expressions.append(
                    pl.when(pl.col(col_name) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
                    .then(None).otherwise(pl.col(col_name)).alias(col_name)
                )
            elif df_daily_weather[col_name].dtype.is_numeric():
                cast_and_clean_expressions.append(
                    pl.when(pl.col(col_name).cast(pl.Float64, strict=False) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
                    .then(None).otherwise(pl.col(col_name).cast(pl.Float64, strict=False)).alias(col_name)
                )
    if cast_and_clean_expressions:
        try:
            df_daily_weather = df_daily_weather.with_columns(cast_and_clean_expressions)
            # print("  Successfully applied cleaning and casting to weather variable columns.") # Less verbose
        except Exception as e:
            print(f"  Error applying casting/cleaning for FIPS list: {e}. Returning raw table & errors.")
            return df_daily_weather, file_scan_errors 
            
    final_columns_to_keep = ["date", "fips"] 
    if "state_name" in df_daily_weather.columns:
        final_columns_to_keep.append("state_name")
    for var_name in config.TARGET_DAILY_WEATHER_VARIABLES:
        if var_name in df_daily_weather.columns and df_daily_weather[var_name].dtype == pl.Float64:
            if var_name not in final_columns_to_keep: final_columns_to_keep.append(var_name)
        elif var_name in df_daily_weather.columns: 
             # print(f"  Warning: Column '{var_name}' exists but is not Float64 after processing ({df_daily_weather[var_name].dtype}).") # Less verbose
             if var_name not in final_columns_to_keep : final_columns_to_keep.append(var_name) 
    final_columns_to_keep = list(dict.fromkeys(final_columns_to_keep))
    
    actual_cols_in_df = [col for col in final_columns_to_keep if col in df_daily_weather.columns]
    if not actual_cols_in_df or "date" not in actual_cols_in_df or "fips" not in actual_cols_in_df :
         print(f"  Error: Essential columns ('date', 'fips') or target variables are missing after processing for FIPS list. Available: {df_daily_weather.columns}")
         return None, file_scan_errors

    df_daily_weather = df_daily_weather.select(actual_cols_in_df)
    # print(f"  Cleaned daily weather data for FIPS list has shape: {df_daily_weather.shape}") # Less verbose
    return df_daily_weather, file_scan_errors


# --- Chunk 3 & 4 (Agronomic Calculations & Yearly Aggregation - Unchanged) ---
def calculate_daily_gdd(tavg_series: pl.Series) -> pl.Series:
    tavg_float = tavg_series.cast(pl.Float64, strict=False) 
    capped_tavg = pl.min_horizontal(tavg_float, pl.lit(config.GDD_MAX_TEMP_C, dtype=pl.Float64))
    gdd_potential = capped_tavg - config.GDD_BASE_TEMP_C
    daily_gdd = pl.max_horizontal(pl.lit(0.0, dtype=pl.Float64), gdd_potential)
    return daily_gdd 

def calculate_daily_kdd(tmax_series: pl.Series) -> pl.Series:
    tmax_float = tmax_series.cast(pl.Float64, strict=False)
    kdd_potential = tmax_float - config.KDD_TEMP_THRESHOLD_C
    daily_kdd = pl.max_horizontal(pl.lit(0.0, dtype=pl.Float64), kdd_potential)
    return daily_kdd

def calculate_daily_chd(tmax_series: pl.Series, prcp_series: pl.Series) -> pl.Series:
    tmax_float = tmax_series.cast(pl.Float64, strict=False)
    prcp_float = prcp_series.cast(pl.Float64, strict=False)
    is_hot = (tmax_float > config.CHD_TEMP_THRESHOLD_C).fill_null(False)
    is_dry = (prcp_float < config.CHD_PRECIP_THRESHOLD_MM).fill_null(False)
    daily_chd = (is_hot & is_dry).cast(pl.Int8) 
    return daily_chd

def _calculate_seasonal_aggregates(df_season: pl.DataFrame, season_prefix: str) -> Dict[str, Any]:
    aggs: Dict[str, Any] = {}
    df_season_with_daily_calcs = df_season.clone() 
    if "tavg" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("tavg").is_null().all():
        if df_season_with_daily_calcs.get_column("tavg").is_null().any(): aggs[f"GDD_{season_prefix}"] = np.nan
        else:
            df_season_with_daily_calcs = df_season_with_daily_calcs.with_columns(calculate_daily_gdd(df_season_with_daily_calcs.get_column("tavg")).alias("_daily_gdd"))
            aggs[f"GDD_{season_prefix}"] = df_season_with_daily_calcs.get_column("_daily_gdd").sum()
    else: aggs[f"GDD_{season_prefix}"] = np.nan
    if "tmax" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("tmax").is_null().all():
        if df_season_with_daily_calcs.get_column("tmax").is_null().any():
            aggs[f"KDD_{season_prefix}"] = np.nan
            aggs[f"TMAX_AVG_{season_prefix}"] = np.nan
        else:
            df_season_with_daily_calcs = df_season_with_daily_calcs.with_columns(calculate_daily_kdd(df_season_with_daily_calcs.get_column("tmax")).alias("_daily_kdd"))
            aggs[f"KDD_{season_prefix}"] = df_season_with_daily_calcs.get_column("_daily_kdd").sum()
            aggs[f"TMAX_AVG_{season_prefix}"] = df_season_with_daily_calcs.get_column("tmax").mean()
    else:
        aggs[f"KDD_{season_prefix}"] = np.nan
        aggs[f"TMAX_AVG_{season_prefix}"] = np.nan
    if "prcp" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("prcp").is_null().all():
        if df_season_with_daily_calcs.get_column("prcp").is_null().any(): aggs[f"PREC_{season_prefix}"] = np.nan
        else: aggs[f"PREC_{season_prefix}"] = df_season_with_daily_calcs.get_column("prcp").sum()
    else: aggs[f"PREC_{season_prefix}"] = np.nan
    if ("tmax" in df_season_with_daily_calcs.columns and "prcp" in df_season_with_daily_calcs.columns and
        not df_season_with_daily_calcs.get_column("tmax").is_null().all() and 
        not df_season_with_daily_calcs.get_column("prcp").is_null().all()):
        if df_season_with_daily_calcs.get_column("tmax").is_null().any() or df_season_with_daily_calcs.get_column("prcp").is_null().any():
            aggs[f"CHD_{season_prefix}"] = np.nan
        else:
            df_season_with_daily_calcs = df_season_with_daily_calcs.with_columns(calculate_daily_chd(df_season_with_daily_calcs.get_column("tmax"), df_season_with_daily_calcs.get_column("prcp")).alias("_daily_chd"))
            aggs[f"CHD_{season_prefix}"] = df_season_with_daily_calcs.get_column("_daily_chd").sum()
    else: aggs[f"CHD_{season_prefix}"] = np.nan
    return aggs

def aggregate_weather_to_yearly(daily_df_for_year: pl.DataFrame, year: int) -> Dict[str, Any]:
    all_aggs: Dict[str, Any] = {"year": year} 
    gs_start_date = pl.datetime(year, config.GS_START_MONTH, config.GS_START_DAY)
    gs_end_date = pl.datetime(year, config.GS_END_MONTH, config.GS_END_DAY)
    df_gs = daily_df_for_year.filter((pl.col("date") >= gs_start_date) & (pl.col("date") <= gs_end_date))
    gs_essential_cols = [col for col in config.TARGET_DAILY_WEATHER_VARIABLES if col != 'date']
    gs_essential_cols_present = all(col in df_gs.columns for col in gs_essential_cols)
    if df_gs.height == 0 or not gs_essential_cols_present:
        gs_aggs = {f"GDD_GS": np.nan, f"KDD_GS": np.nan, f"TMAX_AVG_GS": np.nan, f"PREC_GS": np.nan, f"CHD_GS": np.nan}
    else: gs_aggs = _calculate_seasonal_aggregates(df_gs, "GS")
    all_aggs.update(gs_aggs)
    sgf_start_date = pl.datetime(year, config.SGF_START_MONTH, config.SGF_START_DAY)
    sgf_end_date = pl.datetime(year, config.SGF_END_MONTH, config.SGF_END_DAY)
    df_sgf = daily_df_for_year.filter((pl.col("date") >= sgf_start_date) & (pl.col("date") <= sgf_end_date))
    sgf_essential_cols_present = all(col in df_sgf.columns for col in gs_essential_cols)
    if df_sgf.height == 0 or not sgf_essential_cols_present:
        sgf_aggs = {f"GDD_SGF": np.nan, f"KDD_SGF": np.nan, f"TMAX_AVG_SGF": np.nan, f"PREC_SGF": np.nan, f"CHD_SGF": np.nan}
    else: sgf_aggs = _calculate_seasonal_aggregates(df_sgf, "SGF")
    all_aggs.update(sgf_aggs)
    return all_aggs

def main():
    overall_start_time = time.time()
    print("--- Starting Agricultural Weather Data Processing (Local Parquet Files) ---")
    print(f"Target daily weather variables: {config.TARGET_DAILY_WEATHER_VARIABLES}")
    print(f"Weather data processing will start from year: {config.DATA_START_YEAR}")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script directory: {script_dir}")

    ## ----- GET THE DATA ------

    path_df = "../_noaa_climdiv_local/"
    input_csv_filename = "df_yield_climdiv.csv" 
    input_csv_path = os.path.join(path_df, input_csv_filename)
    
    output_csv_filename = "df_final_local.csv" 
    output_csv_path = os.path.join(script_dir, output_csv_filename)
    
    ## ---- START THE MAIN ----
    
    prepared_data = load_and_prepare_yield_data(input_csv_path)
    if prepared_data is None:
        print("Failed to load and prepare yield data. Exiting.")
        return
    df_yield, unique_fips_to_process, min_year_csv, max_year_csv = prepared_data
    
    local_data_base_path = script_dir 
    
    scan_start_year = config.DATA_START_YEAR
    scan_end_year = max_year_csv 
    print(f"Scanning local Parquet files in year folders from {scan_start_year} to {scan_end_year} (relative to script dir).")

    all_yearly_weather_aggregates: List[Dict[str, Any]] = []
    all_local_file_load_errors: Dict[str, List[Dict[str,str]]] = {} 

    fips_by_state = defaultdict(list)
    for fips in unique_fips_to_process:
        state_fips_prefix = fips[:2]
        fips_by_state[state_fips_prefix].append(fips)
    
    num_states = len(fips_by_state)
    print(f"\n--- Starting Main Processing Loop for {num_states} states ---")

    for state_idx, (state_fips_prefix, fips_in_state_list) in enumerate(fips_by_state.items()):
        state_start_time = time.time()
        print(f"\nProcessing State Group {state_idx+1}/{num_states} (State FIPS Prefix: {state_fips_prefix}, {len(fips_in_state_list)} counties)")

        cleaned_df_for_state_batch, errors_for_this_state_load = load_and_clean_local_daily_weather(
            fips_codes_to_filter=fips_in_state_list, 
            base_data_path=local_data_base_path, 
            start_year_to_scan=scan_start_year, 
            end_year_to_scan=scan_end_year 
        )

        if errors_for_this_state_load: 
            print(f"  Local File Loading/Scanning Errors for state batch (FIPS prefix {state_fips_prefix}):")
            for fips_in_batch_for_error_log in fips_in_state_list: 
                 if fips_in_batch_for_error_log not in all_local_file_load_errors:
                    all_local_file_load_errors[fips_in_batch_for_error_log] = []
                 all_local_file_load_errors[fips_in_batch_for_error_log].extend(
                    [err for err in errors_for_this_state_load if err['error_type'] == 'YearFolderNotFound' or err['error_type'] == 'BATCH_READ_ERROR']
                )
            for error_info in errors_for_this_state_load[:3]: 
                print(f"    - File/Path {error_info.get('file_path', 'N/A')[-40:]}: {error_info['error_type']} - {error_info['error_message'][:100]}...")
            if len(errors_for_this_state_load) > 3:
                print(f"    ... and {len(errors_for_this_state_load) - 3} more loading errors for this state batch.")
        
        if cleaned_df_for_state_batch is None or cleaned_df_for_state_batch.height == 0:
            print(f"  No cleaned daily weather data available for FIPS in state {state_fips_prefix}. Aggregates will be NaN.")
            for fips_code_in_batch in fips_in_state_list:
                df_fips_yield_years = df_yield.filter(pl.col("fips_full") == fips_code_in_batch)
                min_year_for_this_fips = df_fips_yield_years.select(pl.col("year").min()).item()
                max_year_for_this_fips_indiv = df_fips_yield_years.select(pl.col("year").max()).item()
                for year_to_fill in range(min_year_for_this_fips, max_year_for_this_fips_indiv + 1): 
                    nan_aggs = {"fips_full": fips_code_in_batch, "year": year_to_fill}
                    for prefix in ["GS", "SGF"]:
                        nan_aggs.update({
                            f"GDD_{prefix}": np.nan, f"KDD_{prefix}": np.nan, f"TMAX_AVG_{prefix}": np.nan,
                            f"PREC_{prefix}": np.nan, f"CHD_{prefix}": np.nan 
                        })
                    all_yearly_weather_aggregates.append(nan_aggs)
        else:
            cleaned_df_for_state_batch = cleaned_df_for_state_batch.with_columns(
                pl.col("date").dt.year().alias("year_daily") 
            )
            for fips_code in fips_in_state_list:
                df_single_fips_daily_data = cleaned_df_for_state_batch.filter(pl.col("fips") == fips_code)
                df_fips_yield_years = df_yield.filter(pl.col("fips_full") == fips_code)
                min_year_for_this_fips = df_fips_yield_years.select(pl.col("year").min()).item()
                max_year_for_this_fips_indiv = df_fips_yield_years.select(pl.col("year").max()).item()
                relevant_yield_years_for_fips = sorted(
                    df_fips_yield_years.filter(pl.col("year") >= scan_start_year) 
                                        .select("year").unique().to_series().to_list()
                )
                if df_single_fips_daily_data.height == 0 and relevant_yield_years_for_fips:
                    for year_to_aggregate in relevant_yield_years_for_fips:
                        nan_aggs = {"fips_full": fips_code, "year": year_to_aggregate, **{f"GDD_GS": np.nan, f"KDD_GS": np.nan, f"TMAX_AVG_GS": np.nan, f"PREC_GS": np.nan, f"CHD_GS": np.nan}, **{f"GDD_SGF": np.nan, f"KDD_SGF": np.nan, f"TMAX_AVG_SGF": np.nan, f"PREC_SGF": np.nan, f"CHD_SGF": np.nan}}
                        all_yearly_weather_aggregates.append(nan_aggs)
                elif relevant_yield_years_for_fips:
                    for year_to_aggregate in relevant_yield_years_for_fips:
                        daily_data_for_this_year_fips = df_single_fips_daily_data.filter(pl.col("year_daily") == year_to_aggregate)
                        if daily_data_for_this_year_fips.height == 0:
                            yearly_aggs = {"fips_full": fips_code, "year": year_to_aggregate, **{f"GDD_GS": np.nan, f"KDD_GS": np.nan, f"TMAX_AVG_GS": np.nan, f"PREC_GS": np.nan, f"CHD_GS": np.nan}, **{f"GDD_SGF": np.nan, f"KDD_SGF": np.nan, f"TMAX_AVG_SGF": np.nan, f"PREC_SGF": np.nan, f"CHD_SGF": np.nan}}
                        else:
                            yearly_aggs = aggregate_weather_to_yearly(daily_data_for_this_year_fips, year_to_aggregate)
                            yearly_aggs["fips_full"] = fips_code 
                        all_yearly_weather_aggregates.append(yearly_aggs)
            for fips_code in fips_in_state_list:
                df_fips_yield_years = df_yield.filter(pl.col("fips_full") == fips_code)
                pre_data_yield_years = sorted(
                    df_fips_yield_years.filter(pl.col("year") < scan_start_year) 
                                        .select("year").unique().to_series().to_list()
                )
                for year_to_fill_nan in pre_data_yield_years:
                    nan_aggs = {"fips_full": fips_code, "year": year_to_fill_nan, **{f"GDD_GS": np.nan, f"KDD_GS": np.nan, f"TMAX_AVG_GS": np.nan, f"PREC_GS": np.nan, f"CHD_GS": np.nan}, **{f"GDD_SGF": np.nan, f"KDD_SGF": np.nan, f"TMAX_AVG_SGF": np.nan, f"PREC_SGF": np.nan, f"CHD_SGF": np.nan}}
                    all_yearly_weather_aggregates.append(nan_aggs)
        state_end_time = time.time()
        print(f"  Finished processing state batch (FIPS prefix {state_fips_prefix}) in {state_end_time - state_start_time:.2f} seconds.")

    # --- Chunk 6: Merge results and save ---
    if not all_yearly_weather_aggregates:
        print("\nNo yearly weather aggregates were generated. Cannot merge or save.")
    else:
        # Define the schema for the aggregated weather DataFrame
        # All agronomic variables should be Float64 to accommodate NaNs
        aggs_schema = {
            "fips_full": pl.String, "year": pl.Int64,
            "GDD_GS": pl.Float64, "KDD_GS": pl.Float64, "TMAX_AVG_GS": pl.Float64,
            "PREC_GS": pl.Float64, "CHD_GS": pl.Float64, # CHD_GS is a sum, could be Int but Float64 is safer for NaNs
            "GDD_SGF": pl.Float64, "KDD_SGF": pl.Float64, "TMAX_AVG_SGF": pl.Float64,
            "PREC_SGF": pl.Float64, "CHD_SGF": pl.Float64
        }
        df_final_weather_aggs = pl.DataFrame(all_yearly_weather_aggregates, schema=aggs_schema)
        
        print(f"\n--- Processing Complete: Total yearly aggregate records: {df_final_weather_aggs.height} ---")
        # print("Sample of aggregated weather data (first 5 rows):")
        # print(df_final_weather_aggs.head(5))
        
        print("\n--- Merging weather aggregates with yield data ---")
        if "fips_full" not in df_yield.columns or df_yield["fips_full"].dtype != pl.String:
             df_yield = df_yield.with_columns( # Recreate if missing or wrong type
                (pl.col("state").cast(pl.String) + pl.col("county").cast(pl.String).str.zfill(3)).alias("fips_full")
            )
        if df_yield["year"].dtype != pl.Int64:
             df_yield = df_yield.with_columns(pl.col("year").cast(pl.Int64))
        
        df_merged = df_yield.join(
            df_final_weather_aggs,
            on=["fips_full", "year"],
            how="left"
        )
        print(f"\nShape of merged DataFrame: {df_merged.shape}")
        # print("Sample of merged DataFrame (first 5 rows with new columns):")
        # cols_to_show = ["year", "fips_full"] + [col for col in df_final_weather_aggs.columns if col not in ["fips_full", "year"]]
        # cols_to_show = [col for col in cols_to_show if col in df_merged.columns] 
        # if cols_to_show: print(df_merged.head(5).select(cols_to_show))

        print(f"\nSaving merged data to: {output_csv_path}")
        try:
            df_merged.write_csv(output_csv_path)
            print(f"Successfully saved data to {output_csv_path}")
        except Exception as e:
            print(f"Error saving merged data to CSV: {e}")

    if all_local_file_load_errors: 
        print("\n--- Summary of Local File Loading/Scanning Errors Encountered ---")
        for fips_key, errors_list in all_local_file_load_errors.items():
            print(f"  FIPS {fips_key} (or batch containing it): {len(errors_list)} unique file/path errors reported.")
    else:
        print("\nNo local file loading/scanning errors were reported across all FIPS processing.")

    overall_end_time = time.time()
    print(f"\n--- Agricultural Weather Data Processing (Local Files) Finished in {overall_end_time - overall_start_time:.2f} seconds ---")

if __name__ == "__main__":
    main()