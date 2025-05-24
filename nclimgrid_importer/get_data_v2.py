# process_ag_weather_batched.py

# --- Standard Library Imports ---
import os
import time 
from typing import List, Dict, Optional, Any, Tuple 

# --- Third-party Library Imports ---
import polars as pl
from polars.exceptions import ColumnNotFoundError
import numpy as np # For NaN

# --- Custom Module Imports ---
from nclimgrid_importer import load_nclimgrid_data # From python_script_s3_debug
import config

# --- Chunks 1, 3, 4 (Helper functions - mostly unchanged) ---
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

def fetch_and_clean_daily_weather_batched( # Renamed for clarity
    fips_codes_batch: List[str], # Accepts a list of FIPS codes
    start_year: int, 
    end_year: int
) -> Tuple[Optional[pl.DataFrame], List[Dict[str, str]]]:
    """
    Fetches daily weather data for a batch of FIPS codes and year range,
    then performs initial cleaning.
    """
    print(f"\nFetching daily weather for FIPS batch (count: {len(fips_codes_batch)}), Years: {start_year}-{end_year}")
    # print(f"  FIPS in batch: {fips_codes_batch}") # Can be verbose
    
    start_date_str = f"{start_year}-01-01"
    end_date_str = f"{end_year}-12-31"

    daily_arrow_table, file_load_errors = load_nclimgrid_data(
        start_date=start_date_str, end_date=end_date_str, spatial_scale='cty', 
        scaled=True, counties=fips_codes_batch, # Pass the batch of FIPS codes
        variables=config.TARGET_DAILY_WEATHER_VARIABLES 
    )

    if daily_arrow_table is None or daily_arrow_table.num_rows == 0:
        print(f"  No daily weather data returned or data was empty for FIPS batch for years {start_year}-{end_year}.")
        return None, file_load_errors 
    
    print(f"  Successfully fetched {daily_arrow_table.num_rows} daily records from S3 for FIPS batch.")
    df_daily_weather = pl.from_arrow(daily_arrow_table)

    if "date" not in df_daily_weather.columns:
        print(f"  Error: 'date' column missing for FIPS batch. Returning None & errors.")
        return None, file_load_errors
    if df_daily_weather["date"].dtype != pl.Datetime:
        try:
            df_daily_weather = df_daily_weather.with_columns(pl.col("date").cast(pl.Datetime))
        except Exception as e:
            print(f"  Error casting 'date' column for FIPS batch: {e}. Returning raw table & errors.")
            return df_daily_weather, file_load_errors 
    
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
        except Exception as e:
            print(f"  Error applying casting/cleaning for FIPS batch: {e}. Returning raw table & errors.")
            return df_daily_weather, file_load_errors 
            
    final_columns_to_keep = ["date", "fips"] # Always keep fips to identify data within the batch
    # Add state_name if present, as it's useful for context, though not strictly for filtering if full FIPS is used
    if "state_name" in df_daily_weather.columns:
        final_columns_to_keep.append("state_name")

    for var_name in config.TARGET_DAILY_WEATHER_VARIABLES:
        if var_name in df_daily_weather.columns and df_daily_weather[var_name].dtype == pl.Float64:
            final_columns_to_keep.append(var_name)
        elif var_name in df_daily_weather.columns: 
             if var_name not in final_columns_to_keep : final_columns_to_keep.append(var_name)
    final_columns_to_keep = list(dict.fromkeys(final_columns_to_keep))
    
    missing_final_cols = [col for col in final_columns_to_keep if col not in df_daily_weather.columns]
    if missing_final_cols:
        print(f"  Error: Columns {missing_final_cols} missing before final select for FIPS batch. Available: {df_daily_weather.columns}")
        return None, file_load_errors
    df_daily_weather = df_daily_weather.select(final_columns_to_keep)
    print(f"  Cleaned daily weather data for FIPS batch has shape: {df_daily_weather.shape}")
    return df_daily_weather, file_load_errors

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
    print("--- Starting Agricultural Weather Data Processing (Batched FIPS) ---")
    print(f"Target daily weather variables to fetch: {config.TARGET_DAILY_WEATHER_VARIABLES}")
    print(f"Weather data processing will start from year: {config.DATA_START_YEAR}")

    input_csv_path = "df_yield_weather_20_05_14h00m.csv" 
    output_csv_path = "df_yield_with_weather_aggs_batched.csv" # Different output for test
    fips_batch_size = 10 # Define batch size
    
    prepared_data = load_and_prepare_yield_data(input_csv_path)
    if prepared_data is None:
        print("Failed to load and prepare yield data. Exiting.")
        return

    df_yield, unique_fips_to_process, min_year_csv, max_year_csv = prepared_data
    
    overall_fetch_start_year = config.DATA_START_YEAR
    overall_fetch_end_year = max_year_csv 
    print(f"Overall weather data will be fetched for years: {overall_fetch_start_year} - {overall_fetch_end_year}.")

    all_yearly_weather_aggregates: List[Dict[str, Any]] = []
    all_s3_load_errors_summary: Dict[str, List[Dict[str,str]]] = {} 

    # --- Chunk 5: Main Processing Loop (Batched) ---
    num_fips = len(unique_fips_to_process)
    print(f"\n--- Starting Main Processing Loop for {num_fips} FIPS codes in batches of {fips_batch_size} ---")

    for i in range(0, num_fips, fips_batch_size):
        fips_batch = unique_fips_to_process[i : i + fips_batch_size]
        batch_start_time = time.time()
        print(f"\nProcessing FIPS Batch {i//fips_batch_size + 1}/{(num_fips + fips_batch_size - 1)//fips_batch_size}: {fips_batch}")

        # For simplicity in this test, we'll use the overall fetch years for the batch.
        # A more optimized approach might find min/max years *within* the batch.
        batch_fetch_start_year = overall_fetch_start_year
        batch_fetch_end_year = overall_fetch_end_year
        
        # Fetch data for the entire batch of FIPS codes
        cleaned_df_for_batch, errors_for_this_batch_load = fetch_and_clean_daily_weather_batched(
            fips_batch, 
            batch_fetch_start_year, 
            batch_fetch_end_year
        )

        if errors_for_this_batch_load: # Log errors associated with this batch load
            # Errors from load_nclimgrid_data are per-file, so they are inherently FIPS-specific if a FIPS is in only one file.
            # However, a batch-level error (e.g. connection error for the whole batch) might be harder to attribute.
            # For now, associate all errors from this batch call with all FIPS in the batch.
            print(f"  S3 Loading Errors for FIPS batch {fips_batch} (affecting {len(errors_for_this_batch_load)} files/attempts):")
            for fips_in_batch_for_error_log in fips_batch: # Log against each FIPS in batch
                 if fips_in_batch_for_error_log not in all_s3_load_errors_summary:
                    all_s3_load_errors_summary[fips_in_batch_for_error_log] = []
                 all_s3_load_errors_summary[fips_in_batch_for_error_log].extend(errors_for_this_batch_load) # Appends all errors from the batch call
            for error_info in errors_for_this_batch_load[:3]: 
                print(f"    - File for {error_info['yyyymm']}: {error_info['error_type']} - {error_info['error_message'][:100]}...")
            if len(errors_for_this_batch_load) > 3:
                print(f"    ... and {len(errors_for_this_batch_load) - 3} more S3 loading errors for this batch.")

        if cleaned_df_for_batch is None or cleaned_df_for_batch.height == 0:
            print(f"  No cleaned daily weather data available for FIPS batch {fips_batch}. Aggregates will be NaN for these FIPS' yield years >= {overall_fetch_start_year}.")
            for fips_in_batch in fips_batch:
                df_fips_yield_years = df_yield.filter(pl.col("fips_full") == fips_in_batch)
                min_year_for_this_fips = df_fips_yield_years.select(pl.col("year").min()).item()
                max_year_for_this_fips = df_fips_yield_years.select(pl.col("year").max()).item()
                for year_to_fill in range(max(overall_fetch_start_year, min_year_for_this_fips), max_year_for_this_fips + 1):
                    nan_aggs = {"fips_full": fips_in_batch, "year": year_to_fill}
                    for prefix in ["GS", "SGF"]:
                        nan_aggs.update({
                            f"GDD_{prefix}": np.nan, f"KDD_{prefix}": np.nan, f"TMAX_AVG_{prefix}": np.nan,
                            f"PREC_{prefix}": np.nan, f"CHD_{prefix}": np.nan 
                        })
                    all_yearly_weather_aggregates.append(nan_aggs)
            batch_end_time = time.time()
            print(f"  Finished processing FIPS batch (no data) in {batch_end_time - batch_start_time:.2f} seconds.")
            continue # Move to the next batch

        # Add a 'year_daily' column for grouping daily data
        cleaned_df_for_batch = cleaned_df_for_batch.with_columns(
            pl.col("date").dt.year().alias("year_daily") 
        )

        # Now, iterate through each FIPS within the successfully fetched batch
        for fips_code in fips_batch:
            df_single_fips_daily_data = cleaned_df_for_batch.filter(pl.col("fips") == fips_code) # Filter by 'fips' from weather data
            
            if df_single_fips_daily_data.height == 0:
                # print(f"    No daily data for FIPS {fips_code} within the batch after filtering. Aggregates will be NaN.")
                # This case means this FIPS had no data even if other FIPS in batch did.
                # Add NaN rows for all years this FIPS appears in the yield CSV
                df_fips_yield_years = df_yield.filter(pl.col("fips_full") == fips_code)
                min_year_for_this_fips = df_fips_yield_years.select(pl.col("year").min()).item()
                max_year_for_this_fips = df_fips_yield_years.select(pl.col("year").max()).item()
                for year_to_fill in range(max(overall_fetch_start_year, min_year_for_this_fips), max_year_for_this_fips + 1):
                    nan_aggs = {"fips_full": fips_code, "year": year_to_fill}
                    for prefix in ["GS", "SGF"]:
                        nan_aggs.update({
                            f"GDD_{prefix}": np.nan, f"KDD_{prefix}": np.nan, f"TMAX_AVG_{prefix}": np.nan,
                            f"PREC_{prefix}": np.nan, f"CHD_{prefix}": np.nan
                        })
                    all_yearly_weather_aggregates.append(nan_aggs)
                continue # Next FIPS in batch

            df_fips_yield_years = df_yield.filter(pl.col("fips_full") == fips_code)
            min_year_for_this_fips = df_fips_yield_years.select(pl.col("year").min()).item() # For pre-data NaN filling
            max_year_for_this_fips = df_fips_yield_years.select(pl.col("year").max()).item()

            relevant_yield_years_for_fips = sorted(
                df_fips_yield_years.filter(pl.col("year") >= overall_fetch_start_year) 
                                    .select("year").unique().to_series().to_list()
            )

            for year_to_aggregate in relevant_yield_years_for_fips:
                daily_data_for_this_year_fips = df_single_fips_daily_data.filter(pl.col("year_daily") == year_to_aggregate)
                
                if daily_data_for_this_year_fips.height == 0:
                    yearly_aggs = {"fips_full": fips_code, "year": year_to_aggregate}
                    for prefix in ["GS", "SGF"]:
                        yearly_aggs.update({
                            f"GDD_{prefix}": np.nan, f"KDD_{prefix}": np.nan, f"TMAX_AVG_{prefix}": np.nan,
                            f"PREC_{prefix}": np.nan, f"CHD_{prefix}": np.nan
                        })
                else:
                    yearly_aggs = aggregate_weather_to_yearly(daily_data_for_this_year_fips, year_to_aggregate)
                    yearly_aggs["fips_full"] = fips_code 
                all_yearly_weather_aggregates.append(yearly_aggs)
            
            pre_data_yield_years = sorted(
                df_fips_yield_years.filter(pl.col("year") < overall_fetch_start_year)
                                    .select("year").unique().to_series().to_list()
            )
            for year_to_fill_nan in pre_data_yield_years:
                nan_aggs = {"fips_full": fips_code, "year": year_to_fill_nan}
                for prefix in ["GS", "SGF"]:
                     nan_aggs.update({
                        f"GDD_{prefix}": np.nan, f"KDD_{prefix}": np.nan, f"TMAX_AVG_{prefix}": np.nan,
                        f"PREC_{prefix}": np.nan, f"CHD_{prefix}": np.nan
                    })
                all_yearly_weather_aggregates.append(nan_aggs)
        
        batch_end_time = time.time()
        print(f"  Finished processing FIPS batch in {batch_end_time - batch_start_time:.2f} seconds.")

    # --- Chunk 6: Merge results and save ---
    if not all_yearly_weather_aggregates:
        print("\nNo yearly weather aggregates were generated. Cannot merge or save.")
    else:
        df_final_weather_aggs = pl.DataFrame(all_yearly_weather_aggregates)
        df_final_weather_aggs = df_final_weather_aggs.with_columns([
            pl.col("fips_full").cast(pl.String), pl.col("year").cast(pl.Int64)
        ])
        print(f"\n--- Processing Complete: Total yearly aggregate records: {df_final_weather_aggs.height} ---")
        print("\n--- Merging weather aggregates with yield data ---")
        if df_yield["fips_full"].dtype != pl.String:
             df_yield = df_yield.with_columns(pl.col("fips_full").cast(pl.String))
        if df_yield["year"].dtype != pl.Int64:
             df_yield = df_yield.with_columns(pl.col("year").cast(pl.Int64))
        df_merged = df_yield.join(df_final_weather_aggs, on=["fips_full", "year"], how="left")
        print(f"\nShape of merged DataFrame: {df_merged.shape}")
        print(f"\nSaving merged data to: {output_csv_path}")
        try:
            df_merged.write_csv(output_csv_path)
            print(f"Successfully saved data to {output_csv_path}")
        except Exception as e:
            print(f"Error saving merged data to CSV: {e}")

    if all_s3_load_errors_summary:
        print("\n--- Summary of S3 File Loading Errors Encountered ---")
        for fips_key, errors_list in all_s3_load_errors_summary.items():
            print(f"  FIPS {fips_key}: {len(errors_list)} file(s)/attempts had errors.")
            # To avoid printing too many errors if a batch call failed for many FIPS:
            unique_file_errors = {} # Store unique file path errors to avoid repetition for batch errors
            for err_detail in errors_list:
                if err_detail['file_path'] not in unique_file_errors:
                    print(f"    - File/Attempt {err_detail['yyyymm']} ({err_detail['file_path'][-40:] if err_detail['file_path'] != 'N/A - Batch Attempt' else 'Batch Attempt'}): {err_detail['error_type']}")
                    unique_file_errors[err_detail['file_path']] = True
                if len(unique_file_errors) >=3: # Limit details per FIPS
                    if len(errors_list) > len(unique_file_errors):
                         print(f"    ... and more errors for this FIPS batch.")
                    break 
    else:
        print("\nNo S3 file loading errors were reported across all FIPS processing.")

    overall_end_time = time.time()
    print(f"\n--- Agricultural Weather Data Processing Finished in {overall_end_time - overall_start_time:.2f} seconds ---")

if __name__ == "__main__":
    main()
