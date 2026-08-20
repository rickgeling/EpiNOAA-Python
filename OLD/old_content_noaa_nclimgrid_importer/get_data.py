

def create_gdd():
    
    return 0


def create_kdd():
    
    return 0


def aggregated_seasonal():
    
    return 0


def compound_hot_dry():
    
    return 0


def create_csv_files():
    
    return 0


def main_steps():


    return 0


# process_ag_weather.py

# --- Standard Library Imports ---
import os
import time # For timing FIPS processing
from typing import List, Dict, Optional, Any, Tuple 

# --- Third-party Library Imports ---
import polars as pl
from polars.exceptions import ColumnNotFoundError
import numpy as np # For NaN

# --- Custom Module Imports ---
from nclimgrid_importer import load_nclimgrid_data 
import config

def load_and_prepare_yield_data(csv_path: str) -> Optional[Tuple[pl.DataFrame, List[str], int, int]]:
    """
    Loads the yield data CSV, prepares FIPS codes, and extracts unique FIPS and year range.
    """
    if not os.path.exists(csv_path):
        print(f"Error: Input CSV file not found at '{csv_path}'")
        return None
    try:
        yield_column_dtypes = {
            "state": pl.String,
            "county": pl.String,
            "year": pl.Int64
        }
        # For newer Polars versions, dtypes is schema_overrides
        # df_yield = pl.read_csv(csv_path, dtypes=yield_column_dtypes, null_values=["NA", "N/A", "", " "])
        df_yield = pl.read_csv(csv_path, schema_overrides=yield_column_dtypes, null_values=["NA", "N/A", "", " "])
        
        print(f"\nSuccessfully loaded input CSV: '{csv_path}', Shape: {df_yield.shape}")
        required_cols = ["state", "county", "year"]
        for col_name_req in required_cols: # Renamed col to col_name_req
            if col_name_req not in df_yield.columns:
                print(f"Error: CSV must contain a '{col_name_req}' column.")
                return None
        df_yield = df_yield.drop_nulls(subset=["state", "county", "year"])
        if df_yield.height == 0:
            print("Error: No valid rows in CSV after dropping nulls in state, county, or year.")
            return None
        df_yield = df_yield.with_columns(
            pl.col("county").str.zfill(3).alias("county_padded")
        )
        df_yield = df_yield.with_columns(
            (pl.col("state") + pl.col("county_padded")).alias("fips_full")
        )
        # Corrected line for unique_fips_to_process:
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

def fetch_and_clean_daily_weather(
    fips_code: str, 
    start_year: int, 
    end_year: int
) -> Tuple[Optional[pl.DataFrame], List[Dict[str, str]]]:
    """
    Fetches daily weather data for a given FIPS code and year range,
    then performs initial cleaning (casting types, handling missing values).
    """
    # print(f"\nFetching daily weather for FIPS: {fips_code}, Years: {start_year}-{end_year}") # Made less verbose
    start_date_str = f"{start_year}-01-01"
    end_date_str = f"{end_year}-12-31"
    daily_arrow_table, file_load_errors = load_nclimgrid_data(
        start_date=start_date_str, end_date=end_date_str, spatial_scale='cty', 
        scaled=True, counties=[fips_code], variables=config.TARGET_DAILY_WEATHER_VARIABLES 
    )
    if daily_arrow_table is None or daily_arrow_table.num_rows == 0:
        return None, file_load_errors 
    df_daily_weather = pl.from_arrow(daily_arrow_table)
    if "date" in df_daily_weather.columns:
        if df_daily_weather["date"].dtype != pl.Datetime:
            try:
                df_daily_weather = df_daily_weather.with_columns(pl.col("date").cast(pl.Datetime))
            except Exception as e:
                print(f"  Error casting 'date' column for FIPS {fips_code}: {e}. Returning raw table & errors.")
                return df_daily_weather, file_load_errors 
    else:
        print(f"  Error: 'date' column missing for FIPS {fips_code}. Returning None & errors.")
        return None, file_load_errors
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
            print(f"  Error applying casting/cleaning for FIPS {fips_code}: {e}. Returning raw table & errors.")
            return df_daily_weather, file_load_errors 
    final_columns_to_keep = ["date"]
    for var_name in config.TARGET_DAILY_WEATHER_VARIABLES:
        if var_name in df_daily_weather.columns and df_daily_weather[var_name].dtype == pl.Float64:
            final_columns_to_keep.append(var_name)
        elif var_name in df_daily_weather.columns: 
             if var_name not in final_columns_to_keep : final_columns_to_keep.append(var_name)
    final_columns_to_keep = list(dict.fromkeys(final_columns_to_keep))
    missing_final_cols = [col for col in final_columns_to_keep if col not in df_daily_weather.columns]
    if missing_final_cols:
        print(f"  Error: Columns {missing_final_cols} missing before final select for FIPS {fips_code}. Available: {df_daily_weather.columns}")
        return None, file_load_errors
    df_daily_weather = df_daily_weather.select(final_columns_to_keep)
    return df_daily_weather, file_load_errors

# --- Chunk 3: Agronomic Variable Calculation Functions ---
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

# --- Chunk 4: Yearly Aggregation Function (Refactored) ---
def _calculate_seasonal_aggregates(
    df_season: pl.DataFrame, 
    season_prefix: str
) -> Dict[str, Any]:
    """
    Internal helper to calculate agronomic variables for a given seasonal DataFrame.
    If any required daily value is null, the corresponding aggregate will be NaN.
    """
    aggs: Dict[str, Any] = {}
    df_season_with_daily_calcs = df_season.clone() 

    # GDD
    if "tavg" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("tavg").is_null().all():
        if df_season_with_daily_calcs.get_column("tavg").is_null().any(): 
            aggs[f"GDD_{season_prefix}"] = np.nan
        else:
            df_season_with_daily_calcs = df_season_with_daily_calcs.with_columns(
                calculate_daily_gdd(df_season_with_daily_calcs.get_column("tavg")).alias("_daily_gdd")
            )
            aggs[f"GDD_{season_prefix}"] = df_season_with_daily_calcs.get_column("_daily_gdd").sum()
    else: 
        aggs[f"GDD_{season_prefix}"] = np.nan

    # KDD
    if "tmax" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("tmax").is_null().all():
        if df_season_with_daily_calcs.get_column("tmax").is_null().any():
            aggs[f"KDD_{season_prefix}"] = np.nan
        else:
            df_season_with_daily_calcs = df_season_with_daily_calcs.with_columns(
                calculate_daily_kdd(df_season_with_daily_calcs.get_column("tmax")).alias("_daily_kdd")
            )
            aggs[f"KDD_{season_prefix}"] = df_season_with_daily_calcs.get_column("_daily_kdd").sum()
    else:
        aggs[f"KDD_{season_prefix}"] = np.nan
    
    # TMAX_AVG
    if "tmax" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("tmax").is_null().all():
        if df_season_with_daily_calcs.get_column("tmax").is_null().any():
            aggs[f"TMAX_AVG_{season_prefix}"] = np.nan
        else:
            aggs[f"TMAX_AVG_{season_prefix}"] = df_season_with_daily_calcs.get_column("tmax").mean()
    else:
        aggs[f"TMAX_AVG_{season_prefix}"] = np.nan

    # PREC (PREC2 removed)
    if "prcp" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("prcp").is_null().all():
        if df_season_with_daily_calcs.get_column("prcp").is_null().any():
            aggs[f"PREC_{season_prefix}"] = np.nan
        else:
            aggs[f"PREC_{season_prefix}"] = df_season_with_daily_calcs.get_column("prcp").sum()
    else:
        aggs[f"PREC_{season_prefix}"] = np.nan
        
    # CHD
    if ("tmax" in df_season_with_daily_calcs.columns and "prcp" in df_season_with_daily_calcs.columns and
        not df_season_with_daily_calcs.get_column("tmax").is_null().all() and 
        not df_season_with_daily_calcs.get_column("prcp").is_null().all()):
        if df_season_with_daily_calcs.get_column("tmax").is_null().any() or df_season_with_daily_calcs.get_column("prcp").is_null().any():
            aggs[f"CHD_{season_prefix}"] = np.nan
        else:
            df_season_with_daily_calcs = df_season_with_daily_calcs.with_columns(
                calculate_daily_chd(df_season_with_daily_calcs.get_column("tmax"), df_season_with_daily_calcs.get_column("prcp")).alias("_daily_chd")
            )
            aggs[f"CHD_{season_prefix}"] = df_season_with_daily_calcs.get_column("_daily_chd").sum()
    else:
        aggs[f"CHD_{season_prefix}"] = np.nan
        
    return aggs

def aggregate_weather_to_yearly(daily_df_for_year: pl.DataFrame, year: int) -> Dict[str, Any]:
    """
    Aggregates daily weather data to yearly agronomic variables for GS and SGF seasons.
    """
    all_aggs: Dict[str, Any] = {"year": year} 

    # Growing Season (GS)
    gs_start_date = pl.datetime(year, config.GS_START_MONTH, config.GS_START_DAY)
    gs_end_date = pl.datetime(year, config.GS_END_MONTH, config.GS_END_DAY)
    df_gs = daily_df_for_year.filter((pl.col("date") >= gs_start_date) & (pl.col("date") <= gs_end_date))
    
    gs_essential_cols = [col for col in config.TARGET_DAILY_WEATHER_VARIABLES if col != 'date']
    gs_essential_cols_present = all(col in df_gs.columns for col in gs_essential_cols)
    
    if df_gs.height == 0 or not gs_essential_cols_present:
        gs_aggs = {
            f"GDD_GS": np.nan, f"KDD_GS": np.nan, f"TMAX_AVG_GS": np.nan,
            f"PREC_GS": np.nan, f"CHD_GS": np.nan # PREC2_GS removed
        }
    else:
        gs_aggs = _calculate_seasonal_aggregates(df_gs, "GS")
    all_aggs.update(gs_aggs)

    # Silking-Grain-Fill (SGF)
    sgf_start_date = pl.datetime(year, config.SGF_START_MONTH, config.SGF_START_DAY)
    sgf_end_date = pl.datetime(year, config.SGF_END_MONTH, config.SGF_END_DAY)
    df_sgf = daily_df_for_year.filter((pl.col("date") >= sgf_start_date) & (pl.col("date") <= sgf_end_date))

    sgf_essential_cols_present = all(col in df_sgf.columns for col in gs_essential_cols) # Same essential cols
    if df_sgf.height == 0 or not sgf_essential_cols_present:
        sgf_aggs = {
            f"GDD_SGF": np.nan, f"KDD_SGF": np.nan, f"TMAX_AVG_SGF": np.nan,
            f"PREC_SGF": np.nan, f"CHD_SGF": np.nan # PREC2_SGF removed
        }
    else:
        sgf_aggs = _calculate_seasonal_aggregates(df_sgf, "SGF")
    all_aggs.update(sgf_aggs)
        
    return all_aggs

def main():
    overall_start_time = time.time()
    print("--- Starting Agricultural Weather Data Processing ---")
    print(f"Target daily weather variables to fetch: {config.TARGET_DAILY_WEATHER_VARIABLES}")
    print(f"Weather data processing will start from year: {config.DATA_START_YEAR}")

    input_csv_path = "df_yield_weather_20_05_14h00m.csv" # Make sure this filename is correct
    output_csv_path = "df_yield_with_weather_aggs.csv" 
    
    prepared_data = load_and_prepare_yield_data(input_csv_path)
    if prepared_data is None:
        print("Failed to load and prepare yield data. Exiting.")
        return

    df_yield, unique_fips_to_process, min_year_csv, max_year_csv = prepared_data
    
    overall_fetch_start_year = config.DATA_START_YEAR
    overall_fetch_end_year = max_year_csv 
    print(f"Overall weather data will be fetched for years: {overall_fetch_start_year} - {overall_fetch_end_year} where available for each FIPS.")

    all_yearly_weather_aggregates: List[Dict[str, Any]] = []
    all_s3_load_errors_summary: Dict[str, List[Dict[str,str]]] = {} 

    # --- Chunk 5: Main Processing Loop ---
    print(f"\n--- Starting Main Processing Loop for {len(unique_fips_to_process)} FIPS codes ---")
    for i, fips_code in enumerate(unique_fips_to_process):
        fips_start_time = time.time()
        print(f"\nProcessing FIPS {i+1}/{len(unique_fips_to_process)}: {fips_code}")

        df_fips_yield_years = df_yield.filter(pl.col("fips_full") == fips_code)
        if df_fips_yield_years.height == 0:
            print(f"  No yield data found for FIPS {fips_code} in the input CSV. Skipping.")
            continue
        
        min_year_for_this_fips = df_fips_yield_years.select(pl.col("year").min()).item()
        max_year_for_this_fips = df_fips_yield_years.select(pl.col("year").max()).item()

        current_fips_fetch_start_year = overall_fetch_start_year 
        current_fips_fetch_end_year = max(overall_fetch_start_year -1, min(overall_fetch_end_year, max_year_for_this_fips)) 

        if current_fips_fetch_end_year < current_fips_fetch_start_year:
            print(f"  Max year for FIPS {fips_code} ({max_year_for_this_fips}) is before effective data start year ({current_fips_fetch_start_year}). Skipping weather fetch.")
            for year_to_fill in range(min_year_for_this_fips, max_year_for_this_fips + 1):
                nan_aggs = {"fips_full": fips_code, "year": year_to_fill}
                for prefix in ["GS", "SGF"]:
                    nan_aggs.update({
                        f"GDD_{prefix}": np.nan, f"KDD_{prefix}": np.nan, f"TMAX_AVG_{prefix}": np.nan,
                        f"PREC_{prefix}": np.nan, f"CHD_{prefix}": np.nan 
                    })
                all_yearly_weather_aggregates.append(nan_aggs)
            continue
            
        cleaned_df_for_fips, errors_for_this_fips_load = fetch_and_clean_daily_weather(
            fips_code, 
            current_fips_fetch_start_year, 
            current_fips_fetch_end_year
        )

        if errors_for_this_fips_load:
            print(f"  S3 Loading Errors for FIPS {fips_code} (affecting {len(errors_for_this_fips_load)} months):")
            for error_info in errors_for_this_fips_load[:3]: 
                print(f"    - File for {error_info['yyyymm']}: {error_info['error_type']} - {error_info['error_message'][:100]}...")
            if len(errors_for_this_fips_load) > 3:
                print(f"    ... and {len(errors_for_this_fips_load) - 3} more S3 loading errors for this FIPS.")
            all_s3_load_errors_summary[fips_code] = errors_for_this_fips_load
        
        if cleaned_df_for_fips is None or cleaned_df_for_fips.height == 0:
            print(f"  No cleaned daily weather data available for FIPS {fips_code}. Aggregates will be NaN for its yield years >= {overall_fetch_start_year}.")
            for year_to_fill in range(max(overall_fetch_start_year, min_year_for_this_fips), max_year_for_this_fips + 1):
                nan_aggs = {"fips_full": fips_code, "year": year_to_fill}
                for prefix in ["GS", "SGF"]:
                    nan_aggs.update({
                        f"GDD_{prefix}": np.nan, f"KDD_{prefix}": np.nan, f"TMAX_AVG_{prefix}": np.nan,
                        f"PREC_{prefix}": np.nan, f"CHD_{prefix}": np.nan 
                    })
                all_yearly_weather_aggregates.append(nan_aggs)
        else:
            cleaned_df_for_fips = cleaned_df_for_fips.with_columns(
                pl.col("date").dt.year().alias("year_daily") 
            )
            
            relevant_yield_years_for_fips = sorted(
                df_fips_yield_years.filter(pl.col("year") >= current_fips_fetch_start_year) 
                                    .select("year").unique().to_series().to_list()
            )

            for year_to_aggregate in relevant_yield_years_for_fips:
                daily_data_for_this_year = cleaned_df_for_fips.filter(pl.col("year_daily") == year_to_aggregate)
                
                if daily_data_for_this_year.height == 0:
                    print(f"    Warning: No daily data for FIPS {fips_code}, Year {year_to_aggregate} after initial fetch. Setting aggregates to NaN.")
                    yearly_aggs = {"fips_full": fips_code, "year": year_to_aggregate}
                    for prefix in ["GS", "SGF"]:
                        yearly_aggs.update({
                            f"GDD_{prefix}": np.nan, f"KDD_{prefix}": np.nan, f"TMAX_AVG_{prefix}": np.nan,
                            f"PREC_{prefix}": np.nan, f"CHD_{prefix}": np.nan 
                        })
                else:
                    yearly_aggs = aggregate_weather_to_yearly(daily_data_for_this_year, year_to_aggregate)
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
        
        fips_end_time = time.time()
        print(f"  Finished processing FIPS {fips_code} in {fips_end_time - fips_start_time:.2f} seconds.")

    # --- Chunk 6: Merge results and save ---
    if not all_yearly_weather_aggregates:
        print("\nNo yearly weather aggregates were generated. Cannot merge or save.")
    else:
        df_final_weather_aggs = pl.DataFrame(all_yearly_weather_aggregates)
        # Ensure correct dtypes before merge, especially for join keys
        df_final_weather_aggs = df_final_weather_aggs.with_columns([
            pl.col("fips_full").cast(pl.String),
            pl.col("year").cast(pl.Int64)
        ])

        print(f"\n--- Chunk 5 Complete: Processed all FIPS. Total yearly aggregate records: {df_final_weather_aggs.height} ---")
        # print("Sample of aggregated weather data (first 5 rows):")
        # print(df_final_weather_aggs.head(5))
        # print("Schema of aggregated weather data:")
        # for col_name, dtype in df_final_weather_aggs.schema.items():
        #     print(f"  - {col_name}: {dtype}")


        print("\n--- Chunk 6: Merging weather aggregates with yield data ---")
        
        # Ensure join keys in df_yield also have correct types
        if df_yield["fips_full"].dtype != pl.String:
             df_yield = df_yield.with_columns(pl.col("fips_full").cast(pl.String))
        if df_yield["year"].dtype != pl.Int64:
             df_yield = df_yield.with_columns(pl.col("year").cast(pl.Int64))


        df_merged = df_yield.join(
            df_final_weather_aggs,
            on=["fips_full", "year"],
            how="left"
        )
        print(f"\nShape of merged DataFrame: {df_merged.shape}")
        print("Sample of merged DataFrame (first 5 rows with new columns):")
        # Select some original columns and the new weather columns to show the merge
        cols_to_show = ["year", "fips_full"] + [col for col in df_final_weather_aggs.columns if col not in ["fips_full", "year"]]
        # Ensure all cols_to_show actually exist in df_merged
        cols_to_show = [col for col in cols_to_show if col in df_merged.columns]
        print(df_merged.head(5).select(cols_to_show))
        
        print("\nChecking presence and null counts of new weather columns in merged data:")
        new_weather_cols = [col for col in df_final_weather_aggs.columns if col not in ["fips_full", "year"]]
        for col in new_weather_cols:
            if col in df_merged.columns:
                print(f"  Column '{col}' present. Null count: {df_merged[col].is_null().sum()} out of {df_merged.height}")
            else:
                print(f"  WARNING: Column '{col}' is MISSING after merge!")


        print(f"\nSaving merged data to: {output_csv_path}")
        try:
            df_merged.write_csv(output_csv_path)
            print(f"Successfully saved data to {output_csv_path}")
        except Exception as e:
            print(f"Error saving merged data to CSV: {e}")

    if all_s3_load_errors_summary:
        print("\n--- Summary of S3 File Loading Errors Encountered ---")
        for fips, errors in all_s3_load_errors_summary.items():
            print(f"  FIPS {fips}: {len(errors)} file(s) had errors.")
            # for err_detail in errors[:2]: 
            #      print(f"    - {err_detail['yyyymm']} ({err_detail['file_path'][-40:]}): {err_detail['error_type']}")
            # if len(errors) > 2:
            #     print(f"    ... and {len(errors)-2} more errors for this FIPS.")
    else:
        print("\nNo S3 file loading errors were reported across all FIPS processing.")


    overall_end_time = time.time()
    print(f"\n--- Agricultural Weather Data Processing Finished in {overall_end_time - overall_start_time:.2f} seconds ---")

if __name__ == "__main__":
    main()
    
    
    
# ##### VERSION  FULLY CORRECT CHUNK 5

# # process_ag_weather.py

# # --- Standard Library Imports ---
# import os
# from typing import List, Dict, Optional, Any, Tuple 

# # --- Third-party Library Imports ---
# import polars as pl
# from polars.exceptions import ColumnNotFoundError
# import numpy as np # For NaN

# # --- Custom Module Imports ---
# from nclimgrid_importer import load_nclimgrid_data 
# import config

# def load_and_prepare_yield_data(csv_path: str) -> Optional[Tuple[pl.DataFrame, List[str], int, int]]:
#     """
#     Loads the yield data CSV, prepares FIPS codes, and extracts unique FIPS and year range.
#     (Function content remains the same - see previous version)
#     """
#     if not os.path.exists(csv_path):
#         print(f"Error: Input CSV file not found at '{csv_path}'")
#         return None
#     try:
#         yield_column_dtypes = {
#             "state": pl.String,
#             "county": pl.String,
#             "year": pl.Int64
#         }
#         df_yield = pl.read_csv(csv_path, schema_overrides=yield_column_dtypes, null_values=["NA", "N/A", "", " "])
#         print(f"\nSuccessfully loaded input CSV: '{csv_path}', Shape: {df_yield.shape}")
#         required_cols = ["state", "county", "year"]
#         for col_name_req in required_cols:
#             if col_name_req not in df_yield.columns:
#                 print(f"Error: CSV must contain a '{col_name_req}' column.")
#                 return None
#         df_yield = df_yield.drop_nulls(subset=["state", "county", "year"])
#         if df_yield.height == 0:
#             print("Error: No valid rows in CSV after dropping nulls in state, county, or year.")
#             return None
#         df_yield = df_yield.with_columns(
#             pl.col("county").str.zfill(3).alias("county_padded")
#         )
#         df_yield = df_yield.with_columns(
#             (pl.col("state") + pl.col("county_padded")).alias("fips_full")
#         )
#         unique_fips_to_process = df_yield.select("fips_full").unique().to_series().to_list()
#         print(f"\nFound {len(unique_fips_to_process)} unique FIPS codes to process from yield data.")
#         if not unique_fips_to_process:
#             print("Error: No unique FIPS codes found.")
#             return None
#         min_year_csv = df_yield.select(pl.col("year").min()).item()
#         max_year_csv = df_yield.select(pl.col("year").max()).item()
#         print(f"Year range in CSV: {min_year_csv} - {max_year_csv}")
#         return df_yield, unique_fips_to_process, min_year_csv, max_year_csv
#     except Exception as e:
#         print(f"Error loading or processing input CSV: {type(e).__name__} - {e}")
#         return None

# def fetch_and_clean_daily_weather(
#     fips_code: str, 
#     start_year: int, 
#     end_year: int
# ) -> Tuple[Optional[pl.DataFrame], List[Dict[str, str]]]:
#     """
#     Fetches daily weather data for a given FIPS code and year range,
#     then performs initial cleaning (casting types, handling missing values).
#     (Function content remains largely the same - see previous version)
#     """
#     print(f"\nFetching daily weather for FIPS: {fips_code}, Years: {start_year}-{end_year}")
#     start_date_str = f"{start_year}-01-01"
#     end_date_str = f"{end_year}-12-31"
#     daily_arrow_table, file_load_errors = load_nclimgrid_data(
#         start_date=start_date_str, end_date=end_date_str, spatial_scale='cty', 
#         scaled=True, counties=[fips_code], variables=config.TARGET_DAILY_WEATHER_VARIABLES 
#     )
#     if daily_arrow_table is None or daily_arrow_table.num_rows == 0:
#         # print(f"  No daily weather data returned or data was empty for FIPS {fips_code} for years {start_year}-{end_year}.")
#         return None, file_load_errors 
#     df_daily_weather = pl.from_arrow(daily_arrow_table)
#     if "date" in df_daily_weather.columns:
#         if df_daily_weather["date"].dtype != pl.Datetime:
#             try:
#                 df_daily_weather = df_daily_weather.with_columns(pl.col("date").cast(pl.Datetime))
#             except Exception as e:
#                 print(f"  Error casting 'date' column for FIPS {fips_code}: {e}. Returning raw table & errors.")
#                 return df_daily_weather, file_load_errors 
#     else:
#         print(f"  Error: 'date' column missing for FIPS {fips_code}. Returning None & errors.")
#         return None, file_load_errors
#     cast_and_clean_expressions = []
#     for col_name in config.TARGET_DAILY_WEATHER_VARIABLES:
#         if col_name in df_daily_weather.columns:
#             if df_daily_weather[col_name].dtype == pl.String:
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
#                     .then(None).otherwise(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False)).alias(col_name)
#                 )
#             elif df_daily_weather[col_name].dtype == pl.Float64:
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
#                     .then(None).otherwise(pl.col(col_name)).alias(col_name)
#                 )
#             elif df_daily_weather[col_name].dtype.is_numeric():
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name).cast(pl.Float64, strict=False) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
#                     .then(None).otherwise(pl.col(col_name).cast(pl.Float64, strict=False)).alias(col_name)
#                 )
#     if cast_and_clean_expressions:
#         try:
#             df_daily_weather = df_daily_weather.with_columns(cast_and_clean_expressions)
#         except Exception as e:
#             print(f"  Error applying casting/cleaning for FIPS {fips_code}: {e}. Returning raw table & errors.")
#             return df_daily_weather, file_load_errors 
#     final_columns_to_keep = ["date"]
#     for var_name in config.TARGET_DAILY_WEATHER_VARIABLES:
#         if var_name in df_daily_weather.columns and df_daily_weather[var_name].dtype == pl.Float64:
#             final_columns_to_keep.append(var_name)
#         elif var_name in df_daily_weather.columns: 
#              if var_name not in final_columns_to_keep : final_columns_to_keep.append(var_name)
#     final_columns_to_keep = list(dict.fromkeys(final_columns_to_keep))
#     missing_final_cols = [col for col in final_columns_to_keep if col not in df_daily_weather.columns]
#     if missing_final_cols:
#         print(f"  Error: Columns {missing_final_cols} missing before final select for FIPS {fips_code}. Available: {df_daily_weather.columns}")
#         return None, file_load_errors
#     df_daily_weather = df_daily_weather.select(final_columns_to_keep)
#     return df_daily_weather, file_load_errors

# # --- Chunk 3: Agronomic Variable Calculation Functions ---
# def calculate_daily_gdd(tavg_series: pl.Series) -> pl.Series:
#     tavg_float = tavg_series.cast(pl.Float64, strict=False) 
#     capped_tavg = pl.min_horizontal(tavg_float, pl.lit(config.GDD_MAX_TEMP_C, dtype=pl.Float64))
#     gdd_potential = capped_tavg - config.GDD_BASE_TEMP_C
#     daily_gdd = pl.max_horizontal(pl.lit(0.0, dtype=pl.Float64), gdd_potential)
#     return daily_gdd 

# def calculate_daily_kdd(tmax_series: pl.Series) -> pl.Series:
#     tmax_float = tmax_series.cast(pl.Float64, strict=False)
#     kdd_potential = tmax_float - config.KDD_TEMP_THRESHOLD_C
#     daily_kdd = pl.max_horizontal(pl.lit(0.0, dtype=pl.Float64), kdd_potential)
#     return daily_kdd

# def calculate_daily_chd(tmax_series: pl.Series, prcp_series: pl.Series) -> pl.Series:
#     tmax_float = tmax_series.cast(pl.Float64, strict=False)
#     prcp_float = prcp_series.cast(pl.Float64, strict=False)
#     is_hot = (tmax_float > config.CHD_TEMP_THRESHOLD_C).fill_null(False)
#     is_dry = (prcp_float < config.CHD_PRECIP_THRESHOLD_MM).fill_null(False)
#     daily_chd = (is_hot & is_dry).cast(pl.Int8) 
#     return daily_chd

# # --- Chunk 4: Yearly Aggregation Function (Refactored) ---
# def _calculate_seasonal_aggregates(
#     df_season: pl.DataFrame, 
#     season_prefix: str
# ) -> Dict[str, Any]:
#     """
#     Internal helper to calculate agronomic variables for a given seasonal DataFrame.
#     If any required daily value is null, the corresponding aggregate will be NaN.
#     """
#     aggs: Dict[str, Any] = {}
#     df_season_with_daily_calcs = df_season.clone() 

#     required_cols_for_season = { # Define which daily vars are needed for which aggregates
#         f"GDD_{season_prefix}": ["tavg"],
#         f"KDD_{season_prefix}": ["tmax"],
#         f"TMAX_AVG_{season_prefix}": ["tmax"],
#         f"PREC_{season_prefix}": ["prcp"],
#         f"PREC2_{season_prefix}": ["prcp"], # Depends on PREC_
#         f"CHD_{season_prefix}": ["tmax", "prcp"]
#     }

#     # GDD
#     if "tavg" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("tavg").is_null().all():
#         if df_season_with_daily_calcs.get_column("tavg").is_null().any(): 
#             aggs[f"GDD_{season_prefix}"] = np.nan
#         else:
#             df_season_with_daily_calcs = df_season_with_daily_calcs.with_columns(
#                 calculate_daily_gdd(df_season_with_daily_calcs.get_column("tavg")).alias("_daily_gdd")
#             )
#             aggs[f"GDD_{season_prefix}"] = df_season_with_daily_calcs.get_column("_daily_gdd").sum()
#     else: 
#         aggs[f"GDD_{season_prefix}"] = np.nan

#     # KDD
#     if "tmax" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("tmax").is_null().all():
#         if df_season_with_daily_calcs.get_column("tmax").is_null().any():
#             aggs[f"KDD_{season_prefix}"] = np.nan
#         else:
#             df_season_with_daily_calcs = df_season_with_daily_calcs.with_columns(
#                 calculate_daily_kdd(df_season_with_daily_calcs.get_column("tmax")).alias("_daily_kdd")
#             )
#             aggs[f"KDD_{season_prefix}"] = df_season_with_daily_calcs.get_column("_daily_kdd").sum()
#     else:
#         aggs[f"KDD_{season_prefix}"] = np.nan
    
#     # TMAX_AVG
#     if "tmax" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("tmax").is_null().all():
#         if df_season_with_daily_calcs.get_column("tmax").is_null().any():
#             aggs[f"TMAX_AVG_{season_prefix}"] = np.nan
#         else:
#             aggs[f"TMAX_AVG_{season_prefix}"] = df_season_with_daily_calcs.get_column("tmax").mean()
#     else:
#         aggs[f"TMAX_AVG_{season_prefix}"] = np.nan

#     # PREC and PREC2
#     if "prcp" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("prcp").is_null().all():
#         if df_season_with_daily_calcs.get_column("prcp").is_null().any():
#             aggs[f"PREC_{season_prefix}"] = np.nan
#             aggs[f"PREC2_{season_prefix}"] = np.nan
#         else:
#             prec_sum = df_season_with_daily_calcs.get_column("prcp").sum()
#             aggs[f"PREC_{season_prefix}"] = prec_sum
#             aggs[f"PREC2_{season_prefix}"] = prec_sum * prec_sum if prec_sum is not None else np.nan
#     else:
#         aggs[f"PREC_{season_prefix}"] = np.nan
#         aggs[f"PREC2_{season_prefix}"] = np.nan
        
#     # CHD
#     if ("tmax" in df_season_with_daily_calcs.columns and "prcp" in df_season_with_daily_calcs.columns and
#         not df_season_with_daily_calcs.get_column("tmax").is_null().all() and 
#         not df_season_with_daily_calcs.get_column("prcp").is_null().all()):
#         if df_season_with_daily_calcs.get_column("tmax").is_null().any() or df_season_with_daily_calcs.get_column("prcp").is_null().any():
#             aggs[f"CHD_{season_prefix}"] = np.nan
#         else:
#             df_season_with_daily_calcs = df_season_with_daily_calcs.with_columns(
#                 calculate_daily_chd(df_season_with_daily_calcs.get_column("tmax"), df_season_with_daily_calcs.get_column("prcp")).alias("_daily_chd")
#             )
#             aggs[f"CHD_{season_prefix}"] = df_season_with_daily_calcs.get_column("_daily_chd").sum()
#     else:
#         aggs[f"CHD_{season_prefix}"] = np.nan
        
#     return aggs

# def aggregate_weather_to_yearly(daily_df_for_year: pl.DataFrame, year: int) -> Dict[str, Any]:
#     """
#     Aggregates daily weather data to yearly agronomic variables for GS and SGF seasons.
#     """
#     all_aggs: Dict[str, Any] = {"year": year} 

#     # Growing Season (GS)
#     gs_start_date = pl.datetime(year, config.GS_START_MONTH, config.GS_START_DAY)
#     gs_end_date = pl.datetime(year, config.GS_END_MONTH, config.GS_END_DAY)
#     df_gs = daily_df_for_year.filter((pl.col("date") >= gs_start_date) & (pl.col("date") <= gs_end_date))
    
#     # Check if GS DataFrame is empty or if essential weather variables are missing
#     gs_essential_cols_present = all(col in df_gs.columns for col in config.TARGET_DAILY_WEATHER_VARIABLES if col != 'date')
#     if df_gs.height == 0 or not gs_essential_cols_present:
#         # print(f"    Year {year}: Insufficient data or columns for GS calculations. Setting GS aggregates to NaN.")
#         gs_aggs = {
#             f"GDD_GS": np.nan, f"KDD_GS": np.nan, f"TMAX_AVG_GS": np.nan,
#             f"PREC_GS": np.nan, f"PREC2_GS": np.nan, f"CHD_GS": np.nan
#         }
#     else:
#         gs_aggs = _calculate_seasonal_aggregates(df_gs, "GS")
#     all_aggs.update(gs_aggs)

#     # Silking-Grain-Fill (SGF)
#     sgf_start_date = pl.datetime(year, config.SGF_START_MONTH, config.SGF_START_DAY)
#     sgf_end_date = pl.datetime(year, config.SGF_END_MONTH, config.SGF_END_DAY)
#     df_sgf = daily_df_for_year.filter((pl.col("date") >= sgf_start_date) & (pl.col("date") <= sgf_end_date))

#     sgf_essential_cols_present = all(col in df_sgf.columns for col in config.TARGET_DAILY_WEATHER_VARIABLES if col != 'date')
#     if df_sgf.height == 0 or not sgf_essential_cols_present:
#         # print(f"    Year {year}: Insufficient data or columns for SGF calculations. Setting SGF aggregates to NaN.")
#         sgf_aggs = {
#             f"GDD_SGF": np.nan, f"KDD_SGF": np.nan, f"TMAX_AVG_SGF": np.nan,
#             f"PREC_SGF": np.nan, f"PREC2_SGF": np.nan, f"CHD_SGF": np.nan
#         }
#     else:
#         sgf_aggs = _calculate_seasonal_aggregates(df_sgf, "SGF")
#     all_aggs.update(sgf_aggs)
        
#     return all_aggs

# def main():
#     print("--- Starting Agricultural Weather Data Processing ---")
#     # ... (print config constants) ...
#     print(f"Target daily weather variables to fetch: {config.TARGET_DAILY_WEATHER_VARIABLES}")
#     print(f"Weather data processing will start from year: {config.DATA_START_YEAR}")

#     input_csv_path = "df_yield_weather_20_05_14h00m.csv" 
    
#     prepared_data = load_and_prepare_yield_data(input_csv_path)
#     if prepared_data is None:
#         print("Failed to load and prepare yield data. Exiting.")
#         return

#     df_yield, unique_fips_to_process, min_year_csv, max_year_csv = prepared_data
    
#     overall_fetch_start_year = config.DATA_START_YEAR
#     overall_fetch_end_year = max_year_csv # Max year from CSV to limit S3 calls
#     print(f"Overall weather data will be fetched for years: {overall_fetch_start_year} - {overall_fetch_end_year} where available for each FIPS.")

#     all_yearly_weather_aggregates: List[Dict[str, Any]] = []
#     all_s3_load_errors_summary: Dict[str, List[Dict[str,str]]] = {} # To store errors per FIPS

#     # --- Chunk 5: Main Processing Loop ---
#     print(f"\n--- Starting Main Processing Loop for {len(unique_fips_to_process)} FIPS codes ---")
#     for i, fips_code in enumerate(unique_fips_to_process):
#         print(f"\nProcessing FIPS {i+1}/{len(unique_fips_to_process)}: {fips_code}")

#         # Determine the specific year range for this FIPS from the yield data
#         df_fips_yield_years = df_yield.filter(pl.col("fips_full") == fips_code)
#         if df_fips_yield_years.height == 0:
#             print(f"  No yield data found for FIPS {fips_code} in the input CSV. Skipping weather processing.")
#             continue
        
#         min_year_for_this_fips = df_fips_yield_years.select(pl.col("year").min()).item()
#         max_year_for_this_fips = df_fips_yield_years.select(pl.col("year").max()).item()

#         # Actual years to fetch weather data for this FIPS
#         # We need weather data from DATA_START_YEAR up to the max year this FIPS appears in the yield CSV
#         # to ensure we can calculate aggregates for all relevant yield years.
#         current_fips_fetch_start_year = overall_fetch_start_year 
#         current_fips_fetch_end_year = max(overall_fetch_start_year -1, min(overall_fetch_end_year, max_year_for_this_fips)) # Ensure end >= start

#         if current_fips_fetch_end_year < current_fips_fetch_start_year:
#             print(f"  Max year for FIPS {fips_code} ({max_year_for_this_fips}) is before effective data start year ({current_fips_fetch_start_year}). Skipping weather fetch.")
#             continue
            
#         cleaned_df_for_fips, errors_for_this_fips_load = fetch_and_clean_daily_weather(
#             fips_code, 
#             current_fips_fetch_start_year, 
#             current_fips_fetch_end_year
#         )

#         if errors_for_this_fips_load:
#             print(f"  S3 Loading Errors for FIPS {fips_code}:")
#             for error_info in errors_for_this_fips_load:
#                 print(f"    - File for {error_info['yyyymm']}: {error_info['error_type']} - {error_info['error_message'][:100]}...") # Truncate long messages
#             all_s3_load_errors_summary[fips_code] = errors_for_this_fips_load
        
#         if cleaned_df_for_fips is None or cleaned_df_for_fips.height == 0:
#             print(f"  No cleaned daily weather data available for FIPS {fips_code} for period {current_fips_fetch_start_year}-{current_fips_fetch_end_year}. Aggregates will be NaN.")
#             # Still need to generate NaN rows for all years this FIPS appears in the yield CSV
#             # from DATA_START_YEAR up to max_year_for_this_fips
#             for year_to_fill in range(max(overall_fetch_start_year, min_year_for_this_fips), max_year_for_this_fips + 1):
#                 nan_aggs = {"fips_full": fips_code, "year": year_to_fill}
#                 for prefix in ["GS", "SGF"]:
#                     nan_aggs.update({
#                         f"GDD_{prefix}": np.nan, f"KDD_{prefix}": np.nan, f"TMAX_AVG_{prefix}": np.nan,
#                         f"PREC_{prefix}": np.nan, f"PREC2_{prefix}": np.nan, f"CHD_{prefix}": np.nan
#                     })
#                 all_yearly_weather_aggregates.append(nan_aggs)
#             continue # Move to the next FIPS

#         # Add a 'year' column to the daily data for grouping
#         cleaned_df_for_fips = cleaned_df_for_fips.with_columns(
#             pl.col("date").dt.year().alias("year")
#         )

#         # Group by year and calculate aggregates
#         # We need to iterate through years relevant for this FIPS in the yield data,
#         # but only process years for which we have weather data (i.e., >= DATA_START_YEAR)
        
#         # Relevant years for this FIPS from the yield data, ensuring they are within the fetched weather data range
#         relevant_yield_years_for_fips = sorted(
#             df_fips_yield_years.filter(pl.col("year") >= current_fips_fetch_start_year) 
#                                 .select("year").unique().to_series().to_list()
#         )

#         print(f"  Aggregating weather for FIPS {fips_code} for years: {relevant_yield_years_for_fips[:3]}...{relevant_yield_years_for_fips[-3:] if len(relevant_yield_years_for_fips) > 3 else ''} (Total: {len(relevant_yield_years_for_fips)})")

#         for year_to_aggregate in relevant_yield_years_for_fips:
#             daily_data_for_this_year = cleaned_df_for_fips.filter(pl.col("year") == year_to_aggregate)
            
#             if daily_data_for_this_year.height == 0:
#                 # This case should be rare if fetch_and_clean returned data for the overall period
#                 print(f"    Warning: No daily data found for FIPS {fips_code}, Year {year_to_aggregate} after initial fetch. Setting aggregates to NaN.")
#                 yearly_aggs = {"fips_full": fips_code, "year": year_to_aggregate}
#                 for prefix in ["GS", "SGF"]:
#                     yearly_aggs.update({
#                         f"GDD_{prefix}": np.nan, f"KDD_{prefix}": np.nan, f"TMAX_AVG_{prefix}": np.nan,
#                         f"PREC_{prefix}": np.nan, f"PREC2_{prefix}": np.nan, f"CHD_{prefix}": np.nan
#                     })
#             else:
#                 yearly_aggs = aggregate_weather_to_yearly(daily_data_for_this_year, year_to_aggregate)
#                 yearly_aggs["fips_full"] = fips_code # Ensure fips_full is in the dict
            
#             all_yearly_weather_aggregates.append(yearly_aggs)
        
#         # For years in df_fips_yield_years *before* config.DATA_START_YEAR, add NaN rows
#         pre_data_yield_years = sorted(
#             df_fips_yield_years.filter(pl.col("year") < overall_fetch_start_year)
#                                 .select("year").unique().to_series().to_list()
#         )
#         for year_to_fill_nan in pre_data_yield_years:
#             nan_aggs = {"fips_full": fips_code, "year": year_to_fill_nan}
#             for prefix in ["GS", "SGF"]:
#                  nan_aggs.update({
#                     f"GDD_{prefix}": np.nan, f"KDD_{prefix}": np.nan, f"TMAX_AVG_{prefix}": np.nan,
#                     f"PREC_{prefix}": np.nan, f"PREC2_{prefix}": np.nan, f"CHD_{prefix}": np.nan
#                 })
#             all_yearly_weather_aggregates.append(nan_aggs)


#     # --- End of Chunk 5 ---
#     if all_yearly_weather_aggregates:
#         df_final_weather_aggs = pl.DataFrame(all_yearly_weather_aggregates)
#         print(f"\n--- Chunk 5 Complete: Processed all FIPS. Total yearly aggregate records: {df_final_weather_aggs.height} ---")
#         print("Sample of aggregated weather data:")
#         print(df_final_weather_aggs.head())
#         # TODO: Chunk 6: Merge df_final_weather_aggs with df_yield and save
#     else:
#         print("\n--- Chunk 5 Complete: No yearly weather aggregates generated. ---")


#     print("\n--- Agricultural Weather Data Processing Finished ---")

# if __name__ == "__main__":
#     main()


###### VERSION FULLY CORRECT CHUNK 4, AFTER THIS PROCEEDED TO CHUNK 5

# # process_ag_weather.py

# # --- Standard Library Imports ---
# import os
# from typing import List, Dict, Optional, Any, Tuple 

# # --- Third-party Library Imports ---
# import polars as pl
# from polars.exceptions import ColumnNotFoundError
# import numpy as np # For NaN

# # --- Custom Module Imports ---
# from nclimgrid_importer import load_nclimgrid_data 
# import config

# def load_and_prepare_yield_data(csv_path: str) -> Optional[Tuple[pl.DataFrame, List[str], int, int]]:
#     """
#     Loads the yield data CSV, prepares FIPS codes, and extracts unique FIPS and year range.
#     (Function content remains the same - see previous version)
#     """
#     if not os.path.exists(csv_path):
#         print(f"Error: Input CSV file not found at '{csv_path}'")
#         return None
#     try:
#         yield_column_dtypes = {
#             "state": pl.String,
#             "county": pl.String,
#             "year": pl.Int64
#         }
#         df_yield = pl.read_csv(csv_path, schema_overrides=yield_column_dtypes, null_values=["NA", "N/A", "", " "])
#         print(f"\nSuccessfully loaded input CSV: '{csv_path}', Shape: {df_yield.shape}")
#         required_cols = ["state", "county", "year"]
#         for col_name_req in required_cols:
#             if col_name_req not in df_yield.columns:
#                 print(f"Error: CSV must contain a '{col_name_req}' column.")
#                 return None
#         df_yield = df_yield.drop_nulls(subset=["state", "county", "year"])
#         if df_yield.height == 0:
#             print("Error: No valid rows in CSV after dropping nulls in state, county, or year.")
#             return None
#         df_yield = df_yield.with_columns(
#             pl.col("county").str.zfill(3).alias("county_padded"),
#             (pl.col("state") + pl.col("county").str.zfill(3)).alias("fips_full") # Combined for efficiency
#         )
#         unique_fips_to_process = df_yield.select("fips_full").unique().to_series().to_list()
#         print(f"\nFound {len(unique_fips_to_process)} unique FIPS codes to process from yield data.")
#         if not unique_fips_to_process:
#             print("Error: No unique FIPS codes found.")
#             return None
#         min_year_csv = df_yield.select(pl.col("year").min()).item()
#         max_year_csv = df_yield.select(pl.col("year").max()).item()
#         print(f"Year range in CSV: {min_year_csv} - {max_year_csv}")
#         return df_yield, unique_fips_to_process, min_year_csv, max_year_csv
#     except Exception as e:
#         print(f"Error loading or processing input CSV: {type(e).__name__} - {e}")
#         return None

# def fetch_and_clean_daily_weather(
#     fips_code: str, 
#     start_year: int, 
#     end_year: int
# ) -> Tuple[Optional[pl.DataFrame], List[Dict[str, str]]]:
#     """
#     Fetches daily weather data for a given FIPS code and year range,
#     then performs initial cleaning (casting types, handling missing values).
#     (Function content remains largely the same - see previous version)
#     """
#     print(f"\nFetching daily weather for FIPS: {fips_code}, Years: {start_year}-{end_year}")
#     start_date_str = f"{start_year}-01-01"
#     end_date_str = f"{end_year}-12-31"
#     daily_arrow_table, file_load_errors = load_nclimgrid_data(
#         start_date=start_date_str, end_date=end_date_str, spatial_scale='cty', 
#         scaled=True, counties=[fips_code], variables=config.TARGET_DAILY_WEATHER_VARIABLES 
#     )
#     if daily_arrow_table is None or daily_arrow_table.num_rows == 0:
#         print(f"  No daily weather data returned or data was empty for FIPS {fips_code} for years {start_year}-{end_year}.")
#         return None, file_load_errors 
#     df_daily_weather = pl.from_arrow(daily_arrow_table)
#     if "date" in df_daily_weather.columns:
#         if df_daily_weather["date"].dtype != pl.Datetime:
#             try:
#                 df_daily_weather = df_daily_weather.with_columns(pl.col("date").cast(pl.Datetime))
#             except Exception as e:
#                 print(f"  Error casting 'date' column: {e}. Returning raw table & errors.")
#                 return df_daily_weather, file_load_errors 
#     else:
#         print("  Error: 'date' column missing. Returning None & errors.")
#         return None, file_load_errors
#     cast_and_clean_expressions = []
#     for col_name in config.TARGET_DAILY_WEATHER_VARIABLES:
#         if col_name in df_daily_weather.columns:
#             if df_daily_weather[col_name].dtype == pl.String:
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
#                     .then(None).otherwise(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False)).alias(col_name)
#                 )
#             elif df_daily_weather[col_name].dtype == pl.Float64:
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
#                     .then(None).otherwise(pl.col(col_name)).alias(col_name)
#                 )
#             elif df_daily_weather[col_name].dtype.is_numeric():
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name).cast(pl.Float64, strict=False) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
#                     .then(None).otherwise(pl.col(col_name).cast(pl.Float64, strict=False)).alias(col_name)
#                 )
#     if cast_and_clean_expressions:
#         try:
#             df_daily_weather = df_daily_weather.with_columns(cast_and_clean_expressions)
#         except Exception as e:
#             print(f"  Error applying casting/cleaning: {e}. Returning raw table & errors.")
#             return df_daily_weather, file_load_errors 
#     final_columns_to_keep = ["date"]
#     for var_name in config.TARGET_DAILY_WEATHER_VARIABLES:
#         if var_name in df_daily_weather.columns and df_daily_weather[var_name].dtype == pl.Float64:
#             final_columns_to_keep.append(var_name)
#         elif var_name in df_daily_weather.columns: 
#              if var_name not in final_columns_to_keep : final_columns_to_keep.append(var_name)
#     final_columns_to_keep = list(dict.fromkeys(final_columns_to_keep))
#     missing_final_cols = [col for col in final_columns_to_keep if col not in df_daily_weather.columns]
#     if missing_final_cols:
#         print(f"  Error: Columns {missing_final_cols} missing before final select. Available: {df_daily_weather.columns}")
#         return None, file_load_errors
#     df_daily_weather = df_daily_weather.select(final_columns_to_keep)
#     return df_daily_weather, file_load_errors

# # --- Chunk 3: Agronomic Variable Calculation Functions ---
# def calculate_daily_gdd(tavg_series: pl.Series) -> pl.Series:
#     tavg_float = tavg_series.cast(pl.Float64, strict=False) 
#     capped_tavg = pl.min_horizontal(tavg_float, pl.lit(config.GDD_MAX_TEMP_C, dtype=pl.Float64))
#     gdd_potential = capped_tavg - config.GDD_BASE_TEMP_C
#     daily_gdd = pl.max_horizontal(pl.lit(0.0, dtype=pl.Float64), gdd_potential)
#     return daily_gdd 

# def calculate_daily_kdd(tmax_series: pl.Series) -> pl.Series:
#     tmax_float = tmax_series.cast(pl.Float64, strict=False)
#     kdd_potential = tmax_float - config.KDD_TEMP_THRESHOLD_C
#     daily_kdd = pl.max_horizontal(pl.lit(0.0, dtype=pl.Float64), kdd_potential)
#     return daily_kdd

# def calculate_daily_chd(tmax_series: pl.Series, prcp_series: pl.Series) -> pl.Series:
#     tmax_float = tmax_series.cast(pl.Float64, strict=False)
#     prcp_float = prcp_series.cast(pl.Float64, strict=False)
#     is_hot = (tmax_float > config.CHD_TEMP_THRESHOLD_C).fill_null(False)
#     is_dry = (prcp_float < config.CHD_PRECIP_THRESHOLD_MM).fill_null(False)
#     daily_chd = (is_hot & is_dry).cast(pl.Int8) 
#     return daily_chd

# # --- Chunk 4: Yearly Aggregation Function (Refactored) ---
# def _calculate_seasonal_aggregates(
#     df_season: pl.DataFrame, 
#     season_prefix: str
# ) -> Dict[str, Any]:
#     """
#     Internal helper to calculate agronomic variables for a given seasonal DataFrame.
#     If any required daily value is null, the corresponding aggregate will be NaN.
#     """
#     aggs: Dict[str, Any] = {}
    
#     # Create temporary columns for daily calculations to ensure expressions are evaluated
#     df_season_with_daily_calcs = df_season.clone() # Work on a clone

#     # GDD
#     if "tavg" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("tavg").is_null().all():
#         if df_season_with_daily_calcs.get_column("tavg").is_null().any(): # Check for any nulls
#             aggs[f"GDD_{season_prefix}"] = np.nan
#         else:
#             df_season_with_daily_calcs = df_season_with_daily_calcs.with_columns(
#                 calculate_daily_gdd(df_season_with_daily_calcs.get_column("tavg")).alias("_daily_gdd")
#             )
#             aggs[f"GDD_{season_prefix}"] = df_season_with_daily_calcs.get_column("_daily_gdd").sum()
#     else: # Column missing or all nulls
#         aggs[f"GDD_{season_prefix}"] = np.nan

#     # KDD
#     if "tmax" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("tmax").is_null().all():
#         if df_season_with_daily_calcs.get_column("tmax").is_null().any():
#             aggs[f"KDD_{season_prefix}"] = np.nan
#         else:
#             df_season_with_daily_calcs = df_season_with_daily_calcs.with_columns(
#                 calculate_daily_kdd(df_season_with_daily_calcs.get_column("tmax")).alias("_daily_kdd")
#             )
#             aggs[f"KDD_{season_prefix}"] = df_season_with_daily_calcs.get_column("_daily_kdd").sum()
#     else:
#         aggs[f"KDD_{season_prefix}"] = np.nan
    
#     # TMAX_AVG
#     if "tmax" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("tmax").is_null().all():
#         if df_season_with_daily_calcs.get_column("tmax").is_null().any():
#             aggs[f"TMAX_AVG_{season_prefix}"] = np.nan
#         else:
#             aggs[f"TMAX_AVG_{season_prefix}"] = df_season_with_daily_calcs.get_column("tmax").mean()
#     else:
#         aggs[f"TMAX_AVG_{season_prefix}"] = np.nan

#     # PREC and PREC2
#     if "prcp" in df_season_with_daily_calcs.columns and not df_season_with_daily_calcs.get_column("prcp").is_null().all():
#         if df_season_with_daily_calcs.get_column("prcp").is_null().any():
#             aggs[f"PREC_{season_prefix}"] = np.nan
#             aggs[f"PREC2_{season_prefix}"] = np.nan
#         else:
#             prec_sum = df_season_with_daily_calcs.get_column("prcp").sum()
#             aggs[f"PREC_{season_prefix}"] = prec_sum
#             aggs[f"PREC2_{season_prefix}"] = prec_sum * prec_sum if prec_sum is not None else np.nan
#     else:
#         aggs[f"PREC_{season_prefix}"] = np.nan
#         aggs[f"PREC2_{season_prefix}"] = np.nan
        
#     # CHD
#     if ("tmax" in df_season_with_daily_calcs.columns and "prcp" in df_season_with_daily_calcs.columns and
#         not df_season_with_daily_calcs.get_column("tmax").is_null().all() and 
#         not df_season_with_daily_calcs.get_column("prcp").is_null().all()):
#         if df_season_with_daily_calcs.get_column("tmax").is_null().any() or df_season_with_daily_calcs.get_column("prcp").is_null().any():
#             aggs[f"CHD_{season_prefix}"] = np.nan
#         else:
#             df_season_with_daily_calcs = df_season_with_daily_calcs.with_columns(
#                 calculate_daily_chd(df_season_with_daily_calcs.get_column("tmax"), df_season_with_daily_calcs.get_column("prcp")).alias("_daily_chd")
#             )
#             aggs[f"CHD_{season_prefix}"] = df_season_with_daily_calcs.get_column("_daily_chd").sum()
#     else:
#         aggs[f"CHD_{season_prefix}"] = np.nan
        
#     return aggs

# def aggregate_weather_to_yearly(daily_df_for_year: pl.DataFrame, year: int) -> Dict[str, Any]:
#     """
#     Aggregates daily weather data to yearly agronomic variables for GS and SGF seasons.
#     """
#     all_aggs: Dict[str, Any] = {"year": year} 

#     # Growing Season (GS)
#     gs_start_date = pl.datetime(year, config.GS_START_MONTH, config.GS_START_DAY)
#     gs_end_date = pl.datetime(year, config.GS_END_MONTH, config.GS_END_DAY)
#     df_gs = daily_df_for_year.filter((pl.col("date") >= gs_start_date) & (pl.col("date") <= gs_end_date))
    
#     if df_gs.height == 0 or not all(col in df_gs.columns for col in config.TARGET_DAILY_WEATHER_VARIABLES if col != 'date'): # Check if all target weather vars are present
#         print(f"    Year {year}: Insufficient data or columns for GS calculations. Setting GS aggregates to NaN.")
#         gs_aggs = {
#             f"GDD_GS": np.nan, f"KDD_GS": np.nan, f"TMAX_AVG_GS": np.nan,
#             f"PREC_GS": np.nan, f"PREC2_GS": np.nan, f"CHD_GS": np.nan
#         }
#     else:
#         gs_aggs = _calculate_seasonal_aggregates(df_gs, "GS")
#     all_aggs.update(gs_aggs)

#     # Silking-Grain-Fill (SGF)
#     sgf_start_date = pl.datetime(year, config.SGF_START_MONTH, config.SGF_START_DAY)
#     sgf_end_date = pl.datetime(year, config.SGF_END_MONTH, config.SGF_END_DAY)
#     df_sgf = daily_df_for_year.filter((pl.col("date") >= sgf_start_date) & (pl.col("date") <= sgf_end_date))

#     if df_sgf.height == 0 or not all(col in df_sgf.columns for col in config.TARGET_DAILY_WEATHER_VARIABLES if col != 'date'):
#         print(f"    Year {year}: Insufficient data or columns for SGF calculations. Setting SGF aggregates to NaN.")
#         sgf_aggs = {
#             f"GDD_SGF": np.nan, f"KDD_SGF": np.nan, f"TMAX_AVG_SGF": np.nan,
#             f"PREC_SGF": np.nan, f"PREC2_SGF": np.nan, f"CHD_SGF": np.nan
#         }
#     else:
#         sgf_aggs = _calculate_seasonal_aggregates(df_sgf, "SGF")
#     all_aggs.update(sgf_aggs)
        
#     return all_aggs

# def main():
#     print("--- Starting Agricultural Weather Data Processing ---")
#     print(f"Using Growing Season: Month {config.GS_START_MONTH}/{config.GS_START_DAY} to {config.GS_END_MONTH}/{config.GS_END_DAY}")
#     print(f"Target daily weather variables to fetch: {config.TARGET_DAILY_WEATHER_VARIABLES}")
#     print(f"Weather data processing will start from year: {config.DATA_START_YEAR}")

#     input_csv_path = "df_yield_weather_20_05_14h00m.csv" 
    
#     prepared_data = load_and_prepare_yield_data(input_csv_path)
#     if prepared_data is None:
#         print("Failed to load and prepare yield data. Exiting.")
#         return

#     df_yield, unique_fips_to_process, min_year_csv, max_year_csv = prepared_data
    
#     fetch_start_year = config.DATA_START_YEAR
#     fetch_end_year = max_year_csv
#     print(f"Weather data will be fetched for years: {fetch_start_year} - {fetch_end_year} for each FIPS.")

#     if unique_fips_to_process:
#         first_fips = "17173"#unique_fips_to_process[0]
#         print(f"\n--- TESTING CHUNKS 2, 3 & 4 for first FIPS: {first_fips} ---")
#         try:
#             max_year_for_fips = df_yield.filter(pl.col("fips_full") == first_fips).select(pl.col("year").max()).item()
            
#             test_year = 1951 
#             if max_year_for_fips < test_year or test_year < fetch_start_year :
#                  print(f"Test year {test_year} is outside valid range for FIPS {first_fips} ({fetch_start_year}-{max_year_for_fips}). Skipping test.")
#             else:
#                 result_tuple = fetch_and_clean_daily_weather(first_fips, test_year, test_year)
                
#                 cleaned_df_for_year = None
#                 errors_for_fips = [] 

#                 if result_tuple is not None:
#                     cleaned_df_for_year, errors_for_fips = result_tuple

#                 if cleaned_df_for_year is not None and cleaned_df_for_year.height > 0:
#                     print(f"\nSuccessfully fetched and cleaned data for FIPS {first_fips}, Year {test_year}:")
#                     print(f"Shape: {cleaned_df_for_year.shape}")

#                     # Test daily calculations on a sample (e.g., June data)
#                     df_june_sample = cleaned_df_for_year.filter(pl.col('date').dt.month() == 6) # June
#                     print(f"\nTesting daily calculations on June {test_year} data (if available, {df_june_sample.height} rows):")
                    
#                     temp_df_june_with_daily_vars = df_june_sample.clone()

#                     if df_june_sample.height > 0:
#                         if "tavg" in temp_df_june_with_daily_vars.columns:
#                             temp_df_june_with_daily_vars = temp_df_june_with_daily_vars.with_columns(
#                                 calculate_daily_gdd(temp_df_june_with_daily_vars.get_column("tavg")).alias("daily_gdd_june")
#                             )
#                             print("Sample daily GDD (June):")
#                             print(temp_df_june_with_daily_vars.select("daily_gdd_june").head())
#                         if "tmax" in temp_df_june_with_daily_vars.columns:
#                             temp_df_june_with_daily_vars = temp_df_june_with_daily_vars.with_columns(
#                                 calculate_daily_kdd(temp_df_june_with_daily_vars.get_column("tmax")).alias("daily_kdd_june")
#                             )
#                             print("\nSample daily KDD (June):")
#                             print(temp_df_june_with_daily_vars.select("daily_kdd_june").head())
#                         if "tmax" in temp_df_june_with_daily_vars.columns and "prcp" in temp_df_june_with_daily_vars.columns:
#                              temp_df_june_with_daily_vars = temp_df_june_with_daily_vars.with_columns(
#                                 calculate_daily_chd(temp_df_june_with_daily_vars.get_column("tmax"), temp_df_june_with_daily_vars.get_column("prcp")).alias("daily_chd_june")
#                             )
#                              print("\nSample daily CHD (June):")
#                              print(temp_df_june_with_daily_vars.select("daily_chd_june").head())
#                     else:
#                         print(f"No data available for June {test_year} to test daily calculations.")

#                     # Test Chunk 4 function (Yearly Aggregation)
#                     print(f"\nTesting yearly aggregation for FIPS {first_fips}, Year {test_year}:")
#                     yearly_aggs = aggregate_weather_to_yearly(cleaned_df_for_year, test_year)
#                     print("Calculated Yearly Aggregates:")
#                     for key, value in yearly_aggs.items():
#                         print(f"  {key}: {value}")
#                 else:
#                     print(f"\nFailed to fetch/clean data, or data empty for FIPS {first_fips}, Year {test_year}.")
                
#                 if errors_for_fips: 
#                     print(f"\nErrors during S3 load for FIPS {first_fips}, Year {test_year}:")
#                     for error_info in errors_for_fips:
#                         print(f"  - File {error_info['yyyymm']}: {error_info['error_type']}")
#         except Exception as e:
#             print(f"Error during test for FIPS {first_fips}: {type(e).__name__} - {e}")
#     else:
#         print("No unique FIPS codes to test.")

#     # TODO: Chunk 5: Main processing loop (iterate unique_fips, fetch, aggregate, report errors)
#     # TODO: Chunk 6: Merge results and save

#     print("\n--- Agricultural Weather Data Processing Finished (Chunks 1-4 Defined, Chunks 2-4 Tested for one FIPS-Year) ---")

# if __name__ == "__main__":
#     main()



###### VERSION FULLY CORRECT BUT NO CHUNK 4.

# # process_ag_weather.py

# # --- Standard Library Imports ---
# import os
# from typing import List, Dict, Optional, Any, Tuple # For type hinting

# # --- Third-party Library Imports ---
# import polars as pl
# from polars.exceptions import ColumnNotFoundError # To catch errors if a column is missing

# # --- Custom Module Imports ---
# # Assuming nclimgrid_importer.py is in the same directory or accessible in PYTHONPATH
# from nclimgrid_importer import load_nclimgrid_data # From python_script_s3_debug

# # Import constants from our config.py file
# # Assuming config.py is in the same directory
# import config

# def load_and_prepare_yield_data(csv_path: str) -> Optional[Tuple[pl.DataFrame, List[str], int, int]]:
#     """
#     Loads the yield data CSV, prepares FIPS codes, and extracts unique FIPS and year range.
#     """
#     if not os.path.exists(csv_path):
#         print(f"Error: Input CSV file not found at '{csv_path}'")
#         return None

#     try:
#         yield_column_dtypes = {
#             "state": pl.String,
#             "county": pl.String,
#             "year": pl.Int64
#         }
#         # Use schema_overrides instead of dtypes for newer Polars versions
#         df_yield = pl.read_csv(csv_path, schema_overrides=yield_column_dtypes, null_values=["NA", "N/A", "", " "])
        
#         print(f"\nSuccessfully loaded input CSV: '{csv_path}'")
#         print(f"Input CSV shape: {df_yield.shape}")

#         required_cols = ["state", "county", "year"]
#         for col_name_req in required_cols: # Renamed col to col_name_req to avoid conflict
#             if col_name_req not in df_yield.columns:
#                 print(f"Error: CSV must contain a '{col_name_req}' column.")
#                 return None
        
#         df_yield = df_yield.drop_nulls(subset=["state", "county", "year"])
#         if df_yield.height == 0:
#             print("Error: No valid rows remaining in CSV after dropping nulls in state, county, or year.")
#             return None

#         df_yield = df_yield.with_columns(
#             pl.col("county").str.zfill(3).alias("county_padded")
#         )
#         df_yield = df_yield.with_columns(
#             (pl.col("state") + pl.col("county_padded")).alias("fips_full")
#         )
        
#         print("\nFirst 5 rows after FIPS processing (from yield data):")
#         print(df_yield.head(5).select(["year", "state", "county", "county_padded", "fips_full"]))

#         unique_fips_to_process = df_yield.select("fips_full").unique().to_series().to_list()
#         print(f"\nFound {len(unique_fips_to_process)} unique FIPS codes to process from yield data.")
#         print(f"Sample unique FIPS: {unique_fips_to_process[:5] if unique_fips_to_process else 'None'}")

#         if not unique_fips_to_process:
#             print("Error: No unique FIPS codes found to process.")
#             return None

#         min_year_csv = df_yield.select(pl.col("year").min()).item()
#         max_year_csv = df_yield.select(pl.col("year").max()).item()
#         print(f"Year range in CSV: {min_year_csv} - {max_year_csv}")
        
#         return df_yield, unique_fips_to_process, min_year_csv, max_year_csv

#     except Exception as e:
#         print(f"Error loading or processing input CSV: {type(e).__name__} - {e}")
#         return None

# def fetch_and_clean_daily_weather(
#     fips_code: str, 
#     start_year: int, 
#     end_year: int
# ) -> Tuple[Optional[pl.DataFrame], List[Dict[str, str]]]:
#     """
#     Fetches daily weather data for a given FIPS code and year range,
#     then performs initial cleaning (casting types, handling missing values).
#     """
#     print(f"\nFetching daily weather for FIPS: {fips_code}, Years: {start_year}-{end_year}")
    
#     start_date_str = f"{start_year}-01-01"
#     end_date_str = f"{end_year}-12-31"

#     daily_arrow_table, file_load_errors = load_nclimgrid_data(
#         start_date=start_date_str,
#         end_date=end_date_str,
#         spatial_scale='cty', 
#         scaled=True,         
#         counties=[fips_code], 
#         variables=config.TARGET_DAILY_WEATHER_VARIABLES 
#     )

#     if daily_arrow_table is None or daily_arrow_table.num_rows == 0:
#         print(f"  No daily weather data returned or data was empty from S3 for FIPS {fips_code} for years {start_year}-{end_year}.")
#         return None, file_load_errors 
    
#     print(f"  Successfully fetched {daily_arrow_table.num_rows} daily records from S3 for FIPS {fips_code}.")
#     df_daily_weather = pl.from_arrow(daily_arrow_table)

#     if "date" in df_daily_weather.columns:
#         if df_daily_weather["date"].dtype != pl.Datetime:
#             try:
#                 df_daily_weather = df_daily_weather.with_columns(pl.col("date").cast(pl.Datetime))
#                 print("  Successfully cast 'date' column to Datetime.")
#             except Exception as e:
#                 print(f"  Error casting 'date' column to Datetime: {e}. Returning raw table and errors.")
#                 return df_daily_weather, file_load_errors 
#     else:
#         print("  Error: 'date' column missing from fetched weather data. Returning None and errors.")
#         return None, file_load_errors
        
#     cast_and_clean_expressions = []
#     for col_name in config.TARGET_DAILY_WEATHER_VARIABLES:
#         if col_name in df_daily_weather.columns:
#             if df_daily_weather[col_name].dtype == pl.String:
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
#                     .then(None) 
#                     .otherwise(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False))
#                     .alias(col_name)
#                 )
#             elif df_daily_weather[col_name].dtype == pl.Float64:
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
#                     .then(None)
#                     .otherwise(pl.col(col_name))
#                     .alias(col_name)
#                 )
#             elif df_daily_weather[col_name].dtype.is_numeric():
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name).cast(pl.Float64, strict=False) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER) 
#                     .then(None)
#                     .otherwise(pl.col(col_name).cast(pl.Float64, strict=False)) 
#                     .alias(col_name)
#                 )
#     if cast_and_clean_expressions:
#         try:
#             df_daily_weather = df_daily_weather.with_columns(cast_and_clean_expressions)
#             print("  Successfully applied cleaning and casting to weather variable columns.")
#         except Exception as e:
#             print(f"  Error applying casting/cleaning expressions: {e}. Returning raw table and errors.")
#             return df_daily_weather, file_load_errors 
            
#     final_columns_to_keep = ["date"]
#     for var_name in config.TARGET_DAILY_WEATHER_VARIABLES:
#         if var_name in df_daily_weather.columns and df_daily_weather[var_name].dtype == pl.Float64:
#             final_columns_to_keep.append(var_name)
#         elif var_name in df_daily_weather.columns: 
#              print(f"  Warning: Column '{var_name}' exists but is not Float64 after processing ({df_daily_weather[var_name].dtype}).")
#              if var_name not in final_columns_to_keep : final_columns_to_keep.append(var_name)

#     final_columns_to_keep = list(dict.fromkeys(final_columns_to_keep))
    
#     missing_final_cols = [col for col in final_columns_to_keep if col not in df_daily_weather.columns]
#     if missing_final_cols:
#         print(f"  Error: Columns {missing_final_cols} are missing from DataFrame before final select. Available: {df_daily_weather.columns}")
#         return None, file_load_errors

#     df_daily_weather = df_daily_weather.select(final_columns_to_keep)
#     print(f"  Cleaned daily weather data for FIPS {fips_code} has shape: {df_daily_weather.shape}")
    
#     return df_daily_weather, file_load_errors

# # --- Chunk 3: Agronomic Variable Calculation Functions ---

# def calculate_daily_gdd(tavg_series: pl.Series) -> pl.Series:
#     """
#     Calculates daily Growing Degree Days (GDD).
#     Formula: max(0, min(T_avg, GDD_MAX_TEMP_C) - GDD_BASE_TEMP_C)
#     Assumes tavg_series is already cleaned (numeric, missing values handled).
#     """
#     tavg_float = tavg_series.cast(pl.Float64, strict=False) # Ensure float for operations
#     capped_tavg = pl.min_horizontal(tavg_float, pl.lit(config.GDD_MAX_TEMP_C, dtype=pl.Float64)) # Ensure literal is float
#     gdd_potential = capped_tavg - config.GDD_BASE_TEMP_C
#     daily_gdd = pl.max_horizontal(pl.lit(0.0, dtype=pl.Float64), gdd_potential) # Ensure literal is float
#     return daily_gdd.fill_null(0.0)

# def calculate_daily_kdd(tmax_series: pl.Series) -> pl.Series:
#     """
#     Calculates daily Killing Degree Days (KDD).
#     Formula: max(0, T_max - KDD_TEMP_THRESHOLD_C)
#     Assumes tmax_series is already cleaned (numeric, missing values handled).
#     """
#     tmax_float = tmax_series.cast(pl.Float64, strict=False) # Ensure float
#     kdd_potential = tmax_float - config.KDD_TEMP_THRESHOLD_C
#     daily_kdd = pl.max_horizontal(pl.lit(0.0, dtype=pl.Float64), kdd_potential) # Ensure literal is float
#     return daily_kdd.fill_null(0.0)

# def calculate_daily_chd(tmax_series: pl.Series, prcp_series: pl.Series) -> pl.Series:
#     """
#     Identifies concurrent hot and dry days (CHD).
#     Returns 1 if T_max > CHD_TEMP_THRESHOLD_C AND precip < CHD_PRECIP_THRESHOLD_MM, else 0.
#     Assumes tmax_series and prcp_series are already cleaned.
#     """
#     tmax_float = tmax_series.cast(pl.Float64, strict=False) # Ensure float
#     prcp_float = prcp_series.cast(pl.Float64, strict=False) # Ensure float

#     is_hot = tmax_float > config.CHD_TEMP_THRESHOLD_C
#     is_dry = prcp_float < config.CHD_PRECIP_THRESHOLD_MM
    
#     daily_chd = (is_hot.fill_null(False) & is_dry.fill_null(False)).cast(pl.Int8)
#     return daily_chd


# def main():
#     print("--- Starting Agricultural Weather Data Processing ---")
#     print(f"Using Growing Season: Month {config.GS_START_MONTH}/{config.GS_START_DAY} to {config.GS_END_MONTH}/{config.GS_END_DAY}")
#     print(f"Target daily weather variables to fetch: {config.TARGET_DAILY_WEATHER_VARIABLES}")
#     print(f"Weather data processing will start from year: {config.DATA_START_YEAR}")

#     input_csv_path = "df_yield_weather_20_05_14h00m.csv" # Make sure this filename is correct
    
#     prepared_data = load_and_prepare_yield_data(input_csv_path)
#     if prepared_data is None:
#         print("Failed to load and prepare yield data. Exiting.")
#         return

#     df_yield, unique_fips_to_process, min_year_csv, max_year_csv = prepared_data
    
#     fetch_start_year = config.DATA_START_YEAR
#     fetch_end_year = max_year_csv
#     print(f"Weather data will be fetched for years: {fetch_start_year} - {fetch_end_year} for each FIPS.")

#     if unique_fips_to_process:
#         first_fips = unique_fips_to_process[0]
#         print(f"\n--- TESTING CHUNK 2 & 3 for first FIPS: {first_fips} ---")
#         try:
#             max_year_for_fips = df_yield.filter(pl.col("fips_full") == first_fips).select(pl.col("year").max()).item()
#             if max_year_for_fips < fetch_start_year:
#                  print(f"Max year for FIPS {first_fips} ({max_year_for_fips}) is before data start year ({fetch_start_year}). Skipping weather fetch test.")
#             else:
#                 current_fips_end_year = max(fetch_start_year, min(fetch_end_year, max_year_for_fips))
                
#                 result_tuple = fetch_and_clean_daily_weather(first_fips, fetch_start_year, current_fips_end_year)
                
#                 cleaned_df = None
#                 errors_for_fips = [] 

#                 if result_tuple is not None:
#                     cleaned_df, errors_for_fips = result_tuple

#                 if cleaned_df is not None and cleaned_df.height > 0:
#                     print(f"\nSuccessfully fetched and cleaned data for FIPS {first_fips}:")
#                     print(cleaned_df.head())
#                     print(f"Shape: {cleaned_df.shape}")

#                     # --- Test Chunk 3 functions ---
#                     # Create a temporary DataFrame with the calculated daily values for testing
#                     temp_df_with_daily_vars = cleaned_df.clone() # Clone to avoid modifying cleaned_df directly in test

#                     if "tavg" in temp_df_with_daily_vars.columns:
#                         temp_df_with_daily_vars = temp_df_with_daily_vars.with_columns(
#                             calculate_daily_gdd(temp_df_with_daily_vars.get_column("tavg")).alias("daily_gdd")
#                         )
#                         print("\nSample daily GDD (from test calculation):")
#                         print(temp_df_with_daily_vars.select("daily_gdd").head())
#                     else: print("Column 'tavg' not available for GDD calculation.")

#                     if "tmax" in temp_df_with_daily_vars.columns:
#                         temp_df_with_daily_vars = temp_df_with_daily_vars.with_columns(
#                             calculate_daily_kdd(temp_df_with_daily_vars.get_column("tmax")).alias("daily_kdd")
#                         )
#                         print("\nSample daily KDD (from test calculation):")
#                         print(temp_df_with_daily_vars.select("daily_kdd").head())
#                     else: print("Column 'tmax' not available for KDD calculation.")
                    
#                     if "tmax" in temp_df_with_daily_vars.columns and "prcp" in temp_df_with_daily_vars.columns:
#                         temp_df_with_daily_vars = temp_df_with_daily_vars.with_columns(
#                            calculate_daily_chd(temp_df_with_daily_vars.get_column("tmax"), temp_df_with_daily_vars.get_column("prcp")).alias("daily_chd")
#                         )
#                         print("\nSample daily CHD (1 if hot & dry, 0 otherwise - from test calculation):")
#                         print(temp_df_with_daily_vars.select("daily_chd").head())
#                         print(f"Total CHD days in fetched period for {first_fips}: {temp_df_with_daily_vars.get_column('daily_chd').sum()}")
#                     else: print("Columns 'tmax' or 'prcp' not available for CHD calculation.")

#                 else:
#                     print(f"\nFailed to fetch or clean data, or data was empty for FIPS {first_fips}.")
                
#                 if errors_for_fips:
#                     print(f"\nErrors encountered during S3 load for FIPS {first_fips}:")
#                     for error_info in errors_for_fips:
#                         print(f"  - File relating to YYYYMM '{error_info['yyyymm']}': {error_info['error_type']} - {error_info['error_message']}")
#                 else:
#                     print(f"\nNo S3 loading errors reported for FIPS {first_fips}.")

#         except Exception as e:
#             print(f"Error during test fetch for FIPS {first_fips}: {type(e).__name__} - {e}")
#     else:
#         print("No unique FIPS codes to test weather fetching.")

#     # TODO: Chunk 4: Define yearly aggregation function
#     # TODO: Chunk 5: Main processing loop (iterate unique_fips, fetch, aggregate, report errors)
#     # TODO: Chunk 6: Merge results and save

#     print("\n--- Agricultural Weather Data Processing Finished (Chunks 1, 2 & 3 Defined, Chunks 2 & 3 Tested for one FIPS) ---")

# if __name__ == "__main__":
#     main()



# ###### SECOND VERSION!!!: WE REPORT THE ERRORS BUT DONT HAVE THE GDD ETC YET. 

# # process_ag_weather.py

# # --- Standard Library Imports ---
# import os
# from typing import List, Dict, Optional, Any, Tuple # For type hinting

# # --- Third-party Library Imports ---
# import polars as pl
# from polars.exceptions import ColumnNotFoundError # To catch errors if a column is missing

# # --- Custom Module Imports ---
# # Assuming nclimgrid_importer.py is in the same directory or accessible in PYTHONPATH
# from nclimgrid_importer import load_nclimgrid_data # From python_script_s3_debug

# # Import constants from our config.py file
# # Assuming config.py is in the same directory
# import config

# def load_and_prepare_yield_data(csv_path: str) -> Optional[Tuple[pl.DataFrame, List[str], int, int]]:
#     """
#     Loads the yield data CSV, prepares FIPS codes, and extracts unique FIPS and year range.
#     (Function content remains the same as your provided version)
#     """
#     if not os.path.exists(csv_path):
#         print(f"Error: Input CSV file not found at '{csv_path}'")
#         return None

#     try:
#         yield_column_dtypes = {
#             "state": pl.String,
#             "county": pl.String,
#             "year": pl.Int64
#         }
#         df_yield = pl.read_csv(csv_path, dtypes=yield_column_dtypes, null_values=["NA", "N/A", "", " "])
        
#         print(f"\nSuccessfully loaded input CSV: '{csv_path}'")
#         print(f"Input CSV shape: {df_yield.shape}")

#         required_cols = ["state", "county", "year"]
#         for col in required_cols:
#             if col not in df_yield.columns:
#                 print(f"Error: CSV must contain a '{col}' column.")
#                 return None
        
#         df_yield = df_yield.drop_nulls(subset=["state", "county", "year"])
#         if df_yield.height == 0:
#             print("Error: No valid rows remaining in CSV after dropping nulls in state, county, or year.")
#             return None

#         df_yield = df_yield.with_columns(
#             pl.col("county").str.zfill(3).alias("county_padded")
#         )
#         df_yield = df_yield.with_columns(
#             (pl.col("state") + pl.col("county_padded")).alias("fips_full")
#         )
        
#         print("\nFirst 5 rows after FIPS processing (from yield data):")
#         print(df_yield.head(5).select(["year", "state", "county", "county_padded", "fips_full"]))

#         unique_fips_to_process = df_yield.select("fips_full").unique().to_series().to_list()
#         print(f"\nFound {len(unique_fips_to_process)} unique FIPS codes to process from yield data.")
#         print(f"Sample unique FIPS: {unique_fips_to_process[:5] if unique_fips_to_process else 'None'}")

#         if not unique_fips_to_process:
#             print("Error: No unique FIPS codes found to process.")
#             return None

#         min_year_csv = df_yield.select(pl.col("year").min()).item()
#         max_year_csv = df_yield.select(pl.col("year").max()).item()
#         print(f"Year range in CSV: {min_year_csv} - {max_year_csv}")
        
#         return df_yield, unique_fips_to_process, min_year_csv, max_year_csv

#     except Exception as e:
#         print(f"Error loading or processing input CSV: {type(e).__name__} - {e}")
#         return None

# def fetch_and_clean_daily_weather(
#     fips_code: str, 
#     start_year: int, 
#     end_year: int
# ) -> Tuple[Optional[pl.DataFrame], List[Dict[str, str]]]: # Return type updated
#     """
#     Fetches daily weather data for a given FIPS code and year range,
#     then performs initial cleaning (casting types, handling missing values).

#     Args:
#         fips_code (str): The 5-digit county FIPS code.
#         start_year (int): The first year of data to fetch (e.g., 1951).
#         end_year (int): The last year of data to fetch.

#     Returns:
#         Tuple containing:
#             - Optional[pl.DataFrame]: Cleaned daily weather data.
#             - List[Dict[str, str]]: List of errors encountered during S3 load.
#     """
#     print(f"\nFetching daily weather for FIPS: {fips_code}, Years: {start_year}-{end_year}")
    
#     start_date_str = f"{start_year}-01-01"
#     end_date_str = f"{end_year}-12-31"

#     # load_nclimgrid_data now returns a tuple: (ArrowTable or None, list_of_errors)
#     daily_arrow_table, file_load_errors = load_nclimgrid_data(
#         start_date=start_date_str,
#         end_date=end_date_str,
#         spatial_scale='cty', 
#         scaled=True,         
#         counties=[fips_code], 
#         variables=config.TARGET_DAILY_WEATHER_VARIABLES 
#     )

#     if daily_arrow_table is None or daily_arrow_table.num_rows == 0:
#         # Even if table is None/empty, file_load_errors might contain info (e.g., error reading first file)
#         print(f"  No daily weather data returned or data was empty from S3 for FIPS {fips_code} for years {start_year}-{end_year}.")
#         return None, file_load_errors # Return errors even if no data
    
#     print(f"  Successfully fetched {daily_arrow_table.num_rows} daily records from S3 for FIPS {fips_code}.")
#     df_daily_weather = pl.from_arrow(daily_arrow_table)

#     # Ensure 'date' column is datetime (rest of the cleaning logic is the same)
#     if "date" in df_daily_weather.columns:
#         if df_daily_weather["date"].dtype != pl.Datetime:
#             try:
#                 df_daily_weather = df_daily_weather.with_columns(pl.col("date").cast(pl.Datetime))
#                 print("  Successfully cast 'date' column to Datetime.")
#             except Exception as e:
#                 print(f"  Error casting 'date' column to Datetime: {e}. Returning raw table and errors.")
#                 # It might be better to return the uncast df_daily_weather and errors if casting fails
#                 return df_daily_weather, file_load_errors 
#     else:
#         print("  Error: 'date' column missing from fetched weather data. Returning None and errors.")
#         return None, file_load_errors
        
#     cast_and_clean_expressions = []
#     for col_name in config.TARGET_DAILY_WEATHER_VARIABLES:
#         if col_name in df_daily_weather.columns:
#             if df_daily_weather[col_name].dtype == pl.String:
#                 # print(f"  Cleaning and casting column '{col_name}' from String to Float64.") # Less verbose
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
#                     .then(None) 
#                     .otherwise(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False))
#                     .alias(col_name)
#                 )
#             elif df_daily_weather[col_name].dtype == pl.Float64:
#                 # print(f"  Column '{col_name}' is already Float64. Handling missing value placeholder.") # Less verbose
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
#                     .then(None)
#                     .otherwise(pl.col(col_name))
#                     .alias(col_name)
#                 )
#             elif df_daily_weather[col_name].dtype.is_numeric():
#                 # print(f"  Column '{col_name}' is numeric ({df_daily_weather[col_name].dtype}). Casting to Float64 and handling missing value placeholder.") # Less verbose
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name).cast(pl.Float64, strict=False) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER) 
#                     .then(None)
#                     .otherwise(pl.col(col_name).cast(pl.Float64, strict=False)) 
#                     .alias(col_name)
#                 )
#             # else: # Non-string, non-numeric - already handled by TARGET_DAILY_WEATHER_VARIABLES selection
#             #     print(f"  Warning: Column '{col_name}' is type {df_daily_weather[col_name].dtype} and will not be cast or cleaned.")
#         # else: # Warning for missing expected columns is now in load_nclimgrid_data
#         #     print(f"  Warning: Expected weather variable column '{col_name}' not found in fetched data.")


#     if cast_and_clean_expressions:
#         try:
#             df_daily_weather = df_daily_weather.with_columns(cast_and_clean_expressions)
#             print("  Successfully applied cleaning and casting to weather variable columns.")
#         except Exception as e:
#             print(f"  Error applying casting/cleaning expressions: {e}. Returning raw table and errors.")
#             return df_daily_weather, file_load_errors # Return current df and errors
            
#     final_columns_to_keep = ["date"]
#     for var_name in config.TARGET_DAILY_WEATHER_VARIABLES:
#         if var_name in df_daily_weather.columns and df_daily_weather[var_name].dtype == pl.Float64:
#             final_columns_to_keep.append(var_name)
#         elif var_name in df_daily_weather.columns: 
#              print(f"  Warning: Column '{var_name}' exists but is not Float64 after processing ({df_daily_weather[var_name].dtype}). It might not be usable for calculations.")
#              if var_name not in final_columns_to_keep : final_columns_to_keep.append(var_name)

#     final_columns_to_keep = list(dict.fromkeys(final_columns_to_keep))
    
#     missing_final_cols = [col for col in final_columns_to_keep if col not in df_daily_weather.columns]
#     if missing_final_cols:
#         print(f"  Error: Columns {missing_final_cols} are missing from DataFrame before final select. Available: {df_daily_weather.columns}")
#         return None, file_load_errors

#     df_daily_weather = df_daily_weather.select(final_columns_to_keep)
#     print(f"  Cleaned daily weather data for FIPS {fips_code} has shape: {df_daily_weather.shape}")
#     # print(f"  Schema after cleaning: {df_daily_weather.schema}") # Less verbose
    
#     return df_daily_weather, file_load_errors


# def main():
#     print("--- Starting Agricultural Weather Data Processing ---")
#     print(f"Using Growing Season: Month {config.GS_START_MONTH}/{config.GS_START_DAY} to {config.GS_END_MONTH}/{config.GS_END_DAY}")
#     print(f"Target daily weather variables to fetch: {config.TARGET_DAILY_WEATHER_VARIABLES}")
#     print(f"Weather data processing will start from year: {config.DATA_START_YEAR}")

#     input_csv_path = "df_yield_weather_20_05_14h00m.csv" # Make sure this filename is correct
    
#     prepared_data = load_and_prepare_yield_data(input_csv_path)
#     if prepared_data is None:
#         print("Failed to load and prepare yield data. Exiting.")
#         return

#     df_yield, unique_fips_to_process, min_year_csv, max_year_csv = prepared_data
    
#     fetch_start_year = config.DATA_START_YEAR
#     fetch_end_year = max_year_csv
#     print(f"Weather data will be fetched for years: {fetch_start_year} - {fetch_end_year} for each FIPS.")

#     # --- TESTING CHUNK 2 for first FIPS ---
#     if unique_fips_to_process:
#         first_fips = '31109'#unique_fips_to_process[0]
#         print(f"\n--- TESTING CHUNK 2 for first FIPS: {first_fips} ---")
#         try:
#             max_year_for_fips = df_yield.filter(pl.col("fips_full") == first_fips).select(pl.col("year").max()).item()
#             if max_year_for_fips < fetch_start_year:
#                  print(f"Max year for FIPS {first_fips} ({max_year_for_fips}) is before data start year ({fetch_start_year}). Skipping weather fetch test.")
#             else:
#                 current_fips_end_year = max(fetch_start_year, min(fetch_end_year, max_year_for_fips))
                
#                 # fetch_and_clean_daily_weather now returns a tuple
#                 result_tuple = fetch_and_clean_daily_weather(first_fips, fetch_start_year, current_fips_end_year)
                
#                 cleaned_df = None
#                 errors_for_fips = [] # Initialize with empty list

#                 if result_tuple is not None:
#                     cleaned_df, errors_for_fips = result_tuple

#                 if cleaned_df is not None:
#                     print(f"\nSuccessfully fetched and cleaned data for FIPS {first_fips}:")
#                     print(cleaned_df.head())
#                     print(f"Shape: {cleaned_df.shape}")
#                 else:
#                     print(f"\nFailed to fetch or clean data for FIPS {first_fips}.")
                
#                 if errors_for_fips:
#                     print(f"\nErrors encountered during S3 load for FIPS {first_fips}:")
#                     for error_info in errors_for_fips:
#                         print(f"  - File relating to YYYYMM '{error_info['yyyymm']}': {error_info['error_type']} - {error_info['error_message']}")
#                 else:
#                     print(f"\nNo S3 loading errors reported for FIPS {first_fips}.")

#         except Exception as e:
#             print(f"Error during test fetch for FIPS {first_fips}: {type(e).__name__} - {e}")
#     else:
#         print("No unique FIPS codes to test weather fetching.")

#     # TODO: Chunk 3: Define agronomic variable calculation functions
#     # TODO: Chunk 4: Define yearly aggregation function
#     # TODO: Chunk 5: Main processing loop (iterate unique_fips, fetch, aggregate, report errors)
#     # TODO: Chunk 6: Merge results and save

#     print("\n--- Agricultural Weather Data Processing Finished (Chunk 1 & 2 Defined, Chunk 2 Tested for one FIPS with Error Reporting) ---")

# if __name__ == "__main__":
#     main()




##### FIRST VERSION!!!: BEFORE WE DO IN STEPS OF 10 AND REPORT THE ERRORS

# # process_ag_weather.py

# # --- Standard Library Imports ---
# import os
# from typing import List, Dict, Optional, Any, Tuple # For type hinting

# # --- Third-party Library Imports ---
# import polars as pl
# from polars.exceptions import ColumnNotFoundError # To catch errors if a column is missing

# # --- Custom Module Imports ---
# # Assuming nclimgrid_importer.py is in the same directory or accessible in PYTHONPATH
# from nclimgrid_importer import load_nclimgrid_data # From python_script_s3_debug

# # Import constants from our config.py file
# # Assuming config.py is in the same directory
# import config

# def load_and_prepare_yield_data(csv_path: str) -> Optional[Tuple[pl.DataFrame, List[str], int, int]]:
#     """
#     Loads the yield data CSV, prepares FIPS codes, and extracts unique FIPS and year range.

#     Args:
#         csv_path (str): Path to the input CSV file.

#     Returns:
#         Optional[Tuple[pl.DataFrame, List[str], int, int]]: 
#             A tuple containing:
#             - df_yield: The processed Polars DataFrame with yield data and 'fips_full'.
#             - unique_fips_to_process: A list of unique 5-digit FIPS codes.
#             - min_year_csv: The minimum year found in the CSV.
#             - max_year_csv: The maximum year found in the CSV.
#             Returns None if loading or initial processing fails.
#     """
#     if not os.path.exists(csv_path):
#         print(f"Error: Input CSV file not found at '{csv_path}'")
#         return None

#     try:
#         yield_column_dtypes = {
#             "state": pl.String,
#             "county": pl.String,
#             "year": pl.Int64
#         }
#         # Attempt to read with specified dtypes, add null_values for common missing data strings
#         df_yield = pl.read_csv(csv_path, dtypes=yield_column_dtypes, null_values=["NA", "N/A", "", " "])
        
#         print(f"\nSuccessfully loaded input CSV: '{csv_path}'")
#         print(f"Input CSV shape: {df_yield.shape}")

#         required_cols = ["state", "county", "year"]
#         for col in required_cols:
#             if col not in df_yield.columns:
#                 print(f"Error: CSV must contain a '{col}' column.")
#                 return None
        
#         # Drop rows where essential FIPS components or year might be null after loading
#         df_yield = df_yield.drop_nulls(subset=["state", "county", "year"])
#         if df_yield.height == 0:
#             print("Error: No valid rows remaining in CSV after dropping nulls in state, county, or year.")
#             return None

#         df_yield = df_yield.with_columns(
#             pl.col("county").str.zfill(3).alias("county_padded")
#         )
#         df_yield = df_yield.with_columns(
#             (pl.col("state") + pl.col("county_padded")).alias("fips_full")
#         )
        
#         print("\nFirst 5 rows after FIPS processing (from yield data):")
#         print(df_yield.head(5).select(["year", "state", "county", "county_padded", "fips_full"]))

#         unique_fips_to_process = df_yield.select("fips_full").unique().to_series().to_list()
#         print(f"\nFound {len(unique_fips_to_process)} unique FIPS codes to process from yield data.")
#         print(f"Sample unique FIPS: {unique_fips_to_process[:5] if unique_fips_to_process else 'None'}")

#         if not unique_fips_to_process:
#             print("Error: No unique FIPS codes found to process.")
#             return None

#         min_year_csv = df_yield.select(pl.col("year").min()).item()
#         max_year_csv = df_yield.select(pl.col("year").max()).item()
#         print(f"Year range in CSV: {min_year_csv} - {max_year_csv}")
        
#         return df_yield, unique_fips_to_process, min_year_csv, max_year_csv

#     except Exception as e:
#         print(f"Error loading or processing input CSV: {type(e).__name__} - {e}")
#         return None

# def fetch_and_clean_daily_weather(
#     fips_code: str, 
#     start_year: int, 
#     end_year: int
# ) -> Optional[pl.DataFrame]:
#     """
#     Fetches daily weather data for a given FIPS code and year range,
#     then performs initial cleaning (casting types, handling missing values).

#     Args:
#         fips_code (str): The 5-digit county FIPS code.
#         start_year (int): The first year of data to fetch (e.g., 1951).
#         end_year (int): The last year of data to fetch.

#     Returns:
#         Optional[pl.DataFrame]: A Polars DataFrame with cleaned daily weather data,
#                                  or None if fetching or cleaning fails.
#     """
#     print(f"\nFetching daily weather for FIPS: {fips_code}, Years: {start_year}-{end_year}")
    
#     start_date_str = f"{start_year}-01-01"
#     end_date_str = f"{end_year}-12-31"

#     # Call load_nclimgrid_data (from nclimgrid_importer.py)
#     # load_nclimgrid_data expects 'counties' to be a list
#     daily_arrow_table = load_nclimgrid_data(
#         start_date=start_date_str,
#         end_date=end_date_str,
#         spatial_scale='cty', # Assuming county level data
#         scaled=True,         # Assuming scaled data
#         counties=[fips_code], # Pass the single FIPS code as a list
#         variables=config.TARGET_DAILY_WEATHER_VARIABLES 
#     )

#     if daily_arrow_table is None or daily_arrow_table.num_rows == 0:
#         print(f"  No daily weather data returned from S3 for FIPS {fips_code} for years {start_year}-{end_year}.")
#         return None
    
#     print(f"  Successfully fetched {daily_arrow_table.num_rows} daily records from S3 for FIPS {fips_code}.")
#     df_daily_weather = pl.from_arrow(daily_arrow_table)

#     # Ensure 'date' column is datetime
#     if "date" in df_daily_weather.columns:
#         if df_daily_weather["date"].dtype != pl.Datetime:
#             try:
#                 df_daily_weather = df_daily_weather.with_columns(pl.col("date").cast(pl.Datetime))
#                 print("  Successfully cast 'date' column to Datetime.")
#             except Exception as e:
#                 print(f"  Error casting 'date' column to Datetime: {e}. Skipping this FIPS.")
#                 return None
#     else:
#         print("  Error: 'date' column missing from fetched weather data. Skipping this FIPS.")
#         return None
        
#     # Cast weather variable columns to Float64 and handle missing value placeholders
#     cast_and_clean_expressions = []
#     for col_name in config.TARGET_DAILY_WEATHER_VARIABLES:
#         if col_name in df_daily_weather.columns:
#             if df_daily_weather[col_name].dtype == pl.String:
#                 print(f"  Cleaning and casting column '{col_name}' from String to Float64.")
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
#                     .then(None) 
#                     .otherwise(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False))
#                     .alias(col_name)
#                 )
#             elif df_daily_weather[col_name].dtype == pl.Float64: # If already float, just handle missing placeholder
#                 print(f"  Column '{col_name}' is already Float64. Handling missing value placeholder.")
#                 cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER)
#                     .then(None)
#                     .otherwise(pl.col(col_name))
#                     .alias(col_name)
#                 )
#             elif df_daily_weather[col_name].dtype.is_numeric(): # Other numeric types
#                  print(f"  Column '{col_name}' is numeric ({df_daily_weather[col_name].dtype}). Casting to Float64 and handling missing value placeholder.")
#                  cast_and_clean_expressions.append(
#                     pl.when(pl.col(col_name).cast(pl.Float64, strict=False) == config.RAW_DATA_MISSING_VALUE_PLACEHOLDER) 
#                     .then(None)
#                     .otherwise(pl.col(col_name).cast(pl.Float64, strict=False)) 
#                     .alias(col_name)
#                 )
#             else: # Non-string, non-numeric
#                 print(f"  Warning: Column '{col_name}' is type {df_daily_weather[col_name].dtype} and will not be cast or cleaned.")
#         else:
#             print(f"  Warning: Expected weather variable column '{col_name}' not found in fetched data.")

#     if cast_and_clean_expressions:
#         try:
#             df_daily_weather = df_daily_weather.with_columns(cast_and_clean_expressions)
#             print("  Successfully applied cleaning and casting to weather variable columns.")
#         except Exception as e:
#             print(f"  Error applying casting/cleaning expressions: {e}. Skipping this FIPS.")
#             return None
            
#     # Select only essential columns: date and the target weather variables that are now numeric
#     final_columns_to_keep = ["date"]
#     for var_name in config.TARGET_DAILY_WEATHER_VARIABLES:
#         if var_name in df_daily_weather.columns and df_daily_weather[var_name].dtype == pl.Float64:
#             final_columns_to_keep.append(var_name)
#         elif var_name in df_daily_weather.columns: # Exists but not float (should not happen if cast worked)
#              print(f"  Warning: Column '{var_name}' exists but is not Float64 after processing ({df_daily_weather[var_name].dtype}). It might not be usable for calculations.")
#              # Optionally keep it anyway or drop it. For now, let's keep it if it was a target.
#              if var_name not in final_columns_to_keep : final_columns_to_keep.append(var_name)


#     # Ensure no duplicate columns if 'date' was somehow in TARGET_DAILY_WEATHER_VARIABLES
#     final_columns_to_keep = list(dict.fromkeys(final_columns_to_keep))
    
#     # Check if all expected final columns are actually in the dataframe before selecting
#     missing_final_cols = [col for col in final_columns_to_keep if col not in df_daily_weather.columns]
#     if missing_final_cols:
#         print(f"  Error: Columns {missing_final_cols} are missing from DataFrame before final select. Available: {df_daily_weather.columns}")
#         return None

#     df_daily_weather = df_daily_weather.select(final_columns_to_keep)
#     print(f"  Cleaned daily weather data for FIPS {fips_code} has shape: {df_daily_weather.shape}")
#     print(f"  Schema after cleaning: {df_daily_weather.schema}")
    
#     return df_daily_weather


# def main():
#     print("--- Starting Agricultural Weather Data Processing ---")
#     print(f"Using Growing Season: Month {config.GS_START_MONTH}/{config.GS_START_DAY} to {config.GS_END_MONTH}/{config.GS_END_DAY}")
#     print(f"Target daily weather variables to fetch: {config.TARGET_DAILY_WEATHER_VARIABLES}")
#     print(f"Weather data processing will start from year: {config.DATA_START_YEAR}")

#     # --- Chunk 1: Load and Prepare Input CSV ---
#     input_csv_path = "df_yield_weather_20_05_14h00m.csv" # Or use config.INPUT_YIELD_CSV
    
#     prepared_data = load_and_prepare_yield_data(input_csv_path)
#     if prepared_data is None:
#         print("Failed to load and prepare yield data. Exiting.")
#         return

#     df_yield, unique_fips_to_process, min_year_csv, max_year_csv = prepared_data
    
#     fetch_start_year = config.DATA_START_YEAR
#     fetch_end_year = max_year_csv
#     print(f"Weather data will be fetched for years: {fetch_start_year} - {fetch_end_year} for each FIPS.")

#     # --- Placeholder for testing Chunk 2 ---
#     # Let's test fetch_and_clean_daily_weather for the first FIPS code
#     if unique_fips_to_process:
#         first_fips = unique_fips_to_process[0]
#         print(f"\n--- TESTING CHUNK 2 for first FIPS: {first_fips} ---")
#         # Determine the actual max year for this specific FIPS from the yield data
#         # This is more precise if different FIPS have different year ranges in the CSV
#         try:
#             max_year_for_fips = df_yield.filter(pl.col("fips_full") == first_fips).select(pl.col("year").max()).item()
#             if max_year_for_fips < fetch_start_year:
#                  print(f"Max year for FIPS {first_fips} ({max_year_for_fips}) is before data start year ({fetch_start_year}). Skipping weather fetch test.")
#             else:
#                 current_fips_end_year = max(fetch_start_year, min(fetch_end_year, max_year_for_fips)) # Ensure we don't go beyond overall max or before data start
                
#                 cleaned_df = fetch_and_clean_daily_weather(first_fips, fetch_start_year, current_fips_end_year)
#                 if cleaned_df is not None:
#                     print(f"\nSuccessfully fetched and cleaned data for FIPS {first_fips}:")
#                     print(cleaned_df.head())
#                     print(f"Shape: {cleaned_df.shape}")
#                 else:
#                     print(f"\nFailed to fetch or clean data for FIPS {first_fips}.")
#         except Exception as e:
#             print(f"Error during test fetch for FIPS {first_fips}: {e}")
#     else:
#         print("No unique FIPS codes to test weather fetching.")


#     # --- Placeholder for subsequent chunks ---
#     # all_yearly_weather_data = [] # This will store results from Chunk 5

#     # TODO: Chunk 3: Define agronomic variable calculation functions
#     # TODO: Chunk 4: Define yearly aggregation function
#     # TODO: Chunk 5: Main processing loop (iterate unique_fips, fetch, aggregate)
#     # TODO: Chunk 6: Merge results and save

#     print("\n--- Agricultural Weather Data Processing Finished (Chunk 1 & 2 Defined, Chunk 2 Tested for one FIPS) ---")

# if __name__ == "__main__":
#     main()
    
    
    
# So we need to:
# - read the state and counties from the csv file
# - read the data from the nclimgrid using cvlimgrid_importer for these counties (all at once? or better to do it one by one?)
# - using tmax create the GDD and KDD for the growing months
# - using tmax create the GDD and KDD for the silk fill dates
# - create the averaged growing months maximum temperature, and the growing months total precipitation
# - create the averaged silk fill dates maximum temperature, and the silk fill dates total precipitation
# - create the compound hot dry days for the growing months
# - create the compound hot dry days for the silk fill dates
# add the data to the csv file (also save 2 columns indicating the growing months and silk fill dates)