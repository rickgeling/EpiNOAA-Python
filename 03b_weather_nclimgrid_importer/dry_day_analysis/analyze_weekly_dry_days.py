#analyze_weekly_dry_days.py
#
#side analysis, not part of the main pipeline flow: weekly dry-day counts within
#the growing season. Lives in 03b/dry_day_analysis/ (moved 2026-08-17); writes to
#03b/created_dfs_weekly_dry_day_analysis/, which stays at the stage-folder root.

#--- Standard Library Imports ---
import os
import re
import time
from typing import List, Dict, Optional, Any, Tuple
from collections import defaultdict

#--- Third-party Library Imports ---
import polars as pl
import numpy as np

#--- Path setup ---
#this script sits one level below the stage folder, so it needs two entries on
#sys.path: the stage folder (for get_data_importer_precip_extremes) and the repo
#root (for config). Both are resolved from __file__ rather than the current
#working directory, so the script runs correctly from anywhere.
import sys
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_STAGE_DIR = os.path.dirname(_SCRIPT_DIR)
_REPO_ROOT = os.path.dirname(_STAGE_DIR)
for _p in (_STAGE_DIR, _REPO_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

#--- Custom Module Imports ---
#reuse functions from the precip-extremes importer (renamed from
#get_data_importer_paper on 2026-08-17)
from get_data_importer_precip_extremes import load_and_prepare_yield_data, fetch_and_clean_daily_weather_batched
import config #ensure config.py has DRY_DAY_THRESHOLDS_MM = [1.0, 2.0]

#--- Constants ---
DRY_THRESHOLDS = config.DRY_DAY_THRESHOLDS_MM #use thresholds from config [1.0, 2.0]

#which crop to run this analysis for
crop = "corn"  # "corn" or "soy"

#both filenames are derived from `crop`, so they can't drift apart
input_yield_filename = f"df_yield_climdiv_{crop}_paper.csv"
output_csv_filename = f"df_weekly_dry_days_{crop}.csv"

def process_batch_for_weekly_dry_days(df_daily_batch: pl.DataFrame) -> Optional[pl.DataFrame]:
    """
    Processes a batch of daily weather data to calculate the count of dry days
    per week within the Growing Season (GS) for specified thresholds.
    """
    if df_daily_batch is None or df_daily_batch.height == 0:
        return None

    print(f"    Processing batch for weekly dry days. Input shape: {df_daily_batch.shape}")

    #ensure date column is datetime
    if df_daily_batch.select(pl.col("date")).dtypes[0] != pl.Datetime:
         df_daily_batch = df_daily_batch.with_columns(pl.col("date").cast(pl.Datetime))

    #1. Filter for Growing Season & Calculate Day/Week within GS
    gs_start_month = config.GS_START_MONTH
    gs_start_day = config.GS_START_DAY
    gs_end_month = config.GS_END_MONTH
    gs_end_day = config.GS_END_DAY

    df_gs = df_daily_batch.with_columns(
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.day().alias("day"),
        #--- FIX: Create year_daily column here ---
        pl.col("date").dt.year().alias("year_daily")
    ).filter(
        (pl.col("month") > gs_start_month) | ((pl.col("month") == gs_start_month) & (pl.col("day") >= gs_start_day))
    ).filter(
        (pl.col("month") < gs_end_month) | ((pl.col("month") == gs_end_month) & (pl.col("day") <= gs_end_day))
    ).with_columns(
        #calculate day number within GS (April 1st = day 1)
        day_in_gs = (pl.col("date") - pl.min_horizontal(pl.col("date").dt.year().cast(pl.String) + f"-{gs_start_month:02d}-{gs_start_day:02d}").str.to_datetime("%Y-%m-%d")).dt.total_days() + 1,
    ).with_columns(
        #calculate week number within GS (days 1-7 = week 1, etc.)
        week_in_gs = ((pl.col("day_in_gs") - 1) / 7).floor() + 1
    #--- Now the select statement will work ---
    ).select(["fips", "date", "year_daily", "week_in_gs", "prcp"])

    if df_gs.height == 0:
        print("    No GS data found in this batch after filtering.")
        return None

    #2. Calculate Dry Day Flags for each threshold
    dry_flag_exprs = []
    for thresh in DRY_THRESHOLDS:
        col_name = f"is_dry_{int(thresh)}mm"
        dry_flag_exprs.append(
            (pl.col("prcp") < thresh).cast(pl.UInt8).alias(col_name)
        )
    df_gs = df_gs.with_columns(dry_flag_exprs)

    #3. Aggregate dry day counts per week
    agg_exprs = []
    value_cols = [] #keep track of columns to pivot
    for thresh in DRY_THRESHOLDS:
        flag_col = f"is_dry_{int(thresh)}mm"
        agg_col_name = f"CDD_{int(thresh)}mm_count"
        agg_exprs.append(pl.sum(flag_col).alias(agg_col_name))
        value_cols.append(agg_col_name)

    df_weekly_counts = df_gs.group_by(["fips", "year_daily", "week_in_gs"]).agg(agg_exprs).sort(["fips", "year_daily", "week_in_gs"])

    #4. Pivot to get weeks as columns
    df_pivoted = df_weekly_counts.pivot(
        index=["fips", "year_daily"],
        columns="week_in_gs",
        values=value_cols
    ).sort(["fips", "year_daily"])

    #Rename columns for clarity (e.g., CDD_1mm_count_week_in_gs_1.0 -> CDD_1mm_W1)
    #
    #Polars names multi-value pivot outputs "{value}_{pivot_key}_{key_value}", so
    #parts[1] below is the whole suffix "week_in_gs_1.0" — not the bare week
    #number the original code assumed. Taking parts[1] verbatim produced
    # "CDD_1mm_Wweek_in_gs_1.0" (the names in the pre-2026-08-17 output CSV).
    #purely a naming defect: the counts themselvs are computed before the pivot
    #and were never affected. Now the trailing number is extracted and cast to
    #int, dropping the ".0" that week_in_gs carries because it comes from
    #.floor() and is therefore a float.
    new_column_names = {}
    for col in df_pivoted.columns:
        if col.startswith("CDD_") and col.endswith("_count"): #handle potential old name format
             parts = col.split("_")
             if len(parts) == 4 and parts[2] == "count":
                 new_name = f"{parts[0]}_{parts[1]}_W{parts[3]}"
                 new_column_names[col] = new_name
        elif "_count_" in col: #handle expected pivoted names
            parts = col.split("_count_")
            if len(parts) == 2:
                 base_name = parts[0]
                 week_match = re.search(r"(\d+)(?:\.\d+)?$", parts[1])
                 if week_match:
                     new_column_names[col] = f"{base_name}_W{int(week_match.group(1))}"

    df_pivoted = df_pivoted.rename(new_column_names)
    print(f"    Finished processing batch. Output shape: {df_pivoted.shape}")
    return df_pivoted

def main():
    overall_start_time = time.time()
    print("--- Starting Weekly Dry Day Analysis ---")
    print(f"Dry day thresholds being used: {DRY_THRESHOLDS} mm")

    #--- Path Setup ---
    #all paths resolved from this file's location (see _SCRIPT_DIR/_STAGE_DIR/
    #_REPO_ROOT at the top), never from the current working directory.
    input_yield_path = os.path.join(_REPO_ROOT, "02_weather_climdiv_crosswalk", "created_dfs_step2", input_yield_filename)

    #output bundle stays at the stage-folder root, one level up from this script
    output_csv_path = os.path.join(_STAGE_DIR, "created_dfs_weekly_dry_day_analysis", output_csv_filename)

    print(f"  crop  : {crop}")
    print(f"  input : {input_yield_path}")
    print(f"  output: {output_csv_path}")

    #--- Load FIPS/Year Info (reuses the importer's function) ---
    prepared_data = load_and_prepare_yield_data(input_yield_path)
    if prepared_data is None:
        print("Failed to load yield data to get FIPS/Years. Exiting.")
        return
    df_yield, unique_fips_to_process, min_year_csv, max_year_csv = prepared_data

    #same year range as the main importer
    overall_fetch_start_year = config.DATA_START_YEAR
    overall_fetch_end_year = max_year_csv
    print(f"Daily weather data will be fetched for years: {overall_fetch_start_year} - {overall_fetch_end_year}.")

    #--- Process Data in Batches (same batching as the importer) ---
    all_weekly_results: List[pl.DataFrame] = []
    all_s3_load_errors_summary: Dict[str, List[Dict[str,str]]] = {}

    fips_by_state = defaultdict(list)
    for fips in unique_fips_to_process:
        state_fips_prefix = fips[:2]
        fips_by_state[state_fips_prefix].append(fips)

    num_states = len(fips_by_state)
    print(f"\n--- Starting Main Processing Loop for {num_states} states ---")

    for state_idx, (state_fips_prefix, fips_in_state_list) in enumerate(fips_by_state.items()):
        state_start_time = time.time()
        print(f"\nProcessing State Group {state_idx+1}/{num_states} (State FIPS Prefix: {state_fips_prefix}, {len(fips_in_state_list)} counties)")

        #determine year range needed for this batch
        max_year_for_this_state_batch = df_yield.filter(
            pl.col("fips_full").is_in(fips_in_state_list)
        ).select(pl.col("year").max()).item()
        current_state_fetch_start_year = overall_fetch_start_year
        current_state_fetch_end_year = max(overall_fetch_start_year -1, min(overall_fetch_end_year, max_year_for_this_state_batch))

        if current_state_fetch_end_year < current_state_fetch_start_year:
             print(f"  Max relevant year for state {state_fips_prefix} is before start year. Skipping.")
             continue

        #fetch daily data
        cleaned_df_for_state_batch, errors_for_this_state_load = fetch_and_clean_daily_weather_batched(
            fips_in_state_list, current_state_fetch_start_year, current_state_fetch_end_year
        )

        #log errors if any
        if errors_for_this_state_load:
             #(could add the importer's detailed error logging here)
             print(f"  Encountered {len(errors_for_this_state_load)} S3 loading errors for this batch.")

        #process the fetched batch for weekly dry days
        weekly_results_batch = process_batch_for_weekly_dry_days(cleaned_df_for_state_batch)

        if weekly_results_batch is not None and weekly_results_batch.height > 0:
            all_weekly_results.append(weekly_results_batch)
        else:
             print(f"  No weekly dry day results generated for state batch {state_fips_prefix}.")

        state_end_time = time.time()
        print(f"  Finished processing state batch {state_fips_prefix} in {state_end_time - state_start_time:.2f} seconds.")

    #--- Combine and Save Results ---
    if not all_weekly_results:
        print("\nNo weekly dry day results were generated across all batches. Cannot save.")
    else:
        print("\n--- Combining results from all batches ---")
        df_final_weekly = pl.concat(all_weekly_results, how="vertical")

        #rename fips column to fips_full for consistency if needed
        if "fips" in df_final_weekly.columns and "fips_full" not in df_final_weekly.columns:
            df_final_weekly = df_final_weekly.rename({"fips": "fips_full"})
        if "year_daily" in df_final_weekly.columns and "year" not in df_final_weekly.columns:
             df_final_weekly = df_final_weekly.rename({"year_daily": "year"})

        print(f"\nFinal combined DataFrame shape: {df_final_weekly.shape}")
        print("Sample of final data (first 5 rows):")
        print(df_final_weekly.head(5))

        print(f"\nSaving weekly dry day data to: {output_csv_path}")
        try:
            #output_csv_path is already absolute (built from _STAGE_DIR above).
            #the previous version did:
            #   script_dir = os.path.dirname(__file__) if '__file__' in locals() else os.getcwd()
            #which always took the os.getcwd() branch — inside a function __file__
            #is a module global and is never present in locals() — so the file
            #landed relative to wherever the script happened to be launched from.
            os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
            df_final_weekly.write_csv(output_csv_path)
            print(f"Successfully saved data to {output_csv_path}")
        except Exception as e:
            print(f"Error saving data to CSV: {e}")

    overall_end_time = time.time()
    print(f"\n--- Weekly Dry Day Analysis Finished in {overall_end_time - overall_start_time:.2f} seconds ---")

if __name__ == "__main__":
    main()