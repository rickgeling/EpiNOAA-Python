

#########################################

import os
import pandas as pd

# ─── USER‐CONFIGURABLE SECTION ────────────────────────────────────────────────
# Change this to the folder that contains year‐subfolders (e.g. 1951/, 1952/, …)
CLIMATE_ROOT = "." ### TODO: CHANGE!

# Path to your existing yield+climdiv CSV:
path_df = "../../_noaa_climdiv_local/"
input_csv_filename = "df_yield_climdiv.csv" 
YIELD_CSV = os.path.join(path_df, input_csv_filename)

# Name of the output file:
OUTPUT_CSV  = "df_compare_months_local_v2.csv"
# ────────────────────────────────────────────────────────────────────────────────

def build_county_fips(df_yield: pd.DataFrame) -> pd.DataFrame:
    """
    Take a DataFrame with integer columns 'state' and 'county',
    and add a new column 'fips' = state*1000 + county.
    E.g., state=01, county=001 → fips=1001; state=17, county=001 → fips=17001.
    """
    df = df_yield.copy()
    df["state"]  = df["state"].astype(int)
    df["county"] = df["county"].astype(int)
    df["fips"]   = df["state"] * 1000 + df["county"]
    return df


def collect_monthly_climate_stats(
    years:    pd.Series,
    all_fips: pd.Series,
    root:     str
) -> pd.DataFrame:
    """
    For each year ≥1951 and month 01–12 where a 'YYYYMM.parquet' exists:
      1) Read ["fips","date","tmax","prcp","region_type"] from that Parquet.
      2) Convert 'fips' from zero‐padded string → int.
      3) Convert 'tmax'/'prcp' from object/string → float (numeric).
      4) Filter to rows whose fips is in all_fips (your yield‐CSV counties).
      5) Group by fips → compute mean(tmax) & sum(prcp) for that (year,month).
      6) Collect each small grouped DataFrame (with columns year, fips, tmax_mMM, prcp_mMM)
         into a list monthly_dfs.

    After scanning all files, build a single "wide" DataFrame climate_monthly by:
      a) Concatenating all (year,fips) pairs to get the complete set of keys.
      b) Creating a DataFrame indexed by (year,fips) with columns
         ["tmax_m01","tmax_m02",…,"tmax_m12","prcp_m01",…,"prcp_m12"], all NaN initially.
      c) “Updating” it with each month’s small block so that the correct cells get filled.
    This avoids any MergeError from duplicate column names.

    Returns:
      DataFrame with columns [year, fips, tmax_m01, …, tmax_m12, prcp_m01, …, prcp_m12].
      If no county ever matched, returns an empty but well‐shaped DataFrame.
    """
    monthly_dfs = []
    # Only consider years ≥ 1951
    unique_years = sorted(int(y) for y in years.unique() if int(y) >= 1951)
    fips_set     = set(all_fips.unique())

    # Debug counters
    file_counter     = 0
    matched_any_file = 0
    seen_first_file  = False

    # 1) Build a list of (year,month) pairs for which a Parquet file actually exists
    year_month_files = []
    for year in unique_years:
        year_folder = os.path.join(root, str(year))
        if not os.path.isdir(year_folder):
            # If the subfolder is missing, skip it silently here (a warning prints below).
            continue
        for month in range(1, 13):
            mm = f"{month:02d}"
            parquet_fname = os.path.join(year_folder, f"{year}{mm}.parquet")
            if os.path.isfile(parquet_fname):
                year_month_files.append((year, month))

    total_files = len(year_month_files)
    print(f"\n→ Found {total_files} monthly Parquet file(s) under '{root}' (years ≥1951).")
    if total_files > 10:
        print(f"  (First 10: {year_month_files[:10]})\n")
    else:
        print(f"  {year_month_files}\n")

    # 2) Loop over each (year, month) that actually exists
    for idx, (year, month) in enumerate(year_month_files, start=1):
        year_str     = str(year)
        mm           = f"{month:02d}"
        parquet_path = os.path.join(root, year_str, f"{year_str}{mm}.parquet")

        print(f"[{idx:03d}/{total_files:03d}] → Reading '{parquet_path}' …", end=" ")
        try:
            df_m = pd.read_parquet(
                parquet_path,
                columns=["fips", "date", "tmax", "prcp", "region_type"]
            )
        except Exception as e:
            print(f"\n    ✖ FAILED to read '{parquet_path}': {e!s}\n")
            continue

        file_counter += 1
        n_rows = len(df_m)
        print(f"\n    • Loaded {n_rows} total rows from this Parquet.")

        # On the very FIRST successfully‐loaded file, print dtypes + a few samples:
        if not seen_first_file:
            seen_first_file = True
            print("    ├─ [DEBUG] Columns & dtypes:")
            for col, dt in df_m.dtypes.items():
                print(f"    │     {col}: {dt}")
            print("    ├─ [DEBUG] First 5 rows (all columns):")
            sample_str = df_m.head(5).to_string().replace("\n", "\n    │    ")
            print(f"    │    {sample_str}")
            sample_fips = df_m["fips"].dropna().unique()[:10].tolist()
            print(f"    ├─ [DEBUG] Sample distinct fips (strings) in this file: {sample_fips}")
            print(f"    ├─ [DEBUG] Unique region_type values: {df_m['region_type'].dropna().unique().tolist()}\n")

        # ─── Convert fips: zero‐padded string → int ─────────────────────────────────
        try:
            df_m["fips"] = df_m["fips"].astype(int)
        except Exception as e:
            print(f"    ✖ ERROR converting df_m['fips'] to int: {e!s}")
            print("    → The Parquet's fips format is not a simple zero‐padded number. "
                  "Inspect its format and adjust.\n")
            continue

        # ─── Convert tmax & prcp from object/string → numeric (float) ─────────────────
        df_m["tmax"] = pd.to_numeric(df_m["tmax"], errors="coerce")
        df_m["prcp"] = pd.to_numeric(df_m["prcp"], errors="coerce")

        # ─── Filter to only those rows whose fips is in our yield‐CSV fips set ────────
        df_c = df_m[df_m["fips"].isin(fips_set)].copy()
        matched_rows = len(df_c)
        print(f"    • Rows matching our FIPS set (after converting to int): {matched_rows}")

        if matched_rows == 0:
            print("    → No matching county‐rows, skipping.\n")
            continue

        matched_any_file += 1
        print("    → Found matching row(s) for our counties! Proceeding…")

        # ─── Ensure date is datetime, then restrict to exactly this (year,month) ─────
        if not pd.api.types.is_datetime64_any_dtype(df_c["date"]):
            df_c["date"] = pd.to_datetime(df_c["date"], errors="coerce")

        df_c = df_c[
            (df_c["date"].dt.year  == year) &
            (df_c["date"].dt.month == month)
        ]
        if df_c.empty:
            print("    → After filtering on date==(year,month), zero rows remain. Skipping.\n")
            continue

        # ─── Group by fips: compute mean(tmax) & sum(prcp) for that month ────────────
        grouped = (
            df_c
            .groupby("fips", as_index=True)
            .agg({
                "tmax": "mean",
                "prcp": "sum"
            })
            .rename(columns={
                "tmax": f"tmax_m{mm}",
                "prcp": f"prcp_m{mm}"
            })
        ).reset_index()
        grouped["year"] = year
        grouped = grouped[["year", "fips", f"tmax_m{mm}", f"prcp_m{mm}"]]

        # Print a small sample
        print(f"    ├─ [DEBUG] Sample of grouped (fips, tmax_m{mm}, prcp_m{mm}):")
        snippet = grouped.head(3).to_string().replace("\n", "\n    │    ")
        print(f"    │    {snippet}\n")

        monthly_dfs.append(grouped)

    # ─── Final summary of file scanning ───────────────────────────────────────────
    print(f"\n=== SUMMARY OF SCANNING PARQUET FILES ===")
    print(f"Total Parquet files read successfully: {file_counter}/{total_files}")
    print(f"Number of files that had ≥1 matching FIPS‐row: {matched_any_file}")

    # If no monthly block ever matched, return an “empty but well‐shaped” DataFrame
    if not monthly_dfs:
        print(
            "⚠️  After scanning every available file, "
            "we never found any data matching your county‐FIPS codes.\n"
            "   → Returning an empty climate_monthly DataFrame."
        )
        cols = ["year", "fips"] + \
               [f"tmax_m{m:02d}" for m in range(1, 13)] + \
               [f"prcp_m{m:02d}" for m in range(1, 13)]
        return pd.DataFrame(columns=cols)

    # ─── Build a single wide DataFrame from all per‐month blocks ─────────────────
    print("\nMerging all months into one wide DataFrame …")

    # (a) Concatenate all (year,fips) pairs to get every unique key
    key_dfs = [dfm[["year", "fips"]] for dfm in monthly_dfs]
    keys    = pd.concat(key_dfs, ignore_index=True).drop_duplicates().reset_index(drop=True)

    # (b) Set them as a MultiIndex
    full = keys.set_index(["year", "fips"])

    # (c) Create all 24 columns, initialized to NaN
    month_cols = [f"tmax_m{m:02d}" for m in range(1, 13)] + [f"prcp_m{m:02d}" for m in range(1, 13)]
    for col in month_cols:
        full[col] = pd.NA

    # (d) For each month‐block DataFrame, update the 'full' DataFrame
    for dfm in monthly_dfs:
        dfm_idx = dfm.set_index(["year", "fips"])
        # This will fill in only the matching (year,fips) cells for tmax_mMM and prcp_mMM
        full.update(dfm_idx)

    # (e) Reset index so that 'year' and 'fips' become columns again
    climate_monthly = full.reset_index()

    print(f"\n→ climate_monthly final shape: {climate_monthly.shape}")
    print("→ A quick peek at the first few rows of climate_monthly:")
    peek = climate_monthly.head(5).to_string().replace("\n", "\n    ")
    print(f"    {peek}\n")

    return climate_monthly


def main():
    # ── DEBUG: Show current folder & subdirectories ─────────────────────────────
    cwd = os.getcwd()
    print(f"🔍 Current working directory is: {cwd!r}")
    subdirs = sorted([d for d in os.listdir(cwd) if os.path.isdir(d)])
    print(f"🔍 Subdirectories in this folder (should include '1951', etc.):\n   {subdirs}\n")
    print("──────────────────────────────────────────────────────────────────────────")

    # 1) Load df_yield_climdiv.csv
    print("1) Reading df_yield_climdiv.csv …")
    try:
        yield_df = pd.read_csv(YIELD_CSV)
    except Exception as e:
        print(f"✖ FAILED to read '{YIELD_CSV}': {e!s}")
        return
    print(f"   ✓ Loaded yield DataFrame with shape {yield_df.shape}\n")

    # 2) Build county‐FIPS column
    yield_df = build_county_fips(yield_df)
    print("2) Built county FIPS column. Sample rows:")
    sample_yield = yield_df[["year", "state", "county", "fips"]].head(5).to_string().replace("\n", "\n   ")
    print(f"   {sample_yield}")
    unique_fips = sorted(yield_df["fips"].unique())
    print(f"   → You have {len(unique_fips)} unique county-FIPS in your yield data.")
    print(f"   → A few sample FIPS: {unique_fips[:10]}\n")

    # 3) Extract the unique list of (year) & (fips)
    all_years = yield_df["year"]
    all_fips  = yield_df["fips"]

    # 4) Gather monthly climate stats (TMAX mean & PRCP sum) for years ≥1951
    print("3) Aggregating monthly climate stats …\n")
    climate_monthly = collect_monthly_climate_stats(
        years=all_years,
        all_fips=all_fips,
        root=CLIMATE_ROOT
    )

    # 5) Merge those 24 monthly columns back into yield_df on (year, fips)
    print("4) Merging climate_monthly into yield DataFrame …")
    yield_df["year"] = yield_df["year"].astype(int)
    yield_df["fips"] = yield_df["fips"].astype(int)

    merged = pd.merge(
        yield_df,
        climate_monthly,
        on=["year", "fips"],
        how="left",
        sort=False
    )
    print(f"   ✓ After merge, merged DataFrame shape: {merged.shape}")
    print("   A quick peek at the merged DataFrame’s first few rows:")
    peek_merged = merged.head(5).to_string().replace("\n", "\n   ")
    print(f"   {peek_merged}\n")

    # 6) Save out the final CSV
    print("5) Saving out the final DataFrame as CSV …")
    merged.to_csv(OUTPUT_CSV, index=False)
    abs_path = os.path.abspath(OUTPUT_CSV)
    print(f"   ✓ Saved final DataFrame to: {abs_path!r}\n")

    # 7) Reload the first few lines to ensure it isn’t blank
    print("6) Reloading first few lines of the newly‐saved CSV (for sanity check):")
    try:
        check_df = pd.read_csv(OUTPUT_CSV, nrows=5)
        peek_check = check_df.to_string().replace("\n", "\n   ")
        print(f"   {peek_check}\n")
    except Exception as e:
        print(f"   ✖ FAILED to reload '{OUTPUT_CSV}': {e!s}\n")


if __name__ == "__main__":
    main()