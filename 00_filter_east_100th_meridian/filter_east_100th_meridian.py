#filter_east_100th_meridian.py
#
#Robustness-check branch, parallel to the main 5-state pipeline (01a-04):
#takes the all-US-states/counties raw USDA yield export in
#extracted_usda_all_states_<crop>_data/ (split into several alphabetical
#batch files - USDA Quick Stats caps export size, so one all-US pull for a
#crop needs multiple downloads), filters it down to counties east of the
#100th meridian (using county-centroid longitude from the Census Gazetteer
#file in this same folder), and writes ONE combined filtered CSV straight
#into 01a's/01b's own extracted_usda_<crop>_data/ folder, so it's picked up
#and processed by the existing, unmodified 01a/01b format notebooks (just
#point their `file_name` config at the new file, with a distinct
#`save_name`, so it doesn't collide with the main 5-state output).
#
#Gazetteer file (2025_Gaz_counties_national.txt) source:
#https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html

import os
import glob
import re
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.dirname(script_dir)

#--- Configuration: which crop to run this for ---
crop = "corn"  # "corn" or "soy", must match whichever pair is uncommented below

#uncomment the pair that matches `crop` above, comment out the other
input_dir = os.path.join(script_dir, "extracted_usda_all_states_corn_data")          #os.path.join(script_dir, "extracted_usda_all_states_soy_data")
target_extracted_dir = os.path.join(repo_root, "01a_yield_data_corn", "extracted_usda_corn_data")   #os.path.join(repo_root, "01b_yield_data_soy", "extracted_usda_soy_data")

#safety check: make sure crop and the two config values above all agree,
#same pattern used in 02's crop toggle, catches a half-finished swap
#immediately instead of silently filtering into the wrong crop's folder
other_crop = "corn" if crop == "soy" else "soy"
for label, value in [("input_dir", input_dir), ("target_extracted_dir", target_extracted_dir)]:
    if other_crop in value.lower():
        raise ValueError(f"crop = '{crop}' but {label} = '{value}' contains '{other_crop}', looks like only some of the corn/soy lines got toggled.")
    if crop not in value.lower():
        raise ValueError(f"crop = '{crop}' but {label} = '{value}' does not contain '{crop}', looks like only some of the corn/soy lines got toggled.")

print(f"Crop selection OK: crop='{crop}'")
print(f"  input_dir            = {input_dir}")
print(f"  target_extracted_dir = {target_extracted_dir}")

#-----------------------------------------------------------------
#1) Load the Gazetteer file and build the "east of 100th meridian" FIPS keep-list
#-----------------------------------------------------------------
#the file is PIPE-delimited (not tab-delimited, despite the .txt extension)
gaz_path = os.path.join(script_dir, "2025_Gaz_counties_national.txt")
gaz = pd.read_csv(gaz_path, sep="|", dtype={"GEOID": str})
gaz.columns = gaz.columns.str.strip()  #defensive: some Gazetteer vintages have trailing whitespace in headers
gaz["INTPTLONG"] = gaz["INTPTLONG"].astype(float)

#100th meridian west = -100 degrees longitude; east of it = greater than -100
east_fips = set(gaz.loc[gaz["INTPTLONG"] > -100, "GEOID"])
print(f"\n{len(east_fips)} of {len(gaz)} counties nationwide are east of the 100th meridian")

#-----------------------------------------------------------------
#2) Load and concatenate all raw USDA batch files for this crop
#-----------------------------------------------------------------
batch_paths = sorted(glob.glob(os.path.join(input_dir, "*.csv")))
if not batch_paths:
    raise FileNotFoundError(f"No CSV batch files found in {input_dir}")

print(f"\nFound {len(batch_paths)} batch file(s):")
batch_dfs = []
seen_states = set()
for p in batch_paths:
    d = pd.read_csv(p, low_memory=False)
    these_states = set(d["State"].unique())
    print(f"  - {os.path.basename(p)}: {len(d)} rows, states: {sorted(these_states)}")

    #sanity check: no state should appear in more than one batch (would mean
    #overlapping USDA queries, i.e. potential double-counted rows)
    overlap = seen_states & these_states
    if overlap:
        raise ValueError(f"States {overlap} appear in more than one batch file "
                          f"(found again in {os.path.basename(p)}), check for overlapping downloads.")
    seen_states |= these_states
    batch_dfs.append(d)

df = pd.concat(batch_dfs, ignore_index=True)
print(f"\nCombined: {len(df)} rows across {df['State'].nunique()} states")

#-----------------------------------------------------------------
#3) Build a 5-digit FIPS column to match against the Gazetteer's GEOID
#-----------------------------------------------------------------
#drop rows with no county (state-level aggregate rows in the USDA export)
#before building FIPS, same as 01a/01b's own cleanup step
df = df.dropna(subset=["County ANSI"])
df["FIPS"] = (
    df["State ANSI"].astype(int).astype(str).str.zfill(2)
    + df["County ANSI"].astype(int).astype(str).str.zfill(3)
)

#-----------------------------------------------------------------
#4) Filter to counties east of the 100th meridian
#-----------------------------------------------------------------
rows_before = len(df)
df_east = df[df["FIPS"].isin(east_fips)].drop(columns=["FIPS"])
counties_kept = df_east.groupby(["State ANSI", "County ANSI"]).ngroups
print(f"\nKept {len(df_east)} of {rows_before} rows "
      f"({df_east['State ANSI'].nunique()} states, {counties_kept} counties)")

#-----------------------------------------------------------------
#5) Save into 01a's/01b's own extracted_usda_<crop>_data/ folder
#-----------------------------------------------------------------
#output filename reuses the batch files' own date stamp (extraction date,
#shared across all of them) so it's traceable back to when the data was pulled
date_match = re.match(r"(\d{6})_", os.path.basename(batch_paths[0]))
date_prefix = date_match.group(1) if date_match else "unknown_date"
output_filename = f"{date_prefix}_{crop}_yield_east100m.csv"

output_path = os.path.join(target_extracted_dir, output_filename)
df_east.to_csv(output_path, index=False)
print(f"\nSaved: {output_path}")
