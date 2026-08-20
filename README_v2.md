# Pipeline overview (v2): yield data → weather crosswalk

This document explains how the pipeline works from raw USDA yield data
(`01a`/`01b`) through the climdiv weather merge (`02`). It does not yet cover
`03a`/`03b` (nclimgrid weather) or `04` (comparison/validation) — see
`x_logbook/CHANGELOG.md` for the full history of decisions behind every
rename and fix mentioned here.

## Folder naming convention

- Numeric prefix = pipeline stage, in run order.
- `a`/`b` suffix = parallel siblings at the same stage (same step, different
  crop, or different implementation) — not a separate stage.
- `extracted_<source>_<data>_data/` = raw, unmodified input files.
- `created_dfs_stepN/` = this stage's output, `N` matching the stage prefix.
- `created_dfs_step_final/` = the pipeline's true final output (used from
  stage `03` onward, not relevant yet at `01`/`02`).

```
01a_yield_data_corn/
  extracted_usda_corn_data/      raw USDA NASS exports (corn)
  created_dfs_step1/             output of format_corn_yield_data_paper.ipynb
  format_corn_yield_data_paper.ipynb

01b_yield_data_soy/
  extracted_usda_soy_data/       raw USDA NASS exports (soy)
  created_dfs_step1/             output of format_soy_yield_data_paper.ipynb
  format_soy_yield_data_paper.ipynb

02_weather_climdiv_crosswalk/
  extracted_noaa_climdiv_data/   raw NOAA climdiv monthly text files + county-to-climdiv crosswalk
  created_dfs_step2/             output of merge_yield_monthly_weather.ipynb
  webscraper.py                  downloads the raw climdiv files
  merge_yield_monthly_weather.ipynb
```

## Stage 1: format yield data (`01a_yield_data_corn`, `01b_yield_data_soy`)

Each notebook (`format_corn_yield_data_paper.ipynb` /
`format_soy_yield_data_paper.ipynb`) takes one raw USDA NASS Quick Stats
county-level yield export and turns it into a clean, per-county-year yield
dataframe.

**Config cell** (right after "Load Libraries and packages"):

```python
file_name = "082026_corn_yield_all_states.csv"  # file to get from extracted_usda_corn_data/
save_name = "df_corn_yield_2026.csv"            # name to save new df under in created_dfs_step1
```

(soy's config cell is the same shape, pointed at `extracted_usda_soy_data/`
and `082026_soy_yield_all_states.csv`)

**What the notebook does:**

1. Reads `extracted_usda_<crop>_data/<file_name>`.
2. Filters to county-level rows, builds a 5-digit FIPS-style county code from
   State ANSI + County ANSI.
3. Computes `highest_min_year` / `lowest_max_year` — the actual overlapping
   year range present in the data — and uses those (not hardcoded years) to
   check for and report counties with missing county-years.
4. Saves the result to `created_dfs_step1/<save_name>`.

**Current active files:** corn reads `082026_corn_yield_all_states.csv` →
writes `df_corn_yield_2026.csv`; soy reads `082026_soy_yield_all_states.csv`
→ writes `df_soy_yield_2026.csv`. Both extracts include the 2025 season.
Older files (`df_corn_yield.csv`, `df_corn_yield_2025.csv`,
`df_soy_yield.csv`) are leftovers from earlier runs, kept in place but not
used by anything downstream.

**To run for a new yield export:** drop the new raw CSV into
`extracted_usda_<crop>_data/`, update `file_name`/`save_name` in the config
cell, run all cells.

## Stage 2: merge yield with climdiv weather (`02_weather_climdiv_crosswalk`)

`merge_yield_monthly_weather.ipynb` takes one crop's `created_dfs_step1`
output, attaches monthly climdiv weather (max temperature + precipitation)
via the county → climdiv-division crosswalk, and writes a merged yield +
weather dataframe.

### Getting fresh weather data first

`webscraper.py` downloads the two current climdiv monthly files (tmax,
precipitation) into `extracted_noaa_climdiv_data/`. Before running the merge
notebook, make sure these point at NOAA's current file versions — check the
NOAA climdiv directory listing for the latest `-v1.0.0-YYYYMMDD` filenames,
since a stale file can contain `-99.90`/`-9.99` missing-value placeholders
for recent months instead of real data (this happened with the `20250506`
files, which were missing all of May–Dec 2025).

```python
urls = [
    "https://www.ncei.noaa.gov/monitoring-content/data/us/climdiv/monthly/current/climdiv-tmaxcy-v1.0.0-20260806",
    "https://www.ncei.noaa.gov/monitoring-content/data/us/climdiv/monthly/current/climdiv-pcpncy-v1.0.0-20260806",
]
```

Run `webscraper.py` (inside the poetry env — it needs `requests`) whenever
newer files are published. It saves each file into
`extracted_noaa_climdiv_data/` with a `.txt` extension appended.

### Config cell (right after "Load Libraries and packages")

```python
# Which crop to run this notebook for
crop = "soy"  # "corn" or "soy" — must match whichever pair is uncommented below

# Uncomment the pair that matches `crop` above, comment out the other
path_df_step1 = "../01b_yield_data_soy/created_dfs_step1/"       #"../01a_yield_data_corn/created_dfs_step1/"
filename = "df_soy_yield_2026.csv"                                  #"df_corn_yield_2026.csv"
save_file = "created_dfs_step2/df_yield_climdiv_soy_paper.csv"      #"created_dfs_step2/df_yield_climdiv_corn_paper.csv"

# Safety check: makes sure crop, path_df_step1, filename, and save_file all agree
other_crop = "corn" if crop == "soy" else "soy"
for label, value in [("path_df_step1", path_df_step1), ("filename", filename), ("save_file", save_file)]:
    if other_crop in value.lower():
        raise ValueError(f"crop = '{crop}' but {label} = '{value}' contains '{other_crop}' — looks like only some of the corn/soy lines got toggled.")
    if crop not in value.lower():
        raise ValueError(f"crop = '{crop}' but {label} = '{value}' does not contain '{crop}' — looks like only some of the corn/soy lines got toggled.")
```

To switch crops: change `crop`, and swap which of the three commented pairs
is active so all three lines agree with `crop`. The safety check exists
specifically to catch a half-finished swap (e.g. `crop` changed to `"corn"`
but `filename` still says soy) — it raises immediately instead of silently
producing a mislabeled output file.

**What the notebook does:**

1. Loads `path_df_step1/filename` (the stage-1 yield output for the selected
   crop).
2. Loads the two raw climdiv text files from `extracted_noaa_climdiv_data/`
   and the `county-to-climdivs.txt` crosswalk.
3. Checks both weather dataframes for missing values: converts the `Jan`–
   `Dec` columns to numeric, then flags `-99.90` (tmax) and `-9.99` (pcpn) as
   missing, per month.
4. Maps each county to its climate division, joins in the corresponding
   monthly tmax/precipitation values.
5. Saves the merged result to `save_file` (under `created_dfs_step2/`).

**Outputs so far:** `df_yield_climdiv.csv`, `df_yield_climdiv_2025.csv`,
`df_yield_climdiv_soy.csv` are from earlier runs. `df_yield_climdiv_soy_paper.csv`
and `df_yield_climdiv_corn_paper.csv` are the current targets — the notebook
needs to be run once per crop (flip `crop` and the commented pair between
runs) to produce both.

## Run order, end to end (through stage 2)

1. Put the raw USDA export in `01a_yield_data_corn/extracted_usda_corn_data/`
   (and/or `01b_yield_data_soy/extracted_usda_soy_data/`).
2. Set the config cell in `format_corn_yield_data_paper.ipynb` (and/or
   `format_soy_yield_data_paper.ipynb`), run all cells. Produces
   `created_dfs_step1/df_<crop>_yield_<name>.csv`.
3. If NOAA has published newer climdiv files, update and run `webscraper.py`
   to refresh `extracted_noaa_climdiv_data/`.
4. Set the config cell in `merge_yield_monthly_weather.ipynb` (`crop` +
   matching path/filename/save_file trio), run all cells. Produces
   `created_dfs_step2/df_yield_climdiv_<crop>_<name>.csv`.
5. Repeat step 4 for the other crop.

Downstream stages (`03a`/`03b` weather-feature engineering, `04`
comparison/validation) currently still point at the older
`df_yield_climdiv*.csv` filenames from step 2 rather than the new `_paper`
outputs — repointing them is a separate, not-yet-done step.
