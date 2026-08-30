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
file_name = "082026_corn_yield_paper_states.csv"  # file to get from extracted_usda_corn_data/
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

**Current active files:** corn reads `082026_corn_yield_paper_states.csv` →
writes `df_corn_yield_2026.csv`; soy reads `082026_soy_yield_paper_states.csv`
→ writes `df_soy_yield_2026.csv`. Both extracts include the 2025 season.
(Renamed 2026-08-25 from `..._all_states.csv` — that name became misleading
once the 100th-meridian branch introduced a genuine all-US pull; these two
files only ever covered the 5 corn-belt states used for the paper.)
Older files (`df_corn_yield.csv`, `df_corn_yield_2025.csv`,
`df_soy_yield.csv`) were leftovers from earlier runs, not used by anything
downstream — moved to `OLD/old_content_01a_yield_corn/` and
`OLD/old_content_01b_yield_soy/` on 2026-08-25.

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
`df_yield_climdiv_soy.csv` were from earlier runs, moved to
`OLD/old_content_02_climdiv/` on 2026-08-25. `df_yield_climdiv_soy_paper.csv`
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

## Running the `03b` importer scripts (`get_data_importer.py` / `get_data_importer_precip_extremes.py`)

Unlike stages 1–2 (Jupyter notebooks, run interactively cell-by-cell), stage
`03b`'s scripts are plain `.py` files run from a terminal. They need the
project's Poetry-managed Python environment, which is where most of the
friction is — the steps below sidestep the parts that turned out to be
unreliable in practice.

### First-time setup (once per machine)

1. Install Python 3.9+ if you don't have it.
2. Install Poetry:
   ```powershell
   pip install poetry
   ```
3. From the repo root, install the project's dependencies:
   ```powershell
   python -m poetry install
   ```
   (`python -m poetry ...` instead of bare `poetry ...` — see "Why not just
   `poetry run`?" below for why.) This reads `pyproject.toml`/`poetry.lock`
   and creates an isolated virtual environment with the exact package
   versions the project expects (polars, pandas, pyarrow, s3fs, pendulum,
   etc.) — you don't need to `pip install` any of those individually.

   ⚠️ If a Poetry environment for this project already exists from earlier,
   unmanaged work (packages installed by hand, outside Poetry), `poetry
   install` will silently reconcile it to match `poetry.lock` — which can
   mean *downgrading* packages that were manually upgraded later. If you hit
   this, `poetry env list --full-path` shows every environment Poetry has
   created for this project; `<path>\Scripts\python.exe -c "import polars;
   print(polars.__version__)"` tells you what's actually installed in each
   one.

### Every time you want to run a script

Find the environment's Python interpreter and call it directly:

```powershell
$venvPath = python -m poetry env info --path
& "$venvPath\Scripts\python.exe" 03b_weather_nclimgrid_importer/get_data_importer.py
```

The first line asks Poetry where *your* environment lives — this path is
machine- and user-specific (something like
`C:\Users\<you>\AppData\Local\pypoetry\Cache\virtualenvs\nclimgrid-plotting-<hash>-py3.XX`),
so don't hardcode anyone else's path; always rediscover it with this command.
The second line calls that environment's `python.exe` directly on the
script — no `poetry run`, no activation step needed.

To run the other crop / the precip-extremes variant, edit the `crop = "corn"`
line near the top of the target script first (see next section), then rerun
the same two lines above with the other script's filename.

### Why not just `poetry run get_data_importer.py`?

In practice, on a machine with more than one Python version installed,
`poetry`/`poetry run` picks whichever `python` happens to be first on PATH
*in that specific terminal* to decide which environment to use — and creates
a brand-new, empty one if that Python version doesn't already have one. This
means the same `poetry run` command can behave differently across two
terminals on the same machine (e.g. VS Code's integrated terminal vs. a
plain PowerShell window), including silently building an empty environment
that then fails with `ModuleNotFoundError`. Calling the environment's
`python.exe` directly (as above) sidesteps this — there's no ambiguity about
which environment is being used.

If `poetry`/`python -m poetry` itself isn't found: this just means Poetry's
own install location isn't on PATH (harmless — `pip install poetry` will
tell you exactly where it put `poetry.exe` in its output, e.g. `...\Scripts
is not on PATH`). You never need `poetry.exe` on PATH for this workflow;
`python -m poetry ...` works as long as `python` finds the interpreter you
used to `pip install poetry` into.

### `crop` toggle in the `03b` scripts

Both `get_data_importer.py` and `get_data_importer_precip_extremes.py` use a
single variable near the top of the file:

```python
crop = "corn"  # "corn" or "soy"
```

Both the input filename (`df_yield_climdiv_{crop}_paper.csv`, from stage 2)
and output filename are derived from this one variable via f-string, so
there's no multi-line toggle to keep in sync (unlike the stage-2 notebook) —
just edit `crop` and rerun.

**Outputs:**
- `get_data_importer.py` → `created_dfs_step_final/df_final_importer_{crop}_paper.csv`
  — the base five weather metrics (GDD, KDD, TMAX_AVG, PREC, CHD). This is
  the file downstream stages/the paper dataset expect.
- `get_data_importer_precip_extremes.py` → `created_dfs_step_final/df_final_importer_precip_extremes_{crop}.csv`
  — the same base five (identical values, verified AST-identical code path)
  plus exploratory precip-extremes metrics (Rx5day, CDD_1mm/2mm, R10mm/20mm).
  Not part of the paper dataset — run in addition to, not instead of, the
  plain script.

Both scripts also compute an SGF ("Silking to Grain-Fill", `config.py`'s
`SGF_START_MONTH`/`SGF_END_MONTH` — July 1 to August 15) block alongside the
GS one (`GDD_SGF`, `KDD_SGF`, `TMAX_AVG_SGF`, `PREC_SGF`, `CHD_SGF`), written
into the same output file. **Not used in the paper** — only the GS columns
are. Left in for now rather than removed from the scripts.
