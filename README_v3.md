# EpiNOAA-Python

Turns raw USDA county-level corn and soy yield data and NOAA weather data
into a merged county-year panel, ready for weather-sensitivity analysis.
It does not do the econometric modelling itself, the LLDVE and RCS models
live in a separate repository.

## paper

[paper title, citation, link once available]

## why

Before any modelling can happen, the raw yield and weather data need
cleaning, a county-to-climate-division crosswalk, and merging into one
panel. I kept that work in its own repo, separate from the modelling code.

## setup

```powershell
pip install poetry
python -m poetry install
```

Notebooks (stages 01, 02) run interactively in VS Code. Stage 03b's scripts
need Poetry's environment run a specific way, see
`03b_weather_nclimgrid_importer/README.md`.

## data

Sources and links: `docs/Sources data.txt`.

- USDA NASS Quick Stats: county-level corn/soy yield.
- NOAA climdiv: monthly temperature and precipitation, county-to-division crosswalk.
- NOAA nclimgrid-daily (S3): daily weather, 1951 onwards.
- Census Gazetteer: county centroid coordinates, used for the 100th-meridian filter.

Raw data extracts live in each stage's own `extracted_*` folder. The large
nclimgrid parquet archive used by stage 03a is not in the repo, and that
stage isn't currently maintained.

## the pipeline (5-state paper dataset)

1. `01a_yield_data_corn/format_corn_yield_data_paper.ipynb` and
   `01b_yield_data_soy/format_soy_yield_data_paper.ipynb`: clean the raw
   USDA export, one crop each. Output: `created_dfs_step1/df_<crop>_yield_2026.csv`.
   Details: `01a_yield_data_corn/README.md`, `01b_yield_data_soy/README.md`.
2. `02_weather_climdiv_crosswalk/merge_yield_monthly_weather.ipynb`: joins
   in climdiv monthly weather. Run once per crop. Output:
   `created_dfs_step2/df_yield_climdiv_<crop>_paper.csv`.
   Details: `02_weather_climdiv_crosswalk/README.md`.
3. `03b_weather_nclimgrid_importer/get_data_importer.py`: joins in
   nclimgrid daily weather, aggregated to growing-season metrics (GDD, KDD,
   TMAX_AVG, PREC, CHD). Run once per crop. Output:
   `created_dfs_step_final/df_final_importer_<crop>_paper.csv`. This is the
   paper's dataset. Details, including the Poetry setup:
   `03b_weather_nclimgrid_importer/README.md`.
4. `04_compare_validate/compare_GS_climdiv_nclimgrid.ipynb`: sanity-checks
   climdiv against nclimgrid for the same counties.

A second, parallel pipeline covers all US counties east of the 100th
meridian, a common convention in the literature, as a larger dataset to
test the same method against. See
`00_filter_east_100th_meridian/README.md`.

## notes

- Stage 03a (local nclimgrid parquet) isn't currently maintained, and the
  month-level comparison notebook in stage 04 is blocked on it. Details,
  including where the parquet files come from:
  `03a_weather_nclimgrid_local/README.md`.
- Nebraska has no county-level soybean yield data before 1960 in USDA's
  records. Not a bug, I checked the raw export directly.
- Both `03b` scripts also compute an SGF ("Silking to Grain-Fill") weather
  block alongside the growing-season one, GDD_SGF, KDD_SGF, TMAX_AVG_SGF,
  PREC_SGF, CHD_SGF. Not used in the paper, only the growing-season metrics
  are. It's left in and still configurable (`config.py`,
  `SGF_START_MONTH`/`SGF_END_MONTH`, currently 1 July to 15 August) for
  anyone who wants to set a different window and look at that instead.
- Environment quirks (Poetry, PATH) are documented in
  `03b_weather_nclimgrid_importer/README.md`, not repeated here.

## citation

[BibTeX, once available]
