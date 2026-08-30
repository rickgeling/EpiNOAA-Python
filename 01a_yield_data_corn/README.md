# 01a: format corn yield data

Cleans the raw USDA county-level corn yield export into one row per county per year.

## what it does

`format_corn_yield_data_paper.ipynb`, run interactively, cell by cell:

1. Reads the raw export, keeps year, state, county, district, data item, value, CV.
2. Drops district code `99` (state-level rows with no real county), drops rows with no county, zero-pads state (2 digits) and county (3 digits) to match NOAA's format, reverses the district code digits so it lines up with NOAA's own numbering.
3. Reports each state's year range and flags anything starting after 1951. This is a report only, nothing gets trimmed. 1951 is nclimgrid-daily's own start date, stage 03b can't attach weather before that regardless of what the yield data has, so it's the only floor worth knowing about here.
4. Filters to `CORN, GRAIN - YIELD, MEASURED IN BU / ACRE` only, drops irrigated/non-irrigated splits.
5. Reports missing years and missing values per county, against that state's own observed range, not one shared window across every state.
6. Saves to `created_dfs_step1/`.

## config cell

```python
file_name = "082026_corn_yield_paper_states.csv"
save_name = "df_corn_yield_2026.csv"
```

`file_name` is read from `extracted_usda_corn_data/`. Currently covers the 5 corn-belt states used in the paper: Illinois, Indiana, Iowa, Minnesota, Nebraska, 1929 to 2025, and all 5 states already agree on that range exactly, no gaps.

To run this on a new export, drop the raw CSV into `extracted_usda_corn_data/`, update `file_name`/`save_name`, run all cells. For the 100th-meridian branch, point `file_name` at the filtered file from `00_filter_east_100th_meridian/` (currently `082026_corn_yield_east100m.csv`) and give `save_name` something distinct, e.g. `df_corn_yield_2026_east100m.csv`, so it doesn't overwrite the paper's output.
