# 03a: nclimgrid weather from local parquet files

Not currently maintained. I'm using `03b`'s live S3 version instead, this stage needs the local parquet files kept up to date by hand, and I haven't been doing that. Last I checked, the local folder only had data through 2024, so this stage can't produce a complete 2025 result right now.

## what it would do

Same idea as `03b`, join nclimgrid daily weather onto the stage-2 yield and climdiv data, aggregated to growing-season metrics, except reading from parquet files already sitting on disk instead of fetching them from S3 at runtime.

- `get_data_local.py`: the main script, same `crop` toggle and output shape as `03b`'s `get_data_importer.py`.
- `get_compare_months_local.py`: builds the month-level comparison file that `04_compare_validate/compare_MONTHS_climdiv_nclimgrid.ipynb` and `compare_local_vs_importer.py` are both blocked on.
- `old/`: helper scripts from setting this up originally, `create_folders.py`, `move_files.py`, `read_202307.py`, `all_months_check.py`, `stats_parquet_data.py`. Not part of the regular workflow, kept for reference.

## where the parquet files come from

Same public NODD bucket `03b` streams from, county level (`cty`), just downloaded by hand instead of fetched live. Verified directly against the bucket:

Browsable:
```
https://noaa-nclimgrid-daily-pds.s3.amazonaws.com/index.html#EpiNOAA/v1-0-0/parquet/cty/
```

Direct file, one per month, no login needed:
```
https://noaa-nclimgrid-daily-pds.s3.amazonaws.com/EpiNOAA/v1-0-0/parquet/cty/YEAR=<yyyy>/STATUS=scaled/<yyyymm>.parquet
```

`move_files.py` expects the downloaded files named `<yyyymm>.parquet` (matches what the bucket calls them) sitting in your Downloads folder, and moves them into a year-numbered subfolder here. To bring this stage back up to date, that's the manual step: download each missing month, run `move_files.py`, then rerun `get_data_local.py`.
