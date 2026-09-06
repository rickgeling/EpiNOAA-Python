# 02: merge yield with climdiv weather

Joins monthly climdiv weather (max temperature and precipitation) onto the stage-1 yield data, via a county-to-climate-division crosswalk, and computes a growing-season aggregate for each.

## getting fresh weather data

`webscraper.py` downloads the two current climdiv monthly files into `extracted_noaa_climdiv_data/`:

```python
urls = [
    "https://www.ncei.noaa.gov/monitoring-content/data/us/climdiv/monthly/current/climdiv-tmaxcy-v1.0.0-20260806",
    "https://www.ncei.noaa.gov/monitoring-content/data/us/climdiv/monthly/current/climdiv-pcpncy-v1.0.0-20260806",
]
```

Check NOAA's climdiv directory for the current `-v1.0.0-YYYYMMDD` filenames before running this, and update the URLs if a newer one exists. A stale file can quietly contain `-99.90`/`-9.99` missing-value placeholders for recent months instead of real data.  This actually happened once, the `20250506` vintage was missing all of May to December 2025, and the notebook doesn't catch that on its own, it just merges in whatever numbers are there.

## config cell

```python
crop = "soy"  # "corn" or "soy", must match whichever pair is uncommented below

path_df_step1 = "../01b_yield_data_soy/created_dfs_step1/"       #"../01a_yield_data_corn/created_dfs_step1/"
filename = "df_soy_yield_2026.csv"                                  #"df_corn_yield_2026.csv"
save_file = "created_dfs_step2/df_yield_climdiv_soy_paper.csv"      #"created_dfs_step2/df_yield_climdiv_corn_paper.csv"
```

Three lines, all three have to agree with `crop`, there's a safety check right below the cell that raises immediately if only some of them get toggled, rather than silently building a soy output from corn data.

## what it does

1. Loads the two raw climdiv text files and the county-to-climdivs crosswalk.
2. Loads the stage-1 yield CSV for whichever crop is set.
3. Bounds the weather data to the full envelope across every state, from the earliest year any state's yield data covers to the latest, not the narrowest range common to all of them. Climdiv weather goes back to 1895 for every state, well before any yield data starts, so there's nothing to lose by not flooring it to whichever state happens to start latest. I changed this on 2026-08-24, it used to floor everyone to Nebraska's later soy start.
4. Converts temperature to Celsius, precipitation to millimetres, aggregates to the growing season set in `config.py`.
5. Merges yield and weather on state, county, year, outer join, so a county-year with weather but no yield (or the reverse) still shows up rather than getting silently dropped.
6. Saves to `created_dfs_step2/`.

One known non-issue: Illinois county 199 has division 08 in the yield data and 09 in the climdiv data. Checked, its been that way in the source data from the start, not something introduced by this notebook.
