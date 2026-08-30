# 00: filter to east of the 100th meridian

A second, parallel pipeline, not a replacement for the main 5-state one. It runs the same yield and weather processing as `01a`/`01b`/`02`/`03b`, but on all US counties east of the 100th meridian instead of just Illinois, Indiana, Iowa, Minnesota and Nebraska. The 100th meridian is a common convention in the literature (Schlenker and Roberts, Roberts and Schlenker, Burke and Emerick all use it), roughly the line between rain-fed agriculture to the east and irrigation-dependent agriculture to the west. This gives a second, larger dataset to test the LLDVE method against, alongside the paper's own 5 states.

I kept this fully separate from the main pipeline on purpose. Every file this branch produces has its own filename (`_east100m` instead of `_paper`), so there's no risk of it overwriting the actual paper's data, and the main pipeline's notebooks and scripts are reused unmodified, just pointed at different files.

## how the filter works

`filter_east_100th_meridian.py` does the actual filtering. It needs two things in this folder:

- `2025_Gaz_counties_national.txt`, the Census Bureau's county gazetteer, source [here](https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html). It gives a centroid longitude for every US county. I use `INTPTLONG > -100` as "east of the 100th meridian". One thing worth knowing: the file is pipe-delimited (`|`), not tab-delimited, despite the `.txt` extension. Of the 3,222 counties nationwide, 2,589 are east of -100 by this measure.
- The raw USDA yield export for all US states, in `extracted_usda_all_states_corn_data/` or `extracted_usda_all_states_soy_data/` depending on crop.

USDA's Quick Stats export tool caps how much a single query can return, so an all-US pull for one crop needs several downloads. I split mine alphabetically by state into 5 files. The script globs every CSV in the relevant folder, checks that no state appears in more than one file (which would mean overlapping downloads and double-counted rows), and concatenates them before filtering.

The raw export has no ready-made FIPS column, so the script builds one from `State ANSI` and `County ANSI` (zero-padded, same as `01a`/`01b` do) to match against the gazetteer's `GEOID`.

## the `crop` toggle

Same pattern as `03b`:

```python
crop = "corn"  # "corn" or "soy"
```

`input_dir` and `target_extracted_dir` are set as a commented pair below it, one for corn, one for soy, and there's a safety check that raises if `crop` and the paths disagree, so a half-finished toggle gets caught rather than silently filtering into the wrong crop's folder.

## where the output goes

The filtered CSV is written straight into `01a_yield_data_corn/extracted_usda_corn_data/` or `01b_yield_data_soy/extracted_usda_soy_data/`, with a filename built from the input batches' own date stamp, for example `082026_corn_yield_east100m.csv`. From there, run `01a`/`01b` as normal, just set `file_name` to that new file and give `save_name` something distinct so it doesn't overwrite the paper's output, for example `df_corn_yield_2026_east100m.csv`. Same idea for `02` and `03b` further downstream, the config cells need pointing at the `_east100m` filenames instead of the `_paper` ones.

## status

Corn: done, through `01a` -> `02` -> `03b`. 41 of 50 states have any corn data in USDA's export at all (the missing 9, Alaska, Hawaii and 7 non-agricultural states, genuinely have none, not a download gap). After the meridian filter, 31 states and 1,859 counties remain, 126,185 rows going into `01a`.

Soy: not started. The all-states raw export hasn't been downloaded yet, `extracted_usda_all_states_soy_data/` exists but is empty. The script is already toggled to handle it once the data's there.
