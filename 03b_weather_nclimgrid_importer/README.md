# 03b: nclimgrid weather importer

Pulls daily weather from NOAA's nclimgrid-daily dataset on S3 and aggregates it into growing-season weather metrics for each county, joined onto the yield and climdiv data from stage 02. This is the step that produces the actual paper dataset.

## why this exists as its own thing

The original version of this stage was built around a published package, [nclimgrid-importer](https://gitlab.cicsnc.org/arc-project/nclimgrid-importer), and its `load_nclimgrid_data` function. I ended up not depending on that package direcly. `nclimgrid_importer.py` in this folder is my own local version of the same idea: it reads the public NODD S3 bucket with `s3fs` (anonymous access, no credentials needed) and returns the same kind of data. As far as I can tell the two do the same job, I just didn't want an external dependency for something this small.

The raw data itself comes from [NOAA's nclimgrid-daily bucket](https://noaa-nclimgrid-daily-pds.s3.amazonaws.com/index.html#EpiNOAA/v1-0-0/csv/cen/), which only goes back to 1951 (the dataset's own start date, not a limitation I introduced). Any yield row from before 1951 ends up with no real weather columns, they come back empty.

## setup

You need Python 3.9 or newer and Poetry.

```powershell
pip install poetry
python -m poetry install
```

I use `python -m poetry install` rather than the bare `poetry` command. On this machine, `poetry`/`poetry run` picks whichever `python` happens to be first on PATH in a given terminal to decide which environment to use, and creates a brand new, empty one if that Python version doesn't already have one. That means the same `poetry run` command can behave differently between two terminals on the same machine, for example VS Code's integrated terminal versus a plain PowerShell window, and one of them can silently end up with an empty environment that then fails with `ModuleNotFoundError`. `python -m poetry` sidesteps that, as long as `python` resolves to the interpreter you used to `pip install poetry` into.

If `poetry install` finds a Poetry environment for this project that already has packages installed by hand, outside Poetry, it will quietly reconcile it to match `poetry.lock`. That can mean downgrading something that was deliberately upgraded later. If versions look wrong after an install, `poetry env list --full-path` shows every environment Poetry has created for this project, and `<path>\Scripts\python.exe -c "import polars; print(polars.__version__)"` tells you what's actually installed in each one.

## running a script

Poetry's own `poetry run` has the reliability problem above, so I call the environment's Python interpreter directly instead. In PowerShell:

```powershell
$venvPath = python -m poetry env info --path
& "$venvPath\Scripts\python.exe" 03b_weather_nclimgrid_importer/get_data_importer.py
```

The first line asks Poetry where the environment actually lives. That path is machine and user specific, something like `C:\Users\<you>\AppData\Local\pypoetry\Cache\virtualenvs\nclimgrid-plotting-<hash>-py3.XX`, so don't hardcode someone else's path, just rediscover it each time. The second line runs the script with that interpreter directly, no activation step, no `poetry run`.

`$venvPath = ...` is PowerShell syntax, it doesn't work as written in `cmd.exe`. If you're in a plain Command Prompt, or you'd rather not deal with variable capture at all, do it in two manual steps instead, works the same in any shell:

```
python -m poetry env info --path
```

That prints the path. Paste it into the run command directly:

```
"C:\Users\<you>\AppData\Local\pypoetry\Cache\virtualenvs\nclimgrid-plotting-<hash>-py3.XX\Scripts\python.exe" 03b_weather_nclimgrid_importer/get_data_importer.py
```

You never need `poetry.exe` itself on PATH for this. If the bare `poetry` command isn't found, `python -m poetry` still works, as long as `python` finds the interpreter you installed Poetry into.

## the `crop` toggle

Both `get_data_importer.py` and `get_data_importer_precip_extremes.py` use one variable near the top of the file:

```python
crop = "corn"  # "corn" or "soy"
```

Both the input filename (`df_yield_climdiv_{crop}_paper.csv`, from stage 02) and the output filename are built from this one variable with an f-string, so there's no multi-line toggle to keep in sync the way stage 02's notebook needs. Change `crop` and rerun.

## outputs

- `get_data_importer.py` writes `created_dfs_step_final/df_final_importer_{crop}_paper.csv`. Five weather metrics: GDD, KDD, TMAX_AVG, PREC, CHD. This is the file the paper's dataset actually uses.
- `get_data_importer_precip_extremes.py` writes `created_dfs_step_final/df_final_importer_precip_extremes_{crop}.csv`. Same base five (I checked the two code paths are AST-identical for those, so the values match exactly), plus exploratory precipitation-extremes metrics: Rx5day, CDD at 1mm and 2mm, R10mm, R20mm. Not part of the paper, run it in addition to the plain script, not instead of it.

Both scripts also compute an SGF block ("Silking to Grain-Fill", July 1 to August 15, set in `config.py`) alongside the growing-season one: `GDD_SGF`, `KDD_SGF`, `TMAX_AVG_SGF`, `PREC_SGF`, `CHD_SGF`. It ends up in the same output file.  I don't use it in the paper, only the growing-season columns. I've left it in rather than stripping it out, it costs a bit of file size and nothing else.
