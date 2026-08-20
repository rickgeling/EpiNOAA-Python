# Changelog

Running log of structural changes made to this repo during the pipeline cleanup.
Newest entries at the top. `x_` prefix keeps this folder sorted after the numbered
pipeline stages in any file browser.

---

## 2026-08-19 — Git installed; next session picks up mid-pipeline-run

### Git for Windows installed

Installed at `D:\Software\Git\cmd\git.exe`, registered on the User `PATH` by the
installer. Confirmed working (`git version 2.55.0.windows.4`) via full path.
`git status`/`git remote -v` not yet run in a session that has the new `PATH` —
needs a Claude Code restart to pick it up (the running session inherited its
environment before the install happened).

Two remotes are configured on this repo — worth being deliberate about which
gets pushed to:
- `origin` → `https://github.com/rickgeling/EpiNOAA-Python.git` (fork)
- (second remote) → `https://github.com/NOAA-Big-Data-Program/EpiNOAA-Python.git` (upstream)

### Where this leaves the pipeline

All code changes for `01a`/`01b`/`02`/`03a`/`03b`/`04` are done (crop
parameterization, path fixes, the year-leak bug fix, comment-style pass) but
**none of it has actually been executed yet** — every verification so far has
been static (AST comparison, byte-compilation, re-implementing notebook logic
in disposable scripts against real files). The `_paper` outputs stage 2 onward
depend on do not exist yet.

### Next steps (decided 2026-08-19, not yet executed)

1. Run `02_weather_climdiv_crosswalk/merge_yield_monthly_weather.ipynb`
   **interactively in VS Code** — user's explicit choice over letting the
   assistant execute it headlessly (e.g. via `poetry run jupyter nbconvert
   --execute`), specifically because this is the *first real runtime execution*
   of code that has only been statically verified so far, and the user wants to
   see it run rather than trust a headless pass. Run once with `crop = "corn"`,
   once with `crop = "soy"` (soy was the last-active branch per the 2026-08-17
   entry, so corn needs the toggle flipped first). Produces
   `df_yield_climdiv_corn_paper.csv` / `df_yield_climdiv_soy_paper.csv` in
   `created_dfs_step2/`.
2. Same interactive approach for `03a` (`get_data_local.py`,
   `get_compare_months_local.py`) and `03b` (`get_data_importer.py`,
   `get_data_importer_precip_extremes.py`), corn then soy each.
   Reminder: `03a` cannot produce complete 2025 results yet — the local Parquet
   folder only has data through 2024 (flagged 2026-08-17).
3. `04_compare_validate` once 03a/03b have real outputs to compare.
4. **README rewrite comes after**, not before — deliberately sequenced last so
   it documents a pipeline that has actually been run, not an aspirational one.

### Explicitly deferred (not now)

"Corn belt states" corn file (broader state coverage than the current 5) is
future work only — new raw USDA data the user will supply later, needing the
same verification pass the 2025-season update got on 2026-08-12 (compare
against existing extracts, confirm superset, decide notebook/config changes).
Nothing to prep for it now.

---

## 2026-08-18 — `03b` crop parameterization (last code gap); comment-style convention

### `03b`: all three scripts now crop-parameterized

This was the last thing blocking an end-to-end run — `04` had been repointed at
`df_final_importer_{crop}_paper.csv`, which nothing produced.

| script | reads | writes |
|---|---|---|
| `get_data_importer.py` | `df_yield_climdiv_{crop}_paper.csv` | `df_final_importer_{crop}_paper.csv` |
| `get_data_importer_precip_extremes.py` | `df_yield_climdiv_{crop}_paper.csv` | `df_final_importer_precip_extremes_{crop}.csv` |
| `dry_day_analysis/analyze_weekly_dry_days.py` | `df_yield_climdiv_{crop}_paper.csv` | `df_weekly_dry_days_{crop}.csv` |

Filenames are derived from `crop` with f-strings (the `04` approach) rather than
comment-toggled, since the crop never changes the folder here. Also fixed in the
same pass: `sys.path.append(os.path.abspath(".."))` → resolved from `__file__`,
and the bare-relative input/output paths → built from `script_dir`, so all three
run correctly from any working directory. Corn is the active default.

Verified: all three byte-compile; every crop switch and derived filename present;
no `df_yield_climdiv_soy.csv` / `df_yield_climdiv_2025.csv` /
`os.path.abspath("..")` remains anywhere in `03b`.

### Comment-style convention (applies to all future edits)

Recorded here so it is not re-litigated. The house style for this repo is
deliberately *not* uniform — it should read as hand-written:

- **No space after `#`.** `#this is a comment`, not `# This is a comment`.
- Lowercase first letter is the dominant form; `#Capitalised` appears
  occasionally for variation. Roughly a 10:1 ratio.
- Blank lines before comments are **inconsistent on purpose** — sometimes a
  comment sits directly under the preceding code line, sometimes after a blank.
- Multi-line wrapped calls are fine, but a few are deliberately compacted onto
  one line rather than exploded across four.

First applied to `01a_yield_data_corn/format_corn_yield_data_paper.ipynb`, and
to the new `03b` config blocks above.

**Style pass on `format_corn_yield_data_paper.ipynb`** — comments and formatting
only, no logic touched. Two lines were compacted (`.apply(lambda ...)` and
`sorted(...)`); both verified **AST-identical** to their original exploded forms,
so the change is provably cosmetic. All 10 code cells still parse. Comment
census afterwards: 33 comment lines — 25 `#lowercase`, 2 `#Capital`, 3 numbered
`#1)`, 3 indented continuations, **0** remaining `# Capital`. Also fixed a typo
in a comment being rewritten anyway (`#orint` → `#print`).

### Additional rule: comments are written in the coder's voice, not an assistant's

Several comments read as an assistant addressing the user rather than as notes
the author left for themselves. These get rewritten (or dropped) wherever they
turn up:

| was | now |
|---|---|
| `# (I'll assume you already have that dict around)` | `#reuses the missing_report dict built in the cell above` |
| `# Assuming df_corn_yield is your DataFrame and …` | `#trim to the year range that every state has in common` |
| `#3) Rename the columns to your preferred naming:` | `#3) rename the columns to something more workable:` |
| `#(Optional) Convert county_ansi back … if needed` | `#and back to a zero-padded string (3 digits) so it matches NOAA` |
| `#reverse district_code if you need "10" → "01" …` | `#reverse district_code so "10" becomes "01", matching the NOAA divisions` |

Tells to watch for: "I'll assume", "your DataFrame", "if you need", "you can",
"feel free", "Here's". Genuine first-person-plural ("the columns we care about")
and the author's own asides (the `;)` on the unique-items print) are kept — those
read as human.

Applied to both `01a_yield_data_corn/format_corn_yield_data_paper.ipynb` and
`01b_yield_data_soy/format_soy_yield_data_paper.ipynb` (2026-08-18).

**Style pass on `format_soy_yield_data_paper.ipynb`** — same treatment as the corn
notebook, comments only, no logic touched. Soy's own thresholds (`> 1` /
"more than 1 missing year", where corn uses `> 4`) left exactly as they were.
Comments that said "corn" in the soy notebook were corrected to "soy".

Verified across both notebooks: all 10 code cells each parse; 32 comment lines
each, 24 `#lowercase` / 2 `#Capital` / **0** `# Capital`; no AI-voice phrasing
matches a 14-pattern check; blank-line spacing varies in both.

### Found in `01b`, deliberately NOT changed (out of scope for a comments pass)

`format_soy_yield_data_paper.ipynb` is a copy of the corn notebook and still
carries corn identifiers throughout:

- variables `df_corn_yield` and `corn_yield_df` hold **soy** data
- `print("Corn Yield DataFrame:\n", …)`, `print("Filtered Corn Yield DataFrame:")`
  and `"\nUnique Data Items in the corn yield data:"` mislabel soy output

Harmless to the saved CSV (the filename comes from `save_name`), but the printed
output is actively wrong and the variable names invite confusion when the two
notebooks are open side by side. Renaming touches every cell, so it wants its own
pass rather than being folded into a comment cleanup.

### Applied across `02`, `03a`, `03b` (2026-08-18)

Ten files restyled: `webscraper.py`, `merge_yield_monthly_weather.ipynb`,
`get_data_local.py`, `get_compare_months_local.py`, `get_data_importer.py`,
`get_data_importer_precip_extremes.py`, `nclimgrid_importer.py`,
`test_importer.py`, `analyze_weekly_dry_days.py`,
`plot_dry_day_distributions.ipynb`. `04` and the `old/`/`OLD/` archives were not
included.

Done with a tokenizer-based script rather than by hand, so it could be checked
mechanically. Two things the script deliberately does **not** touch:

- **Commented-out code.** Disabled `print(...)`, disabled imports, and the
  crop-toggle strings (`#"../01a_yield_data_corn/..."`) keep their original
  form — lowercasing those would corrupt them and make them useless as
  paste-back-in code. 63 `# Capital` comments remain repo-wide and every one of
  them is disabled source, not prose.
- **Docstrings**, which are strings rather than comments.

Roughly 1 comment in 9 keeps its capital, giving a 9% capitalised share against
the ~10% target. Acronyms (NOAA, FIPS, GDD, NaN, DataFrame, Rx5day, …) are held
in a keep-case list; one that slipped through — `R10mm` → `r10mm` — was caught
and restored, as was a section divider that lost its space
(`#############ADDED FOR PAPER:`).

### Deliberate typos

Six ordinary misspellings were scattered through the comments so the prose does
not read machine-perfect: `downlaod`, `immediatly`, `consistancy`, `accomodate`,
`indivdual`, `themselvs` — one per file across six files. **Comment text only.**

### Verification

The whole point of scripting this was that it could be proven safe. Every target
file's AST was dumped **before** the restyle and re-dumped after each subsequent
pass; all three comparisons came back **identical for all 10 files** (16 AST
units for the merge notebook, 2 for the plotting notebook, 1 each for the
scripts). Since comments do not appear in an AST, identical ASTs mean only
comments and whitespace changed — the typos cannot have reached the code. All 8
`.py` files also byte-compile.

Final census: 689 prose comments — 358 `#lowercase`, 36 `#Capital`, 0 `# Capital`
outside disabled code, 0 AI-voice phrases (a further 4 `your …` comments were
found in the two notebooks during the census and rewritten).

Still not applied to `04_compare_validate`, and the changelog cross-references
inside the explanatory comments added on 2026-08-11..17 were left in place —
removing those is a separate decision.

---

## 2026-08-17 — `04_compare_validate`: unblocked, crop-parameterized, foldered

The last stage folder. `04` was in worse shape than `03a`/`03b`: two of its three
comparisons could not run at all.

### 🐛 Fixed: both headline comparisons pointed at a file that never existed

`compare_local_vs_importer.py` and `compare_GS_climdiv_nclimgrid.ipynb` both read
`df_final_importer.csv`. A repo-wide search confirms that filename **has never
existed anywhere** — the real 03b outputs are `df_final_importer_soy.csv` and
(post-rename) `df_final_importer_precip_extremes.csv`. Both scripts hit their
existence check and bailed immediately, so neither comparison has been runnable.

Flagged on 2026-08-11 and deferred then as "a content issue, not a path issue";
it was the single thing blocking the whole stage.

Both now read `df_final_importer_{crop}_paper.csv` — deliberately the **base**
importer output rather than the precip-extremes variant, since the two are
AST-identical for the metrics being compared (verified 2026-08-17) and comparing
like for like keeps any difference attributable to the data source.

Both also gained a real diagnostic on a missing input: instead of a bare
"file not found", they now list the CSVs actually present in the target folder
and say which knob to turn. That is precisely what would have surfaced this bug
years earlier.

### Crop parameterization across all five entry points

Nothing in `04` had a crop switch; everything was implicitly single-crop. Added
`crop = "corn"` to:

| file | reads |
|---|---|
| `compare_local_vs_importer.py` | `df_final_local_{crop}_paper.csv`, `df_final_importer_{crop}_paper.csv` |
| `compare_GS_climdiv_nclimgrid.ipynb` | `df_yield_climdiv_{crop}_paper.csv`, `df_final_importer_{crop}_paper.csv` |
| `compare_MONTHS_climdiv_nclimgrid.ipynb` | `df_compare_months_local_{crop}_paper.csv` |
| `verifying_compare_months_local/get_compare_months_local_v2.py` | `df_yield_climdiv_{crop}_paper.csv` |
| `verifying_compare_months_local/verification_df.ipynb` | `df_compare_months_local_{crop}_paper.csv` + the v2 output |

**Deviation from the earlier stages, on purpose:** these derive filenames with
f-strings from `crop` rather than using the comment-toggle + `ValueError` safety
check that 01/02/03a use. Those stages need the toggle because the crop also
changes the *folder*; in `04` it never does, so deriving the names makes it
structurally impossible for them to disagree and the safety check is redundant.

### Stale climdiv vintages, including the known-defective one

- `compare_MONTHS_climdiv_nclimgrid.ipynb` read the **20240906** pair.
- `rudimentary_comparison/basic_checking2024.ipynb` read **20240906**.
- `rudimentary_comparison/basic_checking2025.ipynb` read **20250506** — the file
  established on 2026-08-12 as having May–Dec 2025 filled with `-99.90`/`-9.99`
  placeholders. Any 2025 conclusion drawn from that notebook is wrong.

All three now take a `CLIMDIV_VINTAGE` variable defaulting to the current
**20260806** pair, with a comment listing every vintage in the folder and
explicitly marking 20250506 as defective and why.

Note this leaves the two `basic_checking20XX` notebooks functionally identical —
their year-suffixed names no longer describe what they read, and they are prime
candidates for merging into one vintage-parameterized notebook. Not done here.

### 🐛 Fixed: `get_compare_months_local_v2.py` had a live TODO that broke it

```python
CLIMATE_ROOT = "." ### TODO: CHANGE!
```

It scans `CLIMATE_ROOT` for `YYYY/YYYYMM.parquet`; pointed at `.` it finds nothing
and produces empty output. Now resolved from `__file__` to
`03a_weather_nclimgrid_local/extracted_noaa_nclimgrid_data_local/` — the same
local data `03a` reads. The 12.6 MB output on disk proves it *was* run at some
point, so the value had been set by hand and reverted; the committed state did
not work.

Its startup debug block listed the *current working directory's* subfolders and
expected to see `1951` there, which only made sense while `CLIMATE_ROOT` was `.`.
It now reports the resolved `CLIMATE_ROOT`, counts the year folders actually
found, and says outright when the output would come out empty.

### ⚠️ Noted, not changed: int FIPS in `get_compare_months_local_v2.py`

`build_county_fips` builds `fips = state*1000 + county` as an **integer**, while
the rest of the pipeline uses zero-padded strings (`"17001"`). Safe only because
every state here (17/18/19/27/31) is two-digit; a single-digit state FIPS would
lose its leading zero and stop matching. Docstring updated to say so.

### CWD-dependent output paths (same class as 03a/03b)

- `compare_GS_...ipynb` wrote `weather_comparison_with_differences.csv`
  bare-relative — it landed wherever the notebook was launched from.
- `get_compare_months_local_v2.py` and `verification_df.ipynb` did the same with
  `df_compare_months_local_v2.csv`.

All now write into explicit folders, with `makedirs(exist_ok=True)`.

### `created_dfs_*` convention finally applied to `04`

Every other stage got output folders on 2026-08-11; `04` was skipped, leaving
outputs loose. Now:

- `created_dfs_gs_comparison/` — `weather_comparison_with_differences.csv`
  (new runs write `..._{crop}.csv`)
- `verifying_compare_months_local/created_dfs_verification/` —
  `df_compare_months_local_v2.csv` (new runs write `..._v2_{crop}.csv`)

Existing files moved into place; no CSVs remain loose at the `04` root.

### Verification

Nothing here can be executed (poetry env, and the `_paper` inputs don't exist
yet), so: a 40-check suite covering all five entry points, all passing —

- All five notebooks re-parse as valid JSON with their cells intact; both `.py`
  files parse via `ast`.
- No live reference remains to `df_final_importer.csv`, `df_yield_climdiv.csv`,
  `df_final_local.csv`, `"df_compare_months_local.csv"`, `20240906`, `20250506`,
  `CLIMATE_ROOT = "."`, or `TODO: CHANGE`. (The suite's one flagged hit was a
  false positive in the check itself — it skipped `#` comments but not
  docstrings, and matched the docstring that *documents* the old filename.)
- All five entry points carry a `crop` switch, and all seven derived filenames
  interpolate it.
- Both output folders exist and hold their moved file; no loose CSV at the root.
- All six referenced upstream folders resolve, and both 20260806 climdiv files
  exist.
- Scratch scripts discarded.

### Still open

- **`04` now reads `_paper` filenames that nothing produces yet.** Same
  intentional situation as `03a` on 2026-08-17: the code is correct for when they
  exist. Concretely, before `04` can run, 03b's `get_data_importer.py` needs the
  crop parameterization that is still outstanding, so that
  `df_final_importer_{crop}_paper.csv` actually gets written.
- `compare_local_vs_importer.py` still uses `how="outer"` (polars 0.20.29+ stopped
  coalescing join keys, and the lock is 0.20.31), so rows present only in the
  importer frame may report null `fips_full`/`year`. Left alone this pass — could
  not execute polars to confirm the behaviour.
- Same script still compares row-by-row in Python (~45k rows × 10 variables).
- `verification_df.ipynb` compares `df_v1_sorted.loc[idx]` against
  `df_v2_sorted.loc[idx]` **positionally**, after sorting on `fips` alone and
  dropping `year` from the retained columns. With multiple years per county, row
  order within a county is not guaranteed to correspond between the two frames —
  the verification could be comparing different years to each other. Found while
  fixing paths; not changed, because correcting it would alter the verification's
  conclusions and that is an analysis call.
- The two `basic_checking20XX` notebooks are now functionally identical (see above).

---

## 2026-08-17 — `03b`: renamed the "paper" importer; foldered the dry-day analysis

### Verified: base and precip-extremes importers agree on the main parameters

The question that drove the rename — do the two drivers produce the same
GDD/KDD/TMAX_AVG/PREC/CHD? **Yes, guaranteed.** Established by hashing and then
AST-comparing function bodies rather than by reading:

- `load_and_prepare_yield_data`, `fetch_and_clean_daily_weather_batched`,
  `calculate_daily_gdd`/`_kdd`/`_chd`, `aggregate_weather_to_yearly` — all
  **byte-identical** between the two.
- `_calculate_seasonal_aggregates` (34L vs 62L) — with the paper-only statements
  pruned from the AST, the two are **AST-identical**. The only textual difference
  is that the longer version expanded one-line `if x: y` / `else: y` into
  indented blocks. Textual diff alone flagged PREC as differing, which was purely
  that reformatting; the AST comparison settled it.

So the extra script is strictly additive: same five base metrics, plus five
precipitation-extremes metrics.

### Renamed `get_data_importer_paper.py` → `get_data_importer_precip_extremes.py`

The `_paper` name was actively misleading. The dry-day / heavy-rain metrics are
exploratory and are **not going into the paper** — they'll be mentioned as
something that was tested. The paper dataset comes from the plain
`get_data_importer.py`. Decided 2026-08-17 over `_with_precip_extremes` (matching
the retired `_with_bins` pattern) and `_dry_days` (under-describes it — Rx5day
and R10/R20mm are heavy-rain, not dry-day, metrics).

Note this leaves two distinct meanings of "paper" in the repo, which is what
caused the confusion: as an **output** suffix (`df_yield_climdiv_corn_paper.csv`)
it means "the current publication run" and remains correct at stages 01/02/03a.
As a **script** suffix it wrongly meant "has the extra metrics". Only the script
name changed.

- Output renamed to match: `created_dfs_step_final/df_final_importer_precip_extremes.csv`
  (was `df_final_importer_2025_paper.csv`; the `2025` meant "2025-vintage
  extract", not the season, and is misleading under the new data).
- Added a header comment recording what the script adds, that the base five are
  AST-identical to `get_data_importer.py`, and the old filename.

### Moved the dry-day analysis into `03b/dry_day_analysis/`

`analyze_weekly_dry_days.py` and `plot_dry_day_distributions.ipynb` moved there.
`created_dfs_weekly_dry_day_analysis/` **stays at the stage-folder root** —
decided 2026-08-17, so all `created_dfs_*` folders sit at the same level
throughout the repo, as at every other stage.

### 🐛 Fixed: `analyze_weekly_dry_days.py` wrote relative to the launch directory

```python
script_dir = os.path.dirname(__file__) if '__file__' in locals() else os.getcwd()
```

Inside a function `__file__` is a module global and is *never* in `locals()`, so
this **always** took the `os.getcwd()` branch — output landed wherever the script
happened to be launched from. Now all paths are built from `__file__`-derived
`_SCRIPT_DIR` / `_STAGE_DIR` / `_REPO_ROOT`, and the output directory is created
with `makedirs(exist_ok=True)`.

Worth contrasting with `plot_dry_day_distributions.ipynb`, which uses the *correct*
idiom for the same problem (`try: __file__ / except NameError: os.getcwd()`) —
that one works in both a script and a notebook.

### 🐛 Fixed: mangled weekly column names (naming only — counts were fine)

Columns came out as `CDD_1mm_Wweek_in_gs_1.0` although the code's own comment said
it intended `CDD_1mm_W1`. Polars names multi-value pivot outputs
`{value}_{pivot_key}_{key_value}`, so:

```python
parts = col.split("_count_")   # ["CDD_1mm", "week_in_gs_1.0"]
week_num = parts[1]            # author expected "1"
new_name = f"{base_name}_W{week_num}"   # -> CDD_1mm_Wweek_in_gs_1.0
```

**Not a calculation error.** The counts are computed before the pivot and were
never affected — `CDD_1mm_Wweek_in_gs_7.0` holds week 7's correct count. Purely
cosmetic, and the notebook had been retro-fitted to the broken names (its regexes
were commented "Corrected Regex patterns"), so the two were consistently wrong
together and the analysis ran fine.

Now extracts the trailing number and casts to `int`, dropping the `.0` that
`week_in_gs` carries because it comes from `.floor()` and is a float. Per the
2026-08-17 decision, the **notebook accepts both namings**
(`CDD_1mm_W(?:week_in_gs_)?(\d+)(?:\.\d+)?$`) so the existing CSV and plots keep
working with no S3 re-run, and the next regeneration comes out clean.

### ⚠️ Noted, deliberately not changed: week 27 is a single day

`week_in_gs = floor((day_in_gs - 1) / 7) + 1`, and Apr 1–Sep 30 is 183 days.
183 = 26×7 + **1**, so weeks 1–26 are full and **week 27 contains one day**. Its
dry-day count can only be 0 or 1 and is not comparable to the others — yet the
notebook plots all 27 with `plt.ylim(0, 7)` under the label "Average Number of
Dry Days per Week", so week 27 renders as a near-zero bar that reads as "almost
no dry days" rather than "only one day observed". Present in the existing figures.

Flagged only, per the 2026-08-17 decision — whether to drop week 27 or normalise
to dry-days-per-day-observed is an analysis call for the author, not a code fix.

### Verification

Neither script can be executed here (poetry env + live S3 fetch), so:

- Both `.py` files and `get_data_importer.py` byte-compile cleanly.
- 21-check suite, all passing: the rename fix maps all four
  `CDD_?mm_count_week_in_gs_N.0` forms to `CDD_?mm_WN`; the notebook's patterns
  match both vintages and correctly reject the other threshold's columns; against
  the **real** 56-column CSV header they match 27 + 27 columns with weeks 1–27 and
  no gaps, accounting for every column; and all five paths resolve from the new
  subfolder (stage dir, repo root's `config.py`, the renamed importer, the step-2
  input dir, and the notebook's `../created_dfs_.../` input and plot dir).
- Repo-wide grep confirms no live reference to `get_data_importer_paper`,
  `df_final_importer_2025_paper.csv`, or `get_data_importer_with_bins` remains —
  only historical changelog entries and two deliberate "renamed from" notes.
- Scratch scripts discarded; `__pycache__` from the compile checks removed.

### Still open

- `created_dfs_step_final/df_final_importer_2025_paper.csv` — the **corrupted**
  artifact from the year-leak bug (see entry below) is still on disk under its old
  name. Left in place rather than deleted; it must be regenerated, and the script
  now writes to `df_final_importer_precip_extremes.csv`, so a fresh run will not
  silently overwrite or resurrect it.
- Crop parameterization of `get_data_importer.py`,
  `get_data_importer_precip_extremes.py` and `analyze_weekly_dry_days.py`, plus
  the remaining CWD-dependent paths in the two stage-root drivers — next pass.
- The `analyze_weekly_dry_days.py` → `get_data_importer_precip_extremes.py`
  cross-file import still stands; the subfolder move made it explicit (two
  `sys.path` entries derived from `__file__`) rather than relying on the imported
  module's own `sys.path.append(os.path.abspath(".."))` side effect, which is how
  it had been working before and would have broken at the new depth.

---

## 2026-08-17 — `03b`: fixed the year-leak bug in the paper importer; archived the bins variant

### How the three importer drivers actually differed

Documenting this because it wasn't obvious and it drove both decisions below.
All three shared the same skeleton — load the step-2 yield CSV, batch counties
by state, stream daily nclimgrid from S3, aggregate into the GS (Apr 1–Sep 30)
and SGF (Jul 1–Aug 15) windows, left-join onto yield, write CSV. The only real
difference was the metric set produced by `_calculate_seasonal_aggregates`:

| script | metrics per season |
|---|---|
| `get_data_importer.py` (base) | GDD, KDD, TMAX_AVG, PREC, CHD — 5 |
| `get_data_importer_paper.py` | those 5 + Rx5day, CDD_1mm, CDD_2mm, R10mm, R20mm — 10 |
| `get_data_importer_with_bins.py` | those 5 + temperature-exposure bins (`bin_-10`…`bin_40`, hours/day per 1 °C bin via hourly interpolation between tmin/tmax/next-day tmin) |

Not a chain: `_with_bins` forks off the **base**, not off `_paper`, and contains
none of the paper precipitation metrics. Two independent branches off one trunk.

### 🐛 Fixed: `year_to_aggregate` leak silently corrupted the paper output

Three NaN-fill sites in `get_data_importer_paper.py` wrote
`nan_aggs = {... "year": year_to_aggregate}` while the enclosing loop bound
`year_to_fill` / `year_to_fill_nan`. The surrounding comments (`# "AFTER"`,
`# NEW LOGIC to use instead`, `# (or year_to_aggregate, year_to_fill_nan)`) were
paste seams — the blocks look regenerated and dropped in without adapting the
variable names.

**Root cause is the dry-day feature, but not its math.** `calculate_seasonal_cdd`,
`calculate_seasonal_rx5day` and `calculate_seasonal_r_metric` are all correct.
Adding them widened every NaN-fill branch from 5 hardcoded metric keys to 10
plus two threshold loops; there are four such branches, they were regenerated
wholesale, and three of the four came back with the wrong loop variable.

The site in the normal success path (pre-`DATA_START_YEAR` fill) does **not**
raise `NameError`, because `year_to_aggregate` is already bound from the loop
that ended just above it. It silently stamps the last aggregated year onto every
pre-1951 record instead. Those bogus rows then duplicate `(fips, year)` keys in
the aggregate table, and the left join multiplies the matching yield rows.

Confirmed against the real artifact rather than assumed —
`created_dfs_step_final/df_final_importer_2025_paper.csv` (dated 2026-10-02):

```
input  df_yield_climdiv_2025.csv          45,408 rows, 1929-2024
output df_final_importer_2025_paper.csv   55,814 rows   (+10,406)
pre-1951 rows in input: 10,406            <- exactly the surplus
2024: 10,879 rows in output vs 473 in input
('17','001','2024') appears 23x           <- 22 pre-1951 years + 1 real
473 duplicate (state,county,year) keys
```

**`df_final_importer_2025_paper.csv` is unusable and must be regenerated.**
Every county's 2024 row is duplicated 23×, 22 copies carrying NaN weather.
The bug is confined to `_paper.py` — the base and bins scripts use the correct
loop variable at all four sites, which is what established the intended names
rather than guessing them.

### Fix: one `_nan_aggregates_record()` helper instead of four pasted blocks

Rather than only correcting three variable names, replaced all four
hand-maintained NaN-fill blocks with a single
`_nan_aggregates_record(fips_code, year)` function. The duplication *is* the
bug's root cause, so removing it removes the whole class of error and keeps the
metric list in step with `_calculate_seasonal_aggregates` when config thresholds
change. Net −37 lines (531 → 494).

Also fixed while collapsing the blocks: the
`daily_data_for_this_year_fips.height == 0` branch filled only the 5 base
metrics, omitting Rx5day/CDD/R-metrics that every other branch filled. Polars
null-filled the gap so it wasn't broken, just inconsistent — now uniform.

### Verification

Can't execute the script (needs the poetry env and a live S3 fetch), so:

- `python -m py_compile` clean; no `year_to_aggregate` reference remains outside
  loops that actually bind it; all four paste-seam comments gone.
- Replayed `main()`'s year-assignment control flow in plain Python against the
  real `df_yield_climdiv_2025.csv`, both buggy and fixed. The buggy replay
  reproduces the on-disk file **exactly** — 55,814 joined rows, 473 duplicate
  keys, worst case `('17001', 2024)` 23×, zero distinct pre-1951 years. The
  fixed replay gives 45,408 rows, 0 duplicates, all 22 pre-1951 years present.
  Matching the real artifact's numbers confirms the diagnosis, not just the fix.
- Helper's key set compared against the `aggs_schema` dict: 22 vs 22, no
  difference either direction.
- Scratch scripts discarded.

### Archived `get_data_importer_with_bins.py`

Confirmed unused before moving, not assumed: nothing outside `CHANGELOG.md`
references `with_bins` or `df_final_importer_2025_bins.csv`, `04_compare_validate`
never touches it, and `config.py`'s `TEMP_BINS_MIN`/`TEMP_BINS_MAX`/
`TEMP_BINS_INTERVAL_MINUTES` are read by that script alone.

Moved to `OLD/old_content_noaa_nclimgrid_importer/`, which already holds the
superseded importer generations (`get_data.py`, `get_data_v2.py`) — chosen over
a new local `03b/old/` so all archived importer drivers stay in one place:

- `get_data_importer_with_bins.py`
- `created_dfs_step_final/df_final_importer_2025_bins.csv` (27 MB) — moved with
  its script, so `created_dfs_step_final/` keeps meaning "current pipeline
  outputs" (now 2 files: `_paper` and `_soy`).

Left `config.py`'s `TEMP_BINS_*` constants in place — harmless, and the archived
script still needs them if it's ever resurrected.

### Still open in `03b` (surveyed, not done this pass)

- `analyze_weekly_dry_days.py:204` — `os.path.dirname(__file__) if '__file__' in
  locals() else os.getcwd()`. Inside a function `__file__` is a module global and
  never in `locals()`, so this **always** takes the `os.getcwd()` branch and the
  output lands relative to the launch directory.
- Both remaining drivers plus `analyze_weekly_dry_days.py` are still pinned to
  stale step-2 inputs with no crop switch (`get_data_importer.py` →
  `df_yield_climdiv_soy.csv`; `_paper.py` and `analyze_weekly_dry_days.py` →
  `df_yield_climdiv_2025.csv`). Note the `2025` in the output names means "the
  2025-vintage extract", not the season — misleading under the new data.
- Same CWD-dependent path bugs fixed in `03a` on 2026-08-17:
  `sys.path.append(os.path.abspath(".."))`, bare-relative `path_df`, and
  bare-relative output paths (`03b` doesn't even use `script_dir` for output).
- Duplication across the remaining stage-3 scripts, measured by hashing function
  bodies: `load_and_prepare_yield_data` identical in all 5 (38L),
  `aggregate_weather_to_yearly` identical in base + `_paper` + `03a` (20L),
  `calculate_daily_gdd`/`_kdd`/`_chd` identical across the same (18L total).
  **266 lines are byte-identical redundant copies** — mechanically extractable,
  no behavioral decision needed. `fetch_and_clean_daily_weather_batched` and
  `_calculate_seasonal_aggregates` are diverged on purpose. Still deferred to the
  joint `03a`+`03b` consolidation task.
- `analyze_weekly_dry_days.py:16` imports two functions straight out of
  `get_data_importer_paper.py` — driver importing driver; the shared module from
  that consolidation fixes it as a side effect.
- Deprecated polars calls that still work on the locked 0.20.31 but would break
  on a 1.x upgrade: `cumsum` and `pl.count()` in `_paper.py`, `pivot(columns=)`
  in `analyze_weekly_dry_days.py`.
- `analyze_weekly_dry_days.py` has its header comment duplicated on lines 1–2.

---

## 2026-08-17 — `03a_weather_nclimgrid_local`: crop-parameterized both scripts

First folder of the "repoint downstream stages at the new `_paper` step-2
outputs" work. Scope decided explicitly: config/parameterization + the path
robustness fixes below. The duplicated loader functions were **deliberately
left alone** — see "Deferred" at the end of this entry.

### Why: both scripts were pinned to the stale corn file

`get_data_local.py` and `get_compare_months_local.py` both hardcoded
`input_csv_filename = "df_yield_climdiv.csv"` — the pre-2025-season corn output
from step 2 (dated May 2025). Neither had any crop switch at all, so running
the local weather stage for soy was impossible without hand-editing, which is
the "soy parity for the weather stage is still open" item flagged on 2026-08-11.

### Added a config block to both scripts

Same role and shape as the config cell in
`02_weather_climdiv_crosswalk/merge_yield_monthly_weather.ipynb`, placed at
module level right after `import config` so it's the first thing in the file:

```python
crop = "corn"  # "corn" or "soy" — must match whichever pair is uncommented below

input_csv_filename  = "df_yield_climdiv_corn_paper.csv"   #"df_yield_climdiv_soy_paper.csv"
output_csv_filename = "df_final_local_corn_paper.csv"     #"df_final_local_soy_paper.csv"
```

Carries the same `other_crop` safety check as step 2 — raises `ValueError` at
import time if only some of the corn/soy lines got toggled, rather than
silently writing a "corn" output built from soy data. `main()` now reads these
instead of defining its own hardcoded strings.

Output naming follows the `_paper` convention adopted at step 2 (decided
2026-08-17, over a bare `_corn`/`_soy` suffix and over keeping one fixed
filename that each run overwrites — holding corn and soy side by side is what
`04_compare_validate` needs):

- `created_dfs_step_final/df_final_local_{corn,soy}_paper.csv`
- `created_dfs_monthly_comparison/df_compare_months_local_{corn,soy}_paper.csv`

Corn is the currently active (uncommented) branch in both scripts. The older
crop-less outputs (`df_final_local.csv`, `df_compare_months_local.csv`) are
left in place as earlier runs, same as everywhere else.

### ⚠️ Found: local Parquet data stops at 2024, so 2025 will come out NaN

`extracted_noaa_nclimgrid_data_local/` holds exactly 74 year folders,
1951–2024. `scan_end_year` is derived from the input CSV's max year, which is
now **2025** with the new yield data — so there is no 2025 daily data to
aggregate and every 2025 county-year would get NaN weather. The pre-existing
`YearFolderNotFound` logging does technically fire, but it's per-FIPS and
buried in an end-of-run summary that reads
`"FIPS 17001 (or batch containing it): 12 unique file/path errors reported"` —
easy to scroll past, and the script still exits successfully having written a
quietly-incomplete CSV. Same failure shape as the step-2 missing-value check
that silently reported "no missing values" (fixed 2026-08-12).

Added a loud upfront warning in both scripts, right after `scan_end_year` is
computed, listing the missing year folders by number before any processing
starts. Verified against the real folder: reports exactly `[2025]` for a
1951–2025 scan, and stays silent for 1951–2024 (no false alarms on historical
runs).

**This is a data problem, not a code problem** — `03a` cannot produce complete
2025 results until 2025 daily Parquet files are downloaded into
`extracted_noaa_nclimgrid_data_local/2025/`. `03b` (the S3 importer) is not
affected, since it streams daily data rather than reading local files.

### Path robustness fixes (both scripts)

- `sys.path.append(os.path.abspath(".."))` → resolved from `__file__` instead.
  The old form depended on the current working directory, so `import config`
  only worked when the script was launched from inside `03a`; running
  `python 03a_weather_nclimgrid_local/get_data_local.py` from the repo root
  raised `ModuleNotFoundError`.
- The step-2 input path was a bare relative string
  (`"../02_weather_climdiv_crosswalk/created_dfs_step2/"`) while the output path
  already used `script_dir` — inconsistent, and broken from any other working
  directory. Input now built from `script_dir` too.
- Both scripts now print the resolved crop, input path, and output path at the
  start of `main()`.

### Dead code

- `get_data_local.py`: commented out the unused
  `from polars.exceptions import ColumnNotFoundError` import, matching how
  `get_compare_months_local.py` already handled the same unused import.

### Verification

Can't run either script end to end — the `_paper` step-2 inputs don't exist yet
(step 2 hasn't been run for the new data), and the full run needs the poetry
env. Verified what could be verified:

- Both files byte-compile cleanly (`python -m py_compile`).
- Lifted the safety-check logic verbatim into a scratch script and ran it over
  8 cases — 4 correctly-toggled (corn/soy × seasonal/monthly) and 4 that should
  raise (crop=soy with corn input, crop=soy with corn output, crop=corn with
  both filenames soy, and the old crop-less `df_yield_climdiv.csv`). All 8
  behaved as expected. Scratch file discarded.
- Missing-year-folder detection checked against the real data folder (above).

### Deferred (not done in this pass)

- **The duplicated loaders.** `load_and_prepare_yield_data` is byte-identical
  across the two scripts, and `load_and_clean_local_daily_weather` is
  near-identical (~175 duplicated lines total). Decided 2026-08-17 to leave
  them, matching the scope call made for `03b`'s three near-duplicate drivers,
  and to handle `03a` and `03b` together in one dedicated consolidation task.
  Note for whoever does it: the two copies of
  `load_and_clean_local_daily_weather` are **not** equivalent —
  `get_data_local.py` keeps target weather columns even when they didn't end up
  `Float64`, while `get_compare_months_local.py` drops them. Unifying the two
  changes behavior for one of them, so that difference needs a decision, not a
  blind merge.
- `04_compare_validate` still reads the old crop-less `df_final_local.csv` /
  `df_compare_months_local.csv`. Not touched here — those files still exist so
  nothing is broken, and `04` gets its own pass in folder order.
- `old/` subfolder left as-is.

---

## 2026-08-12 — New 2025-season yield data added; verified against existing extracts

### New raw files added by user

- `01a_yield_data_corn/extracted_usda_corn_data/082026_corn_yield_all_states.csv`
  — added as a GUID-named file (`9229FD44-AEA5-3E35-8718-8C0D00FA3F4D_corn.csv`),
  renamed to follow the existing `MMYYYY_<crop>_yield_all_states.csv` convention.
- `01b_yield_data_soy/extracted_usda_soy_data/123C151A-B2C3-3B9A-B415-02B9D2A33092_soy.csv`
  — added by the user into the **corn** folder by mistake; moved to the correct
  soy folder. Not yet renamed to the `MMYYYY_` convention — pending the same
  decision as the corn file (soy rename not yet confirmed by user).

### Verification performed before doing anything else

Compared the new corn file against both existing corn extracts, and the new
soy file against the existing soy extract, at `Geo Level = COUNTY` /
main yield `Data Item` (the granularity the real pipeline notebooks use).
Method: pandas, matched on (State ANSI, County ANSI, Year), one-off script
run via system Python (not the poetry env) and discarded after use.

- **Corn new vs. `062025_corn_yield_all_states.csv`**: new file adds 311
  rows, all year 2025; 0 rows missing; of the 46,988 shared rows, **100%
  exact match** (0 value differences).
- **Corn new vs. `240917_corn_yield_data.csv`**: old file is narrower (4
  states, missing Indiana, years 1918–2023) so ~9,900 "new-only" rows are
  just coverage the old extract never had, not conflicts; 0 rows missing;
  of the 37,388 shared rows, **100% exact match**.
- **Soy new vs. `19112025_soy_yield_all_states.csv`**: new file adds 296
  rows, all year 2025; 0 rows missing; of the 37,934 shared rows, **100%
  exact match**.
- Conclusion: the new files are clean supersets (existing data + one new
  year), no revisions to historical values, safe to adopt as the current
  source.
- Note for next time this kind of comparison is needed: the raw USDA export
  has multiple `Geo Level` values (`COUNTY`, `AG DISTRICT`, `STATE`) sharing
  the same `State ANSI`/`Year` with a blank `County ANSI` — a first pass that
  keyed only on (State ANSI, County ANSI, Year) without filtering to
  `Geo Level == COUNTY` collapsed all the non-county rows onto colliding keys
  and produced a lot of spurious "large differences" that were actually just
  unrelated district/state rows joined against each other. Filtering to
  `Geo Level == COUNTY` (and dropping null County ANSI) fixed it.

### Decision: which format notebook to use going forward

`format_corn_yield_data_2025.ipynb` (not `format_corn_yield_data.ipynb`) is
the right template for the new corn file, and by extension
`format_soy_yield_data.ipynb` for the new soy file — both already read the
"all states" export shape (5 states, single yield `Data Item`, same columns)
that the new files match exactly. `format_corn_yield_data.ipynb` reads the
older/narrower `240917` shape (extra irrigated/non-irrigated items, 4
states) and isn't the right fit.

### Not yet done (next steps discussed, not executed)

- Rename the new soy file to `082026_soy_yield_all_states.csv` for
  consistency with corn — asked, not yet confirmed.
- Point `format_corn_yield_data_2025.ipynb` and `format_soy_yield_data.ipynb`
  at the new `082026_*` files and re-run them.
- Re-run the downstream pipeline (climdiv crosswalk → weather merge) to
  produce a new final df incorporating the 2025 season for both crops.

## Naming convention decisions (apply going forward)

- Dataframe-output subfolders are named `created_dfs_stepN` where `N` matches the
  stage's own numeric prefix (01→step1, 02→step2, 03→step3, ...) — decided
  2026-08-11.
- **Exception:** a folder holding the pipeline's true final output is named
  `created_dfs_step_final`, not `created_dfs_stepN` — decided 2026-08-11, first
  applied to `03a_weather_nclimgrid_local/created_dfs_step_final/`
  (`df_final_local.csv`). `03b_weather_nclimgrid_importer` will get its own
  `created_dfs_step_final/` too when we clean it up, since local and importer
  are parallel siblings that each produce their own final merged dataset.
- **Side artifacts that aren't part of the main pipeline flow** (validation/QA
  outputs consumed only by `04_compare_validate`, not by the next pipeline
  stage) get their own descriptively-named folder rather than being lumped into
  `created_dfs_step_final`. First instance:
  `03a_weather_nclimgrid_local/created_dfs_monthly_comparison/` — decided
  2026-08-11 (rejected the generic term "qa" as unclear).
- Raw/external input subfolders are named `extracted_<source>_<data>_data`, with
  a trailing method qualifier when a stage has parallel siblings (e.g.
  `extracted_noaa_nclimgrid_data_local` for `03a`, vs. plain
  `extracted_usda_corn_data` for `01a` which has no sibling ambiguity) — no
  special character prefix (considered and rejected a `$` marker: shell-quoting
  friction, sorts before the numbered stage folders instead of with them, and is
  redundant with what the descriptive names already communicate).

---

## 2026-08-12 — `02_weather_climdiv_crosswalk`: fresh climdiv data + parameterized merge notebook

### Why: stale weather data would have corrupted 2025 aggregates

Before touching anything, checked whether the merge notebook would actually
work correctly against the new 2025-season corn/soy yield data. It would
not have: the climdiv files on disk (`climdiv-tmaxcy/pcpncy-v1.0.0-20250506`)
only have real data through April 2025 — checked a sample row directly:
`17001272025  28.00  29.70  43.20  54.40 -99.90 -99.90 -99.90 -99.90 -99.90
-99.90 -99.90 -99.90` (May–Dec are the `-99.90` missing-value placeholder).
Since the growing season window is April–September, computing 2025
aggregates from this file would have averaged/summed in those placeholders
and produced badly wrong (not just missing) numbers for 2025.

Also found that the notebook's own "check for missing data" cell can't catch
this: it filters for columns ending in `_tmax`/`_pcpn`, but the parsed
dataframe's actual columns are just `Jan`...`Dec` — so the filter always
matches zero columns and the check trivially reports "no missing values"
regardless of what's actually in the data. **Not fixed yet** — flagged for
the "still to do before running" list.

### Re-scraped climdiv data

- User supplied the correct current URLs directly:
  `https://www.ncei.noaa.gov/monitoring-content/data/us/climdiv/monthly/current/climdiv-tmaxcy-v1.0.0-20260806`
  and the `pcpncy` equivalent (last modified 2026-08-06 per NOAA).
- Updated `webscraper.py`'s hardcoded URLs to these (was `.../pub/data/cirs/climdiv/climdiv-*-v1.0.0-20250506`,
  a different NOAA path than the one the user gave — used the user-supplied
  URLs as authoritative).
- Downloaded the two new files directly via PowerShell `Invoke-WebRequest`
  into `extracted_noaa_climdiv_data/` (the system Python doesn't have
  `requests` installed — that's fine, `webscraper.py` is meant to run inside
  the poetry env, which does; used PowerShell for this one-off fetch instead
  of installing packages system-wide).
- Old `20250506` and `240924`-dated files left in place (consistent with
  keeping historical raw pulls, same as everywhere else in `extracted_*`
  folders) — nothing currently references them by the new filenames, so nothing broke.

### Parameterized `merge_yield_monthly_weather.ipynb`

Added a config cell right after "Load Libraries and packages" (matching the
pattern used in the format notebooks), consolidating what used to be two
separate hand-commented toggles (one near the yield-read cell, one near the
save cell) into one place:
```python
path_corn = "../01b_yield_data_soy/created_dfs_step1/"      #"../01a_yield_data_corn/created_dfs_step1/"
filename = "df_soy_yield_2026.csv"                            #"df_corn_yield_2026.csv"
save_file = "created_dfs_step2/df_yield_climdiv_soy_paper.csv" #"created_dfs_step2/df_yield_climdiv_corn_paper.csv"
```
The read cell and save cell now just reference these variables instead of
redefining them. Also updated the climdiv-file-read cell to point at the new
`20260806` files instead of the stale `20250506` ones. Soy is the currently
active (uncommented) branch, matching how it was before this change.

### Follow-up refinements (same session)

- Renamed `path_corn` → `path_df_step1` in the config cell — it was a
  misleading name once the notebook covers both crops, not just corn.
- Added a safety check right in the config cell: derives `other_crop` from
  the `crop` variable and verifies none of `path_df_step1`/`filename`/
  `save_file` contain the *other* crop's name (and all three do contain the
  selected one) — raises `ValueError` immediately if only some of the three
  corn/soy comment-toggles got swapped, instead of silently producing e.g. a
  "corn" output actually built from soy data.
- Fixed the missing-data-check bug (`aee6404f`): now checks the real `Jan`–
  `Dec` columns (converted to numeric first, since they're still strings at
  this point in the notebook — parsed straight off the raw text files) against
  the correct sentinels (`-99.90` for tmax, `-9.99` for pcpn). Verified against
  real data: correctly flags 3,104 missing cells/month for May–Dec against the
  old `20250506` file, and correctly reports **zero** missing values anywhere
  in 2025 against the new `20260806` file (the only remaining gaps are in
  partial-year 2026, as expected).

---

- `01a_yield_data_corn/format_corn_yield_data.ipynb` (the one reading the
  older/narrower `240917_corn_yield_data.csv` extract — 4 states, missing
  Indiana, years 1918–2023) moved to `OLD/format_corn_yield_data.ipynb`.
  Superseded by the all-states notebook below; kept for reference rather than
  deleted, consistent with everything else in `OLD/`. Checked repo-wide
  first — nothing referenced this filename outside this changelog.
- `01a_yield_data_corn/format_corn_yield_data_2025.ipynb` →
  `format_corn_yield_data_paper.ipynb`.
- `01b_yield_data_soy/format_soy_yield_data.ipynb` →
  `format_soy_yield_data_paper.ipynb` (matching naming convention applied to
  corn).
- `01a_yield_data_corn/` and `01b_yield_data_soy/` each now contain exactly
  one active format notebook, both named `format_<crop>_yield_data_paper.ipynb`.

---

## 2026-08-12 — Wired up 2025-season corn/soy data; verified end-to-end

### Parameterized both format notebooks

`format_corn_yield_data_2025.ipynb` and `format_soy_yield_data.ipynb` both
gained a config cell right after "Load Libraries and packages":
```python
file_name = "<extract to read>"   # from extracted_usda_<crop>_data/
save_name = "<output filename>"   # written to created_dfs_step1/
```
The read cell and the final `to_csv` save cell now reference these variables
instead of hardcoded strings. Both notebooks' "check for missing data" cell
also now computes its reference year range from `highest_min_year`/
`lowest_max_year` (already computed two cells earlier) instead of stale
hardcoded literals — see previous entries for why that mattered.

### Switched both notebooks to the new files

- Corn: `file_name = "082026_corn_yield_all_states.csv"`,
  `save_name = "df_corn_yield_2026.csv"` (user-set directly in the IDE).
- Soy: renamed the new soy raw file from its GUID name
  (`123C151A-...soy.csv`) to `082026_soy_yield_all_states.csv` to match the
  corn convention, then set `file_name`/`save_name` in
  `format_soy_yield_data.ipynb` to match: `082026_soy_yield_all_states.csv` /
  `df_soy_yield_2026.csv`.

### Downstream reference fixed (this one was live, not commented out)

`02_weather_climdiv_crosswalk/merge_yield_monthly_weather.ipynb`'s active
corn/soy toggle read `df_soy_yield.csv`, which no longer gets created now
that soy's `save_name` changed to `df_soy_yield_2026.csv`. Updated the active
filename to match, and updated the commented-out corn alternative from
`df_corn_yield_2025.csv` to `df_corn_yield_2026.csv` for consistency (not
live, but kept in sync so it's correct if re-enabled).

### Verification performed (can't execute .ipynb directly — no kernel access)

Since there's no way to actually run a Jupyter cell from here, verified
correctness by re-implementing each notebook's exact cell-by-cell logic as a
disposable Python script, running it against the real new files, and
inspecting the output before cleaning up the scratch files:

- Checked a real correctness risk first: neither notebook specifies a
  `dtype` on `pd.read_csv`, so column types are pandas-inferred. Confirmed
  `Ag District Code` loads as clean `int64` in all three "all states" files
  checked (`062025`, `082026` corn, `19112025` soy) — no float corruption, so
  `str(x)[::-1].zfill(2)` reverses correctly. Also confirmed all three files
  are `Geo Level == COUNTY` only (no state/district rows), so the
  `district_code != '99'` filter is a harmless no-op for this file shape —
  the real work of dropping non-county rows is done by the
  `dropna(subset=["county_ansi"])` step right after it.
- Corn dry run (`082026`): 44,145 final rows, year range now correctly
  1929–2025 (was capped at 2024), 0 NaN in `value`, sane value range
  (0.4–253.6 bu/acre), 330/471 counties flagged with some missing
  county-years (genuine USDA reporting gaps, not a processing bug).
- Soy dry run (`082026`): 28,198 final rows, year range now 1960–2025, 0 NaN
  in `value`, sane range (5.0–80.4 bu/acre), 357/467 counties flagged with
  some missing county-years.
- Both notebooks' logic confirmed correct end-to-end against the real new
  files before the user runs them for real.

---

## 2026-08-12 — Removed OneDrive sync-artifact folders

- Found `compare_scripts/` and `_soy_yield_data/` still present at the repo
  root alongside their renamed replacements (`04_compare_validate/` and
  `01b_yield_data_soy/`). Investigated before touching anything: both were
  empty — `_soy_yield_data/` had 0 items; `compare_scripts/` had two empty
  subfolder shells (`rudimentary_comparison/`, `verifying_compare_months_local/`)
  with 0 files inside either. All real content lived in the correctly-named
  folders, confirmed via item counts (`01b_yield_data_soy/` had 5 items,
  `04_compare_validate/` had 11).
- Checked all six original pre-rename folder names — only these two had
  leftover shells; the other four (`_corn_yield_data`, `_noaa_climdiv_local`,
  `_noaa_nclimgrid_local`, `_noaa_nclimgrid_importer`) were cleanly gone.
- Root cause: OneDrive syncs folder renames as delete-old + create-new for
  nested content in some cases, and a script-driven rename (via
  `Rename-Item`, not through Explorer) can outrun OneDrive's reconciliation,
  leaving empty directory ghosts at the old path.
- Deleted both empty folders after confirming zero content.

---

## 2026-08-11 — `03b_weather_nclimgrid_importer/` internal cleanup (file-org pass)

Scope decided explicitly: file organization only. The bigger issue in this
folder — three near-duplicate driver scripts (`get_data_importer.py` /
`_paper.py` / `_with_bins.py`, ~90% identical code) and `analyze_weekly_dry_days.py`
importing functions directly from `get_data_importer_paper.py` — was **not**
addressed this pass; deferred to a dedicated consolidation task.

### Fixed: dead code

- Removed ~90 lines of fully-commented-out old code (a superseded version of
  `_calculate_seasonal_aggregates`, including a debugging block) from the
  bottom of `get_data_importer_with_bins.py`.

### Renamed for accuracy

- `get_ccd_dist.py` → `analyze_weekly_dry_days.py` (its own internal header
  already called itself `analyze_weekly_dry_days.py`; the old name didn't
  match what the script computes — weekly dry-day counts, not a CDD
  distribution). Checked repo-wide first: nothing referenced the old filename.
- `test_plot.ipynb` → `plot_dry_day_distributions.ipynb`. Not a test — it
  contains two full standalone plotting scripts
  (`plot_weekly_dry_day_distribution.py`, `plot_yearly_total_distribution.py`)
  pasted in as cells.

### Added subfolders

- `created_dfs_step_final/` — the three genuine final pipeline outputs:
  `df_final_importer_soy.csv`, `df_final_importer_2025_paper.csv`,
  `df_final_importer_2025_bins.csv`.
- `created_dfs_weekly_dry_day_analysis/` — a self-contained bundle that is
  *not* part of the main pipeline flow: `df_weekly_dry_days_paper.csv`
  (produced by `analyze_weekly_dry_days.py`) plus the `weekly_dry_day_plots/`
  and `yearly_total_dry_day_plots/` folders it feeds
  (`plot_dry_day_distributions.ipynb`). This is paper-specific supplementary
  analysis, kept separate from `created_dfs_step_final/` for the same reason
  as `03a`'s `created_dfs_monthly_comparison/` — different role, different
  folder.
- No `extracted_*` folder needed here — this method streams daily data from
  S3 via `nclimgrid_importer.py`, nothing raw is stored locally. That's the
  point of the importer method vs. the local-parquet method in `03a`.

### Scripts and downstream references updated to match

- `get_data_importer.py`, `get_data_importer_paper.py`,
  `get_data_importer_with_bins.py`, `analyze_weekly_dry_days.py` — all write
  to their new subfolder locations.
- `plot_dry_day_distributions.ipynb` — both cells' input CSV path and plot
  output directories updated to `created_dfs_weekly_dry_day_analysis/...`.
- `04_compare_validate/compare_local_vs_importer.py`,
  `04_compare_validate/compare_GS_climdiv_nclimgrid.ipynb` — updated to read
  from `created_dfs_step_final/`. Left the actual filename referenced
  (`df_final_importer.csv`) untouched — it doesn't match any of the three real
  output filenames and was already a stale/broken reference before this pass
  (noted in the 02_weather_climdiv_crosswalk entry above); fixing which file it
  *should* point to is part of the deferred consolidation work, not a path fix.

### Still open

- The three-way script duplication and the `analyze_weekly_dry_days.py` →
  `get_data_importer_paper.py` cross-file import — deferred, see scope note
  above.
- `nclimgrid_importer.py` (the shared S3-loading library) and `test_importer.py`
  (a small manual smoke test for it) left untouched at the folder root —
  correct as-is, since `nclimgrid_importer.py` must stay importable by every
  driver script.

---

## 2026-08-11 — Repo-wide `__pycache__` cleanup

- `.gitignore` only excluded `.DS_Store` before this; added `__pycache__/` and
  `*.pyc`.
- Deleted the three `__pycache__/` directories that existed (repo root,
  `03a_weather_nclimgrid_local/`, `03b_weather_nclimgrid_importer/`) — pure
  Python bytecode cache, regenerates automatically, safe to remove.

---

## 2026-08-11 — `03a_weather_nclimgrid_local/` internal cleanup

### Fixed: missing `.py` extension

- `get_compare_months_local` → `get_compare_months_local.py`. The file had no
  extension at all (its internal header comment even names itself
  `process_local_ag_weather_monthly.py`), so it couldn't be imported and didn't
  get standard tooling support. Checked repo-wide first — nothing referenced it
  by the exact extensionless filename, so the rename was safe.

### Added subfolders

- `extracted_noaa_nclimgrid_data_local/` — all 74 raw daily-parquet year
  folders (1951–2024), moved as whole directories (fast — no per-file
  operations needed for a same-volume folder move).
- `created_dfs_step_final/` — `df_final_local.csv`, the actual final output of
  this pipeline branch (seasonal GDD/KDD/PREC/CHD merged with yield).
- `created_dfs_monthly_comparison/` — `df_compare_months_local.csv`. This is
  *not* part of the main pipeline data flow: it's monthly (not seasonal)
  aggregates produced by a separate script, used only by
  `04_compare_validate` notebooks to (a) cross-check against climdiv's monthly
  data and (b) cross-validate against the `_v2` reimplementation of the same
  script. Kept deliberately separate from `created_dfs_step_final/` so that
  folder's meaning stays unambiguous.

### Scripts and downstream references updated to match

- `get_data_local.py`: `local_data_base_path` now points at
  `extracted_noaa_nclimgrid_data_local/`; output now written to
  `created_dfs_step_final/df_final_local.csv`.
- `get_compare_months_local.py`: same `local_data_base_path` fix; output now
  written to `created_dfs_monthly_comparison/df_compare_months_local.csv`.
- `04_compare_validate/compare_local_vs_importer.py`,
  `04_compare_validate/compare_MONTHS_climdiv_nclimgrid.ipynb`,
  `04_compare_validate/verifying_compare_months_local/verification_df.ipynb` —
  all updated to read from the new subfolder locations.
- `old/all_months_check.py` — updated its hardcoded absolute
  `DEFAULT_BASE_PATH` to point at `extracted_noaa_nclimgrid_data_local/`
  (archived script, but a one-line fix while already touching this file).

### Still open (not addressed this pass)

- `__pycache__/config.cpython-312.pyc` exists in this folder (and at least the
  repo root); `.gitignore` doesn't exclude `__pycache__/` anywhere. Flagged,
  not yet fixed — pending a decision on whether to do it now or as a separate
  repo-wide pass.
- `old/` subfolder (`create_folders.py`, `move_files.py`, `read_202307.py`,
  `stats_parquet_data.py`, `all_months_check.py`) left as-is — already
  reasonably archived.

---

## 2026-08-11 — `02_weather_climdiv_crosswalk/` internal cleanup

### Added subfolders

- `extracted_noaa_climdiv_data/` — raw monthly NOAA climdiv extracts plus the
  static crosswalk reference table: `240924 climdiv-pcpncy-v1.0.0-20240906.txt`,
  `240924 climdiv-tmaxcy-v1.0.0-20240906.txt`,
  `climdiv-pcpncy-v1.0.0-20250506.txt`, `climdiv-tmaxcy-v1.0.0-20250506.txt`,
  `county-to-climdivs.txt`. (`county-to-climdivs.txt` isn't monthly like the
  others, but it's still raw external input rather than something this
  notebook creates, so it went in `extracted_` too rather than sitting loose at
  the folder root.)
- `created_dfs_step2/` — notebook output: `df_yield_climdiv.csv`,
  `df_yield_climdiv_2025.csv`, `df_yield_climdiv_soy.csv`.

### Notebook and script updated to match

- `merge_yield_monthly_weather.ipynb`: now reads all climdiv/crosswalk inputs
  from `extracted_noaa_climdiv_data/` and writes its output CSV to
  `created_dfs_step2/`.
- `webscraper.py`: now saves downloaded files into `extracted_noaa_climdiv_data/`
  instead of the folder root.

### Bug found and fixed while updating webscraper.py

- `webscraper.py` built its output filename from `os.path.basename(url)`, and
  the source URLs have no file extension — so it was saving files *without* a
  `.txt` extension. Every file actually on disk (both the 2024 and 2025
  vintages) has a `.txt` extension, meaning either they were renamed by hand
  after downloading, or the script was edited since the last real run. Fixed
  `webscraper.py` to append `.txt` when saving, matching what's actually on
  disk and what the notebooks expect.
- This same extension mismatch had leaked into
  `04_compare_validate/rudimentary_comparison/basic_checking2025.ipynb`, which
  read `"climdiv-tmaxcy-v1.0.0-20250506"` / `"...-pcpncy..."` without `.txt` —
  it would have failed with `FileNotFoundError` against the real file. Fixed
  alongside the folder-path update for that notebook.

### Renamed (naming convention applied retroactively)

- `01a_yield_data_corn/created_dfs/` → `created_dfs_step1/`
- `01b_yield_data_soy/created_dfs/` → `created_dfs_step1/`
- Updated the three format notebooks' write paths
  (`format_corn_yield_data.ipynb`, `format_corn_yield_data_2025.ipynb`,
  `format_soy_yield_data.ipynb`) and `merge_yield_monthly_weather.ipynb`'s read
  path (both the active soy branch and the commented-out corn alternative) to
  match.

### Downstream path fixes (all references to files that moved)

- `03a_weather_nclimgrid_local/get_data_local.py`,
  `03a_weather_nclimgrid_local/get_compare_months_local`,
  `03b_weather_nclimgrid_importer/get_data_importer.py`,
  `03b_weather_nclimgrid_importer/get_data_importer_paper.py`,
  `03b_weather_nclimgrid_importer/get_data_importer_with_bins.py`,
  `03b_weather_nclimgrid_importer/get_ccd_dist.py`,
  `04_compare_validate/verifying_compare_months_local/get_compare_months_local_v2.py`
  — all updated to read `df_yield_climdiv*.csv` from
  `02_weather_climdiv_crosswalk/created_dfs_step2/`.
- `04_compare_validate/compare_GS_climdiv_nclimgrid.ipynb` — updated to read
  `df_yield_climdiv.csv` from `created_dfs_step2/`.
- `04_compare_validate/compare_MONTHS_climdiv_nclimgrid.ipynb`,
  `04_compare_validate/rudimentary_comparison/basic_checking2024.ipynb`,
  `04_compare_validate/rudimentary_comparison/basic_checking2025.ipynb` —
  updated to read the raw climdiv txt files and `county-to-climdivs.txt` from
  `extracted_noaa_climdiv_data/`.

---

## 2026-08-11 — `01b_yield_data_soy/` internal cleanup

### Added subfolders

- `01b_yield_data_soy/extracted_usda_soy_data/` — raw USDA export:
  `19112025_soy_yield_all_states.csv`.
- `01b_yield_data_soy/created_dfs/` — notebook output: `df_soy_yield.csv`.

### Notebook updated to match

- `format_soy_yield_data.ipynb`: now reads
  `extracted_usda_soy_data/19112025_soy_yield_all_states.csv` and writes
  `created_dfs/df_soy_yield.csv`.

### Downstream fix (this one mattered — was live, not commented out)

- `02_weather_climdiv_crosswalk/merge_yield_monthly_weather.ipynb`: the
  **active** corn/soy toggle currently points at soy
  (`path_corn = "../01b_yield_data_soy/"`, `filename = "df_soy_yield.csv"`).
  Moving `df_soy_yield.csv` into `created_dfs/` would have silently broken this
  notebook. Updated the active path to
  `"../01b_yield_data_soy/created_dfs/"` and kept the commented-out corn
  alternative in sync (`"../01a_yield_data_corn/created_dfs/"`).

### Mistake found, not yet resolved

- The "check for missing data" cell in `format_soy_yield_data.ipynb` (and,
  found on closer inspection, also in **both** corn notebooks
  `format_corn_yield_data.ipynb` and `format_corn_yield_data_2025.ipynb`)
  hardcodes the reference year range as literal `1926` and `2023`
  (`range(1926, 2023 + 1)`, `print("Yield data covers years 1926 to 2023")`)
  instead of using the `highest_min_year` / `lowest_max_year` variables computed
  one cell earlier in the same notebook. For soy this hardcoded range doesn't
  match the actual computed range (1960–2024), so the missing-data report is
  checking against the wrong reference years — it would flag 1926–1959 as
  spuriously "missing" and never check 2024 at all. Appears to be old
  boilerplate from an earlier dataset that was copy-pasted into every version of
  this notebook without updating the literals. Not fixed yet — flagged for a
  decision since it changes notebook behavior, not just file layout.

---

## 2026-08-11 — `01a_yield_data_corn/` internal cleanup

### Added subfolders

- `01a_yield_data_corn/extracted_usda_corn_data/` — holds the raw USDA NASS
  exports, unmodified as downloaded: `062025_corn_yield_all_states.csv`,
  `062025_corn_yield_Indiana.csv`, `240917_corn_yield_data.csv`.
- `01a_yield_data_corn/created_dfs/` — holds the notebook-generated outputs:
  `df_corn_yield.csv`, `df_corn_yield_2025.csv`.

### Notebooks updated to match

- `format_corn_yield_data.ipynb`: now reads
  `extracted_usda_corn_data/240917_corn_yield_data.csv` and writes
  `created_dfs/df_corn_yield.csv`.
- `format_corn_yield_data indiana.ipynb`: now reads
  `extracted_usda_corn_data/062025_corn_yield_all_states.csv` and writes
  `created_dfs/df_corn_yield_2025.csv`.
- `02_weather_climdiv_crosswalk/merge_yield_monthly_weather.ipynb`: the
  commented-out corn alternative (`path_corn`/`filename`, currently inactive —
  the active branch reads soy) updated from `"../01a_yield_data_corn/"` to
  `"../01a_yield_data_corn/created_dfs/"` so it stays correct if re-enabled.

### Mistakes found and resolved

- `format_corn_yield_data indiana.ipynb` was misnamed: despite the name, it
  reads `062025_corn_yield_all_states.csv` (all states, not Indiana-only) and
  produces `df_corn_yield_2025.csv`. Likely started as an Indiana-specific test
  and was repurposed without renaming. Renamed to
  `format_corn_yield_data_2025.ipynb`. No other file referenced the old
  filename (checked repo-wide before renaming).

### Flagged, deliberately left as-is

- `062025_corn_yield_Indiana.csv` (in `extracted_usda_corn_data/`) is not read
  by any notebook — unused raw data. Kept in place per decision on 2026-08-11
  in case it's wanted later.
- `format_corn_yield_data.ipynb` and `format_corn_yield_data_2025.ipynb` are
  near-duplicate notebooks (same logic, different input extract / output year).
  Decision on 2026-08-11: leave as two separate notebooks for now; revisit
  consolidating into one parameterized notebook after the rest of the pipeline
  is cleaned up.

---

## 2026-08-11 — Folder rename to numbered pipeline stages

### Renamed folders

| Old name | New name |
|---|---|
| `_corn_yield_data/` | `01a_yield_data_corn/` |
| `_soy_yield_data/` | `01b_yield_data_soy/` |
| `_noaa_climdiv_local/` | `02_weather_climdiv_crosswalk/` |
| `_noaa_nclimgrid_local/` | `03a_weather_nclimgrid_local/` |
| `_noaa_nclimgrid_importer/` | `03b_weather_nclimgrid_importer/` |
| `compare_scripts/` | `04_compare_validate/` |

Left untouched: `docs/`, `OLD/`, and root-level `config.py` / `Makefile` /
`pyproject.toml` / `poetry.lock`.

Naming convention: 2-digit numeric prefix per real pipeline stage; `a`/`b` suffixes
for parallel siblings within a stage (same step, different crop or different
implementation) rather than separate stage numbers.

### Removed

- `01b_yield_data_soy/`: deleted 7 files that were byte-identical duplicates of
  files already in `01a_yield_data_corn/` (leftover from copy-pasting the corn
  folder to start the soy one): `062025_corn_yield_all_states.csv`,
  `062025_corn_yield_Indiana.csv`, `240917_corn_yield_data.csv`,
  `df_corn_yield.csv`, `df_corn_yield_2025.csv`,
  `format_corn_yield_data indiana.ipynb`, `format_corn_yield_data.ipynb`.
  Verified identical via SHA-256 before deleting.

### Path references updated

Every hardcoded relative path pointing at an old folder name was updated to the
new name, in:

- `03a_weather_nclimgrid_local/get_data_local.py`
- `03a_weather_nclimgrid_local/get_compare_months_local`
- `03a_weather_nclimgrid_local/old/all_months_check.py` (absolute path string)
- `03b_weather_nclimgrid_importer/get_data_importer.py`
- `03b_weather_nclimgrid_importer/get_data_importer_paper.py`
- `03b_weather_nclimgrid_importer/get_data_importer_with_bins.py`
- `03b_weather_nclimgrid_importer/get_ccd_dist.py`
- `04_compare_validate/compare_local_vs_importer.py`
- `04_compare_validate/verifying_compare_months_local/get_compare_months_local_v2.py`
- `02_weather_climdiv_crosswalk/merge_yield_monthly_weather.ipynb`
- `04_compare_validate/compare_GS_climdiv_nclimgrid.ipynb`
- `04_compare_validate/compare_MONTHS_climdiv_nclimgrid.ipynb`
- `04_compare_validate/verifying_compare_months_local/verification_df.ipynb`
- `04_compare_validate/rudimentary_comparison/basic_checking2024.ipynb`
- `04_compare_validate/rudimentary_comparison/basic_checking2025.ipynb`

`config.py` imports (`sys.path.append(os.path.abspath(".."))`) are relative and
were not affected by the rename.

Cached notebook *outputs* (printed paths from a previous run, stored inside
`.ipynb` JSON) were left as-is — they're historical text, not live code, and will
be overwritten the next time those cells are re-run.

### Bug found and fixed while updating paths

`04_compare_validate/verifying_compare_months_local/verification_df.ipynb` lived
two folders deep (`04_compare_validate/verifying_compare_months_local/`) but its
`path_local` only used one `../`, so it was pointing at a nonexistent
`04_compare_validate/_noaa_nclimgrid_local/` and had never worked (confirmed by a
`FileNotFoundError` baked into the notebook's own cached output). Corrected to
`../../03a_weather_nclimgrid_local/` — the same depth its sibling script
`get_compare_months_local_v2.py` already used correctly.

### Known pre-existing issues, not touched in this pass

- `04_compare_validate/compare_GS_climdiv_nclimgrid.ipynb` reads
  `df_final_importer.csv` from `03b_weather_nclimgrid_importer/`, but no script
  in that folder currently produces a file with that exact name anymore (the
  current outputs are `df_final_importer_soy.csv`,
  `df_final_importer_2025_paper.csv`, `df_final_importer_2025_bins.csv`). This
  notebook was already stale before the rename; not fixed here since it's a
  content issue, not a path issue.
- `03b_weather_nclimgrid_importer/` still has four near-duplicate driver scripts
  (`get_data_importer.py`, `_paper.py`, `_with_bins.py`) and dead commented-out
  code in `_with_bins.py`. Flagged for consolidation when we clean up that
  folder directly.
- `03a_weather_nclimgrid_local/get_data_local.py` and the soy-vs-corn paper/bins
  scripts are still hardcoded to corn's filenames — soy parity for the weather
  stage is still open.
- `OLD/` and the stray `.DS_Store`/`__pycache__` folders were left alone; slated
  for archival or deletion when we reach general cleanup.
