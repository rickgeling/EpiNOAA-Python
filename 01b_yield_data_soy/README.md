# 01b: format soy yield data

Cleans the raw USDA county-level soy yield export into one row per county per year. Mirrors `01a_yield_data_corn`'s notebook exactly, same steps, same structure, one crop instead of the other. See that folder's README for the step-by-step detail, this one only covers what's different.

## config cell

```python
file_name = "082026_soy_yield_paper_states.csv"
save_name = "df_soy_yield_2026.csv"
```

## the Nebraska gap

Unlike corn, the 5 states don't agree on a start year. I checked the raw USDA export directly:

| state | earliest soy year |
|---|---|
| Illinois | 1927 |
| Iowa | 1927 |
| Indiana | 1937 |
| Minnesota | 1941 |
| Nebraska | 1960 |

Nebraska has no county-level soybean yield data before 1960 anywhere in USDA's records, as far as I can tell. Not a bug, not a download gap, I checked. Because the year-range check in this notebook only reports and flags rather than trims (see `01a`'s README), the other 4 states keep their full history, Nebraska just has fewer years than the rest, which is a real and intentional unbalanced panel rather than something forced to match.

To run this on a new export, or point it at the 100th-meridian branch's filtered file, same instructions as `01a`.
