from nclimgrid_importer import load_nclimgrid_data
import polars as pl
import pandas as pd
import seaborn as sbn

county_old = load_nclimgrid_data(
        start_date = '1951-01-01',
        end_date = '2000-01-01',
        spatial_scale = 'cty',
        scaled = True,
        states = ['OK', 'NC'],
        counties = ['Cleveland County', 'Buncombe County'],
        variables = ['TMIN', 'TMAX']
        )

county_new = load_nclimgrid_data(
        start_date = '2011-01-01',
        end_date = '2020-01-01',
        spatial_scale = 'cty',
        scaled = True,
        states = ['OK', 'NC'],
        counties = ['Cleveland County', 'Buncombe County'],
        variables = ['TMIN', 'TMAX']
        )


county_comparison = pl.concat(
        [
            pl.from_arrow(county_old).with_columns([pl.lit('Historical (1951-200)').alias('Period')]),
            pl.from_arrow(county_new).with_columns([pl.lit('Recent (2011-2020)').alias('Period')])
        ]
        )

county_plot_data = (
        county_comparison.lazy()
        .with_columns([
                pl.col('date').str.slice(0,2).alias('month'),
                pl.col('TMIN').cast(pl.Float64),
                pl.col('TMAX').cast(pl.Float64),
            ])
        .filter((pl.col('month').is_in(["11","12","01", "02"])))
        .with_columns([
            (pl.col('TMAX')-pl.col('TMIN')).alias('Diff')
            ])
       ).collect().to_pandas()

cp = sbn.displot(county_plot_data,
        x="Diff", hue="Period", common_norm = False, kind="kde", col = 'county', fill=True)

cp.savefig('./docs/docs/figures/county_compare.png')
