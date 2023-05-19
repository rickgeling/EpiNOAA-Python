from nclimgrid_importer import load_nclimgrid_data
import polars as pl
import pandas as pd
import seaborn as sbn

harris_old = load_nclimgrid_data(
        start_date = '1951-01-01',
        end_date = '2000-01-01',
        spatial_scale = 'cty',
        scaled = True,
        states = ['TX'],
        counties = ['Harris County'],
        variables = ['TMIN']
        )

harris_new = load_nclimgrid_data(
        start_date = '2011-01-01',
        end_date = '2020-01-01',
        spatial_scale = 'cty',
        scaled = True,
        states = ['TX'],
        counties = ['Harris County'],
        variables = ['TMIN']
        )


harris_comparison = pl.concat(
        [
            pl.from_arrow(harris_old).with_columns([pl.lit('Historical (1951-200)').alias('Period')]),
            pl.from_arrow(harris_new).with_columns([pl.lit('Recent (2011-2020)').alias('Period')])
        ]
        )

harris_plot_data = (
        harris_comparison.lazy()
        .with_columns([
                pl.col('date').str.slice(0,2).alias('month'),
                pl.col('TMIN').cast(pl.Float64)
            ])
        .filter((pl.col('month').is_in(["07","08","09"])))
        )

harris_plot = sbn.displot(
        harris_plot_data.collect().to_pandas(), 
        x="TMIN", hue="Period", common_norm = False, kind="kde", fill=True)

harris_plot.savefig('./docs/docs/figures/harris_county.png')






