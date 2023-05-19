import polars as pl
import pyarrow.parquet as pq
import s3fs
import pyarrow.dataset as ds
import pendulum
from typing import Optional, Union
from pyarrow import Table


def make_file_names(
        bucket: str,
        start_date: str,
        end_date: str,
        spatial_scale: str,
        scaled: bool,
        ) -> list[str]:

    scaled_text = 'scaled' if scaled else 'prelim'

    """ documentation here"""
    start_date = pendulum.from_format(start_date, 'YYYY-MM-DD') 
    end_date = pendulum.from_format(end_date, 'YYYY-MM-DD')

    file_names = [f"s3://{bucket}/EpiNOAA/parquet/{month.format('YYYYMM')}-{spatial_scale}-{scaled_text}.parquet" for month in pendulum.period(start_date, end_date).range('months')]

    return(file_names)
    
def load_nclimgrid_data(
        start_date: str,
        end_date: str,
        spatial_scale: str = 'cty',
        scaled: bool = True,
        states: Union[list[str], str] = 'all',
        counties: Union[list[str], str, None] = 'all',
        variables: list[str] = ['TAVG']
        ) -> Table:



    file_names = make_file_names(
            bucket = 'noaa-nclimgrid-daily-pds',
            start_date = start_date,
            end_date = end_date,
            spatial_scale= spatial_scale,
            scaled = scaled
            )


    fs = s3fs.S3FileSystem()
    dataset = ds.dataset(file_names, filesystem = fs)

    if spatial_scale == 'cty':
        if states == 'all':
            if counties == 'all':
                scanned_ds = dataset.scanner(
                    columns=["date", "state", "county"] + variables,
                    )
            else:
                scanned_ds = dataset.scanner(
                    columns=["date", "state", "county"] + variables,
                    filter = ds.field('county').isin(counties)
                    )
        else:
            if counties == 'all':
                scanned_ds = dataset.scanner(
                    columns=["date", "state", "county"] + variables,
                    filter = ds.field('state').isin(states)
                    )
            else:
                scanned_ds = dataset.scanner(
                    columns=["date", "state", "county"] + variables,
                    filter = (ds.field('county').isin(counties)) & (ds.field('state').isin(states))
                    )

    else:
        if states == 'all':
            scanned_ds = dataset.scanner(
                columns=["date", "state"] + variables,
                )
        else:
            scanned_ds = dataset.scanner(
                columns=["date", "state"] + variables,
                filter = ds.field('state').isin(states)
                )

    
    return(scanned_ds.to_table())

