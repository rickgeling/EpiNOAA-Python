import polars as pl
from nclimgrid_importer import load_nclimgrid_data # Assuming nclimgrid_importer.py is accessible
from typing import List

# --- Define Parameters for this specific fetch ---
TARGET_FIPS_LIST = ["31109"]  # FIPS code for Lancaster County, Nebraska
# For state, load_nclimgrid_data can work with just FIPS if it's the full 5-digit county FIPS.
# However, if your load_nclimgrid_data's filtering logic still requires a state name
# (even if 'all' by default), you might need to pass it or ensure it handles FIPS-only correctly.
# For this example, we'll rely on FIPS-only filtering as discussed.
TARGET_STATES_LIST = 'all' # Or you could specify ["Nebraska"] if needed by your load_nclimgrid_data

START_DATE_FETCH = '2023-07-01'
END_DATE_FETCH = '2023-07-31' # To get data for the full month of July 2023

# Variables to fetch (ensure these are lowercase as in your schema)
VARIABLES_TO_FETCH = ["tmin", "tmax", "tavg", "prcp"] 

def fetch_and_display_data_for_fips_month():
    """
    Fetches and displays data for a specific FIPS code and month using load_nclimgrid_data.
    """
    print(f"--- Fetching S3 data for FIPS: {TARGET_FIPS_LIST} ---")
    print(f"Period: {START_DATE_FETCH} to {END_DATE_FETCH}")
    print(f"Variables: {VARIABLES_TO_FETCH}")

    # Call your data loading function
    # Note: load_nclimgrid_data expects 'counties' to be a list of FIPS strings
    arrow_table = load_nclimgrid_data(
        start_date=START_DATE_FETCH,
        end_date=END_DATE_FETCH,
        spatial_scale='cty',  # Assuming county level
        scaled=True,          # Assuming scaled data
        states=TARGET_STATES_LIST, # Using 'all' relies on FIPS being specific enough
        counties=TARGET_FIPS_LIST,
        variables=VARIABLES_TO_FETCH
    )

    if arrow_table is not None and arrow_table.num_rows > 0:
        print(f"\nSuccessfully loaded {arrow_table.num_rows} rows from S3.")
        
        # Convert to Polars DataFrame for easier inspection
        df_fips_month = pl.from_arrow(arrow_table)
        
        print("\nSchema of loaded data:")
        for col_name, dtype in df_fips_month.schema.items():
            print(f"  - {col_name}: {dtype}")

        print("\nFirst 5 rows of loaded data:")
        print(df_fips_month.head(5))

        # You can also print descriptive statistics for the fetched data
        # Ensure numeric columns are correctly cast if they are still strings here
        # (though load_nclimgrid_data should have handled casting and missing values)
        
        numeric_cols_for_stats = [
            var for var in VARIABLES_TO_FETCH 
            if var in df_fips_month.columns and df_fips_month[var].dtype == pl.Float64
        ]
        if numeric_cols_for_stats:
            print("\nDescriptive Statistics for numeric variables:")
            print(df_fips_month.select(numeric_cols_for_stats).describe())
        else:
            # This might happen if variables were not cast to Float64 in load_nclimgrid_data
            # or if the selected variables were not found/returned.
            print("\nNo numeric columns found for descriptive statistics, or variables not cast to Float64.")
            print("Checking dtypes of TARGET_VARIABLES in the dataframe:")
            for var in VARIABLES_TO_FETCH:
                if var in df_fips_month.columns:
                    print(f"  Column '{var}' dtype: {df_fips_month[var].dtype}")
                else:
                    print(f"  Column '{var}' not found in DataFrame.")


    elif arrow_table is not None and arrow_table.num_rows == 0:
        print("\nData loaded successfully, but 0 rows were returned for the specified FIPS and date range.")
    else:
        print("\nFailed to load data from S3, or an error occurred in load_nclimgrid_data.")

    print("\n--- Script finished ---")

if __name__ == "__main__":
    fetch_and_display_data_for_fips_month()
