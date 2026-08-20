# from nclimgrid_importer import load_nclimgrid_data
# import polars as pl
# import pandas as pd
# import seaborn as sbn

# harris_old = load_nclimgrid_data(
#         start_date = '1951-01-01',
#         end_date = '2000-01-01',
#         spatial_scale = 'cty',
#         scaled = True,
#         states = ['Texas'],
#         counties = ['48201'],
#         variables = ['tmin']
#         )

# harris_new = load_nclimgrid_data(
#         start_date = '2011-01-01',
#         end_date = '2020-01-01',
#         spatial_scale = 'cty',
#         scaled = True,
#         states = ['Texas'],
#         counties = ['48201'],
#         variables = ['tmin']
#         )


# harris_comparison = pl.concat(
#         [
#             pl.from_arrow(harris_old).with_columns([pl.lit('Historical (1951-200)').alias('Period')]),
#             pl.from_arrow(harris_new).with_columns([pl.lit('Recent (2011-2020)').alias('Period')])
#         ]
#         )

# harris_plot_data = (
#         harris_comparison.lazy()
#         .with_columns([
#                 pl.col('date').str.slice(0,2).alias('month'),
#                 pl.col('tmin').cast(pl.Float64)
#             ])
#         .filter((pl.col('month').is_in(["07","08","09"])))
#         )

# harris_plot = sbn.displot(
#         harris_plot_data.collect().to_pandas(), 
#         x="tmin", hue="Period", common_norm = False, kind="kde", fill=True)

# harris_plot.savefig('./docs/docs/figures/harris_county_RICK.png')

# from nclimgrid_importer import load_nclimgrid_data # From python_script_s3_debug
# import polars as pl
# import pandas as pd # Seaborn typically expects Pandas DataFrames
# import seaborn as sbn
# import matplotlib.pyplot as plt # For plot display/customization if needed
# import os # For creating directories

# # --- Define Parameters ---
# HARRIS_COUNTY_FIPS = ["48201"] # Harris County, Texas FIPS code
# TARGET_STATE = ["Texas"]       # Full state name as in the data
# TARGET_VARIABLES = ["tmin"]    # Actual column name from schema

# # --- Load Data ---
# print("Loading historical data (1951-01-01 to 1999-12-31)...")
# harris_old_arrow = load_nclimgrid_data(
#     start_date='1951-01-01',
#     end_date='2025-01-01', # Includes all of 1999
#     spatial_scale='cty',
#     scaled=True,
#     states=TARGET_STATE,
#     counties=HARRIS_COUNTY_FIPS,
#     variables=TARGET_VARIABLES
# )

# print("\nLoading recent data (2011-01-01 to 2020-12-31)...")
# harris_new_arrow = load_nclimgrid_data(
#     start_date='2011-01-01',
#     end_date='2020-12-31', # Includes all of 2020
#     spatial_scale='cty',
#     scaled=True,
#     states=TARGET_STATE,
#     counties=HARRIS_COUNTY_FIPS,
#     variables=TARGET_VARIABLES
# )

# # --- Convert to Polars DataFrames ---
# # Create empty DataFrames with expected schema if loading fails or returns no rows,
# # to prevent errors in concat or later operations.
# # The schema should match what load_nclimgrid_data is expected to return for the selected columns.
# # Based on load_nclimgrid_data, selected columns would be like: ['date', 'state_name', 'fips', 'tmin']
# expected_schema = {
#     "date": pl.Datetime,
#     "state_name": pl.String,
#     "fips": pl.String,
#     TARGET_VARIABLES[0]: pl.String # Initially loaded as string, will be cast later
# }

# df_old = pl.from_arrow(harris_old_arrow) if harris_old_arrow is not None and harris_old_arrow.num_rows > 0 else pl.DataFrame(schema=expected_schema)
# df_new = pl.from_arrow(harris_new_arrow) if harris_new_arrow is not None and harris_new_arrow.num_rows > 0 else pl.DataFrame(schema=expected_schema)

# # --- Add Period Column and Combine ---
# if df_old.height > 0:
#     df_old = df_old.with_columns(pl.lit('Historical (1951-1999)').alias('Period'))
#     print(f"\nHistorical data processed. Rows: {df_old.height}")
# else:
#     print("\nNo historical data or it was empty.")

# if df_new.height > 0:
#     df_new = df_new.with_columns(pl.lit('Recent (2011-2020)').alias('Period'))
#     print(f"\nRecent data processed. Rows: {df_new.height}")
# else:
#     print("\nNo recent data or it was empty.")

# if df_old.height == 0 and df_new.height == 0:
#     print("\nNo data available for plotting. Exiting.")
# else:
#     # Concatenate. If one is empty, this will effectively be the other DataFrame.
#     # If both have data, ensure schemas are compatible (handled by creating with schema above)
#     harris_comparison = pl.concat([df_old, df_new], how="diagonal_relaxed")
        
#     print(f"\nCombined data. Total rows: {harris_comparison.height}")
#     if harris_comparison.height == 0:
#         print("Combined data is empty. Exiting.")
#     else:
#         print(f"Schema of combined data: {harris_comparison.schema}")
        
#         data_var_to_plot = TARGET_VARIABLES[0] # e.g., 'tmin'
        
#         # Ensure the data variable and 'date' columns exist before processing
#         if data_var_to_plot in harris_comparison.columns and "date" in harris_comparison.columns:
#             harris_plot_data_lazy = (
#                 harris_comparison.lazy()
#                 .with_columns([
#                     pl.col('date').dt.strftime("%m").alias('month'), # Get month as "01", "02", etc.
#                     pl.col(data_var_to_plot).cast(pl.Float64, strict=False).alias(f"{data_var_to_plot}_float") # Use lowercase, cast, and alias
#                 ])
#                 .filter(pl.col('month').is_in(["07", "08", "09"])) # Filter by string month
#             )

#             harris_plot_final_df = harris_plot_data_lazy.collect()
#             print(f"\nData for plotting (Jul-Sep). Rows: {harris_plot_final_df.height}")

#             if harris_plot_final_df.height > 0:
#                 harris_pandas_df = harris_plot_final_df.to_pandas()

#                 print("\nGenerating plot...")
#                 plot = sbn.displot(
#                     harris_pandas_df,
#                     x=f"{data_var_to_plot}_float", # Use the aliased float column
#                     hue="Period",
#                     common_norm=False,
#                     kind="kde",
#                     fill=True
#                 )
#                 plot.set_axis_labels(f"Minimum Temperature ({data_var_to_plot.upper()})", "Density")
#                 plot.fig.suptitle(f"Distribution of {data_var_to_plot.upper()} in Harris County, TX (Jul-Sep)", y=1.03)

#                 figure_dir = './docs/docs/figures/'
#                 os.makedirs(figure_dir, exist_ok=True)
#                 figure_path = os.path.join(figure_dir, 'harris_county_RICK.png') # Using your specified filename
#                 plot.savefig(figure_path)
#                 print(f"Plot saved to {figure_path}")
#                 # plt.show() # Uncomment to display plot if running in an environment that supports it
#             else:
#                 print("No data remaining after filtering for months Jul-Sep for Harris County, TX. Cannot generate plot.")
#         else:
#             missing_cols = []
#             if data_var_to_plot not in harris_comparison.columns: missing_cols.append(data_var_to_plot)
#             if "date" not in harris_comparison.columns: missing_cols.append("date")
#             print(f"Required column(s) {missing_cols} not found in combined data. Cannot generate plot. Available columns: {harris_comparison.columns}")


# from nclimgrid_importer import load_nclimgrid_data # From python_script_s3_debug
# import polars as pl
# import os # Not strictly needed for this version, but good if saving anything later

# # --- Define Parameters ---
# HARRIS_COUNTY_FIPS = ["48201"] # Harris County, Texas FIPS code
# TARGET_STATE = ["Texas"]       # Full state name as in the data
# TARGET_VARIABLES = ["tmin", "tmax", "tavg", "prcp"] # Specify variables you want stats for
# START_DATE = '2010-01-01'
# END_DATE = '2019-12-31' # Example: 10-year period

# # --- Load Data ---
# print(f"Loading data for Harris County, TX ({HARRIS_COUNTY_FIPS[0]})")
# print(f"Period: {START_DATE} to {END_DATE}")
# print(f"Variables: {TARGET_VARIABLES}")

# harris_arrow_table = load_nclimgrid_data(
#     start_date=START_DATE,
#     end_date=END_DATE,
#     spatial_scale='cty',
#     scaled=True, # Assuming you want the 'scaled' data version
#     states=TARGET_STATE,
#     counties=HARRIS_COUNTY_FIPS,
#     variables=TARGET_VARIABLES
# )

# # --- Convert to Polars DataFrame and Calculate Descriptive Statistics ---
# if harris_arrow_table is not None and harris_arrow_table.num_rows > 0:
#     print(f"\nSuccessfully loaded {harris_arrow_table.num_rows} rows.")
#     df_harris = pl.from_arrow(harris_arrow_table)
    
#     print("\nSchema of loaded data:")
#     for col_name, dtype in df_harris.schema.items():
#         print(f"  - {col_name}: {dtype}")

#     # Columns to attempt to cast to numeric for statistics
#     numeric_cols_to_process = [var for var in TARGET_VARIABLES if var in df_harris.columns]
    
#     if numeric_cols_to_process:
#         print(f"\n--- Debugging String Values Before Casting for columns: {numeric_cols_to_process} ---")
#         for col_name in numeric_cols_to_process:
#             if df_harris[col_name].dtype == pl.String:
#                 print(f"Sample string values in '{col_name}' (first 5 non-null, if any):")
#                 # Print a sample of non-null strings to see their format
#                 sample_strings = df_harris.select(pl.col(col_name)).filter(pl.col(col_name).is_not_null()).head(5)
#                 if sample_strings.height > 0:
#                     print(sample_strings)
#                 else:
#                     print(f"  No non-null string values found in '{col_name}' for this sample.")
#             else:
#                 print(f"Column '{col_name}' is not of type String, it's {df_harris[col_name].dtype}.")
#         print("--- End Debugging String Values ---")


#         print(f"\nAttempting to prepare columns for statistics: {numeric_cols_to_process}")
#         cast_expressions = []
#         df_for_stats_cols = []

#         for col_name in numeric_cols_to_process:
#             if col_name in df_harris.columns:
#                 # Add to list of columns we want for stats
#                 df_for_stats_cols.append(col_name)
                
#                 if df_harris[col_name].dtype == pl.String:
#                     print(f"  Processing column '{col_name}' for casting from String to Float64.")
#                     # Attempt to strip whitespace and then cast, replacing errors with null
#                     cast_expressions.append(
#                         pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False).alias(col_name)
#                     )
#                 elif df_harris[col_name].dtype == pl.Float64:
#                     print(f"  Column '{col_name}' is already Float64.")
#                     # No cast needed, but ensure it's part of the selection if it was already float
#                     # This case is less likely given the initial schema.
#                 else:
#                     print(f"  Warning: Column '{col_name}' is {df_harris[col_name].dtype}, not String. Skipping cast for stats, but will try to include if numeric.")
#                     if df_harris[col_name].dtype.is_numeric():
#                          pass # It will be included in df_for_stats_cols if numeric
#                     else: # Not string and not numeric, remove from stats consideration
#                         if col_name in df_for_stats_cols: df_for_stats_cols.remove(col_name)

#         if cast_expressions:
#             print(f"Applying cast expressions...")
#             df_harris = df_harris.with_columns(cast_expressions)

#         # Select only the columns intended for statistics (those that are now numeric)
#         # and filter out any that couldn't be made numeric.
#         final_numeric_cols_for_stats = [
#             col for col in df_for_stats_cols if col in df_harris.columns and df_harris[col].dtype == pl.Float64
#         ]

#         if final_numeric_cols_for_stats:
#             df_for_stats = df_harris.select(final_numeric_cols_for_stats)
#             print("\nDescriptive Statistics for numeric variables:")
#             print(df_for_stats.describe())
#         else:
#             print("No numeric columns were successfully prepared for statistics after casting attempts.")
            
#     else:
#         print("No variables specified in TARGET_VARIABLES were found in the loaded data for statistics.")

#     # print("\nSample of the processed data (first 5 rows):")
#     # print(df_harris.head(5))

# else:
#     print("\nNo data retrieved for Harris County for the specified period and variables.")

# print("\nScript finished.")

from nclimgrid_importer import load_nclimgrid_data # From python_script_s3_debug
import polars as pl
import os

# --- Define Parameters ---
HARRIS_COUNTY_FIPS = ["48201"] # Harris County, Texas FIPS code
TARGET_STATE = ["Texas"]       # Full state name as in the data
TARGET_VARIABLES = ["tmin", "tmax", "tavg", "prcp"] # Specify variables you want stats for
START_DATE = '1951-01-01'
END_DATE = '1960-12-31' # Example: 10-year period
MISSING_VALUE_PLACEHOLDER = -999.99 # Define the placeholder for missing data

# --- Load Data ---
print(f"Loading data for Harris County, TX ({HARRIS_COUNTY_FIPS[0]})")
print(f"Period: {START_DATE} to {END_DATE}")
print(f"Variables: {TARGET_VARIABLES}")

harris_arrow_table = load_nclimgrid_data(
    start_date=START_DATE,
    end_date=END_DATE,
    spatial_scale='cty',
    scaled=True, # Assuming you want the 'scaled' data version
    states=TARGET_STATE,
    counties=HARRIS_COUNTY_FIPS,
    variables=TARGET_VARIABLES
)

# --- Convert to Polars DataFrame and Calculate Descriptive Statistics ---
if harris_arrow_table is not None and harris_arrow_table.num_rows > 0:
    print(f"\nSuccessfully loaded {harris_arrow_table.num_rows} rows.")
    df_harris = pl.from_arrow(harris_arrow_table)
    
    print("\nSchema of loaded data:")
    for col_name, dtype in df_harris.schema.items():
        print(f"  - {col_name}: {dtype}")

    # Columns to attempt to cast to numeric for statistics
    numeric_cols_to_process = [var for var in TARGET_VARIABLES if var in df_harris.columns]
    
    if numeric_cols_to_process:
        print(f"\n--- Debugging String Values Before Casting for columns: {numeric_cols_to_process} ---")
        for col_name in numeric_cols_to_process:
            if df_harris[col_name].dtype == pl.String:
                print(f"Sample string values in '{col_name}' (first 5 non-null, if any):")
                sample_strings = df_harris.select(pl.col(col_name)).filter(pl.col(col_name).is_not_null()).head(5)
                if sample_strings.height > 0:
                    print(sample_strings)
                else:
                    print(f"  No non-null string values found in '{col_name}' for this sample.")
            else:
                print(f"Column '{col_name}' is not of type String, it's {df_harris[col_name].dtype}.")
        print("--- End Debugging String Values ---")

        print(f"\nAttempting to prepare columns for statistics: {numeric_cols_to_process}")
        cast_and_clean_expressions = []
        df_for_stats_cols = []

        for col_name in numeric_cols_to_process:
            if col_name in df_harris.columns:
                df_for_stats_cols.append(col_name) # Add to list of columns we want for stats
                
                if df_harris[col_name].dtype == pl.String:
                    print(f"  Processing column '{col_name}': Stripping chars, casting to Float64, then replacing {MISSING_VALUE_PLACEHOLDER} with null.")
                    # 1. Strip whitespace
                    # 2. Cast to Float64 (strict=False turns uncastable to null)
                    # 3. Replace the specific placeholder with actual null
                    cast_and_clean_expressions.append(
                        pl.when(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False) == MISSING_VALUE_PLACEHOLDER)
                        .then(None) # Replace with Polars null
                        .otherwise(pl.col(col_name).str.strip_chars().cast(pl.Float64, strict=False))
                        .alias(col_name)
                    )
                elif df_harris[col_name].dtype == pl.Float64:
                    print(f"  Processing column '{col_name}': Already Float64, replacing {MISSING_VALUE_PLACEHOLDER} with null.")
                    cast_and_clean_expressions.append(
                        pl.when(pl.col(col_name) == MISSING_VALUE_PLACEHOLDER)
                        .then(None)
                        .otherwise(pl.col(col_name))
                        .alias(col_name)
                    )
                else: # Not String or Float64
                    print(f"  Warning: Column '{col_name}' is {df_harris[col_name].dtype}. Skipping processing for stats if not numeric.")
                    if not df_harris[col_name].dtype.is_numeric():
                        if col_name in df_for_stats_cols: df_for_stats_cols.remove(col_name)
        
        if cast_and_clean_expressions:
            print(f"Applying cast and missing value replacement expressions...")
            df_harris = df_harris.with_columns(cast_and_clean_expressions)

        final_numeric_cols_for_stats = [
            col for col in df_for_stats_cols if col in df_harris.columns and df_harris[col].dtype == pl.Float64
        ]

        if final_numeric_cols_for_stats:
            df_for_stats = df_harris.select(final_numeric_cols_for_stats)
            print("\nDescriptive Statistics for numeric variables (after handling missing values):")
            print(df_for_stats.describe())
        else:
            print("No numeric columns were successfully prepared for statistics after casting and cleaning attempts.")
            
    else:
        print("No variables specified in TARGET_VARIABLES were found in the loaded data for statistics.")

else:
    print("\nNo data retrieved for Harris County for the specified period and variables.")

print("\nScript finished.")
