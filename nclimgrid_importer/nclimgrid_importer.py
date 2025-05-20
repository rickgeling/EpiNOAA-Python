# import polars as pl
# import pyarrow.parquet as pq
# import s3fs
# import pyarrow.dataset as ds
# import pendulum
# from typing import Optional, Union
# from pyarrow import Table


# def make_file_names(
#         bucket: str,
#         start_date: str,
#         end_date: str,
#         spatial_scale: str,
#         scaled: bool,
#         ) -> list[str]:

#     scaled_text = 'scaled' if scaled else 'prelim'

#     """ documentation here"""
#     start_date = pendulum.from_format(start_date, 'YYYY-MM-DD') 
#     end_date = pendulum.from_format(end_date, 'YYYY-MM-DD')

#     # file_names = [f"s3://{bucket}/EpiNOAA/parquet/{month.format('YYYYMM')}-{spatial_scale}-{scaled_text}.parquet" for month in pendulum.period(start_date, end_date).range('months')]
#     # --- Start of new iteration logic ---
#     months_to_process = []
#     current_month = start_date.start_of('month')
#     # Ensure we iterate up to and including the month of the processed_end_date
#     loop_until_date = end_date.start_of('month')

#     while current_month <= loop_until_date:
#         months_to_process.append(current_month)
#         current_month = current_month.add(months=1)
#     # --- End of new iteration logic ---

#     # Now use the generated list of months
#     #file_names = [f"s3://{bucket}/EpiNOAA/parquet/{month.format('YYYYMM')}-{spatial_scale}-{scaled_text}.parquet" for month in months_to_process]
#     file_names = [f"s3://{bucket}/EpiNOAA/v1-0-0/parquet/{spatial_scale}/YEAR={month.format('YYYY')}/STATUS={scaled_text}/{month.format('YYYYMM')}.parquet" for month in months_to_process]
    
#     print(f"Generated {len(file_names)} file names. First few: {file_names[:3]}") # Print some generated names
    

#     return(file_names)
    
# def load_nclimgrid_data(
#         start_date: str,
#         end_date: str,
#         spatial_scale: str = 'cty',
#         scaled: bool = True,
#         states: Union[list[str], str] = 'all',
#         counties: Union[list[str], str, None] = 'all',
#         variables: list[str] = ['TAVG']
#         ) -> Table:



#     file_names = make_file_names(
#             bucket = 'noaa-nclimgrid-daily-pds',
#             start_date = start_date,
#             end_date = end_date,
#             spatial_scale= spatial_scale,
#             scaled = scaled
#             )


#     fs = s3fs.S3FileSystem(anon=True)
    
    
#     # --- START OF TEMPORARY TEST for the first file ---
#     if file_names: # Ensure there's at least one file name
#         first_file_to_test = file_names[0]
#         print(f"Attempting to directly read first file with pyarrow.parquet.read_table: {first_file_to_test}")
#         try:
#             # Explicitly pass the filesystem object
#             table = pq.read_table(first_file_to_test, filesystem=fs)
#             print(f"Successfully read first file! Schema: {table.schema}")
#             print(f"Number of rows: {table.num_rows}")
#             # If you want to stop after this successful test:
#             # import sys
#             # sys.exit("Successfully read one file with read_table. Exiting for test.")
#         except FileNotFoundError:
#             print(f"FileNotFoundError when reading {first_file_to_test} with pq.read_table.")
#             # Optionally, try listing just this one file with s3fs to double-check
#             try:
#                 print(f"Double checking with s3fs.exists for {first_file_to_test}: {fs.exists(first_file_to_test)}")
#                 print(f"Double checking with s3fs.info for {first_file_to_test}: {fs.info(first_file_to_test)}")
#             except Exception as s3fs_e:
#                 print(f"Error during s3fs double check: {s3fs_e}")
#         except Exception as e:
#             print(f"OTHER error reading first file with pyarrow.parquet.read_table: {type(e).__name__} - {e}")
#     else:
#         print("file_names list is empty, skipping direct read test.")
#     # --- END OF TEMPORARY TEST ---
    
#     dataset = ds.dataset(file_names, filesystem = fs)

#     if spatial_scale == 'cty':
#         if states == 'all':
#             if counties == 'all':
#                 scanned_ds = dataset.scanner(
#                     columns=["date", "state", "county"] + variables,
#                     )
#             else:
#                 scanned_ds = dataset.scanner(
#                     columns=["date", "state", "county"] + variables,
#                     filter = ds.field('county').isin(counties)
#                     )
#         else:
#             if counties == 'all':
#                 scanned_ds = dataset.scanner(
#                     columns=["date", "state", "county"] + variables,
#                     filter = ds.field('state').isin(states)
#                     )
#             else:
#                 scanned_ds = dataset.scanner(
#                     columns=["date", "state", "county"] + variables,
#                     filter = (ds.field('county').isin(counties)) & (ds.field('state').isin(states))
#                     )

#     else:
#         if states == 'all':
#             scanned_ds = dataset.scanner(
#                 columns=["date", "state"] + variables,
#                 )
#         else:
#             scanned_ds = dataset.scanner(
#                 columns=["date", "state"] + variables,
#                 filter = ds.field('state').isin(states)
#                 )

    
#     return(scanned_ds.to_table())


# import polars as pl # Not used in the provided functions, but imported in user's code
# import pyarrow.parquet as pq
# import s3fs
# import pyarrow.dataset as ds
# import pendulum
# from typing import Optional, Union, List # Corrected List import
# from pyarrow import Table


# def make_file_names(
#         bucket: str,
#         start_date: str, # This is initially a string
#         end_date: str,   # This is initially a string
#         spatial_scale: str,
#         scaled: bool,
#         ) -> list[str]:

#     scaled_text = 'scaled' if scaled else 'prelim'

#     """ documentation here"""
#     # These lines convert the input strings to Pendulum DateTime objects
#     processed_start_date = pendulum.from_format(start_date, 'YYYY-MM-DD')
#     processed_end_date = pendulum.from_format(end_date, 'YYYY-MM-DD')

#     # Manual month iteration
#     months_to_process = []
#     current_month = processed_start_date.start_of('month')
#     loop_until_date = processed_end_date.start_of('month')

#     while current_month <= loop_until_date:
#         months_to_process.append(current_month)
#         current_month = current_month.add(months=1)

#     # Corrected S3 path generation
#     file_names = [f"s3://{bucket}/EpiNOAA/v1-0-0/parquet/{spatial_scale}/YEAR={month.format('YYYY')}/STATUS={scaled_text}/{month.format('YYYYMM')}.parquet" for month in months_to_process]
    
#     print(f"Generated {len(file_names)} file names. First few: {file_names[:3] if file_names else 'None'}")
#     return file_names
    
# def load_nclimgrid_data(
#         start_date: str,
#         end_date: str,
#         spatial_scale: str = 'cty', # e.g., 'cty', 'ste', 'cen'
#         scaled: bool = True,
#         states: Union[List[str], str, None] = 'all', # List of state names or 'all'
#         counties: Union[List[str], str, None] = 'all', # List of FIPS codes or 'all'
#         variables: List[str] = ['tavg'] # Requested variables, e.g., ['tavg', 'prcp']
#         ) -> Optional[Table]: # Return type is Arrow Table or None if error

#     file_names = make_file_names(
#             bucket = 'noaa-nclimgrid-daily-pds',
#             start_date = start_date,
#             end_date = end_date,
#             spatial_scale= spatial_scale,
#             scaled = scaled
#             )

#     if not file_names:
#         print("No file names generated by make_file_names, nothing to load.")
#         return None

#     fs = s3fs.S3FileSystem(anon=True)
#     print(f"s3fs.S3FileSystem initialized with anon=True. Type: {type(fs)}")
    
#     known_good_schema = None
#     actual_schema_column_names = []

#     # --- Read the first file to confirm readability and get the actual schema ---
#     if file_names:
#         first_file_to_test = file_names[0]
#         print(f"Attempting to directly read first file with pyarrow.parquet.read_table: {first_file_to_test}")
#         try:
#             table = pq.read_table(first_file_to_test, filesystem=fs)
#             known_good_schema = table.schema
#             actual_schema_column_names = [name.lower() for name in known_good_schema.names] # Store lowercase names
#             print(f"Successfully read first file! Schema: {known_good_schema}")
#             print(f"Actual column names (lowercase): {actual_schema_column_names}")
#             print(f"Number of rows in first file: {table.num_rows}")
#         except Exception as e:
#             print(f"Error reading first file with pyarrow.parquet.read_table: {type(e).__name__} - {e}")
#             print("Cannot proceed without reading at least one file to determine schema.")
#             return None
#     else:
#         print("file_names list is empty, cannot proceed.")
#         return None

#     # --- Create the full dataset ---
#     try:
#         print(f"\nAttempting to create dataset with ds.dataset for {len(file_names)} files...")
#         dataset = ds.dataset(
#             source=file_names,
#             filesystem=fs,
#             format="parquet",
#             schema=known_good_schema # Provide the schema obtained from the first file
#         )
#         print(f"Successfully created dataset object. Schema from dataset: {dataset.schema}")

#         # --- Determine columns to select based on actual schema and requested variables ---
#         columns_to_select = ["date"] # Always include 'date'

#         # Map spatial identifiers to actual schema columns
#         # Schema has: 'fips', 'state_name', 'region_name', 'postal_code'
#         if spatial_scale == 'cty':
#             if "state_name" in actual_schema_column_names:
#                 columns_to_select.append("state_name")
#             if "fips" in actual_schema_column_names: # FIPS is typically used for county identification
#                 columns_to_select.append("fips")
#             elif "region_name" in actual_schema_column_names: # Fallback if FIPS not present but region_name is
#                 columns_to_select.append("region_name") # This might be the county name
#         elif spatial_scale in ['ste', 'cen']: # For state or census division
#             if "state_name" in actual_schema_column_names:
#                 columns_to_select.append("state_name")
#             # For 'cen', 'region_name' might be relevant if it represents census divisions
#             if spatial_scale == 'cen' and "region_name" in actual_schema_column_names:
#                  if "region_name" not in columns_to_select: columns_to_select.append("region_name")


#         # Add requested data variables, ensuring they exist in the schema (case-insensitive check)
#         processed_variables = []
#         for var_req in variables:
#             var_req_lower = var_req.lower()
#             if var_req_lower in actual_schema_column_names:
#                 # Find the original cased name from the schema to use for selection
#                 original_case_var = known_good_schema.names[actual_schema_column_names.index(var_req_lower)]
#                 processed_variables.append(original_case_var)
#             else:
#                 print(f"Warning: Requested variable '{var_req}' (as '{var_req_lower}') not found in schema columns: {actual_schema_column_names}")
        
#         columns_to_select.extend(processed_variables)
#         columns_to_select = list(dict.fromkeys(columns_to_select)) # Remove duplicates, preserve order

#         print(f"Columns to select for scanner: {columns_to_select}")

#         # --- Construct filters based on actual schema ---
#         active_filters = []
#         if states != 'all' and "state_name" in actual_schema_column_names:
#             print(f"Applying state filter for: {states}")
#             # Ensure states is a list
#             states_list = states if isinstance(states, list) else [states]
#             active_filters.append(ds.field("state_name").isin(states_list))

#         if spatial_scale == 'cty' and counties != 'all' and "fips" in actual_schema_column_names:
#             print(f"Applying county (FIPS) filter for: {counties}")
#             # Ensure counties is a list
#             counties_list = counties if isinstance(counties, list) else [counties]
#             active_filters.append(ds.field("fips").isin(counties_list))
        
#         combined_filter = None
#         if active_filters:
#             # Start with the first filter
#             combined_filter = active_filters[0]
#             # If there are more filters, combine them with an AND operator
#             for i in range(1, len(active_filters)):
#                 combined_filter = combined_filter & active_filters[i]
#             print(f"Combined filter to apply: {combined_filter}")
#         else:
#             print("No filters to apply.")

#         # --- Create scanner and read table ---
#         print(f"\nAttempting to create scanner with selected columns and filters...")
#         scanned_ds = dataset.scanner(
#             columns=columns_to_select,
#             filter=combined_filter
#         )
#         print("Successfully created scanner.")
            
#         print("\nAttempting to read full table from scanner...")
#         final_table = scanned_ds.to_table()
#         print(f"Successfully read full table from scanner. Total rows: {final_table.num_rows}, Schema: {final_table.schema}")
            
#         return final_table

#     except Exception as e:
#         print(f"Error during dataset creation or scanning: {type(e).__name__} - {e}")
#         if dataset:
#              print(f"Dataset schema at time of error (if available): {dataset.schema}")
#         return None
    
#     print("Reached end of function unexpectedly.") # Should not happen
#     return None

# import polars as pl # Not used in the provided functions, but imported in user's code
# import pyarrow.parquet as pq
# import s3fs
# import pyarrow.dataset as ds
# import pendulum
# from typing import Optional, Union, List # Corrected List import
# from pyarrow import Table


# def make_file_names(
#         bucket: str,
#         start_date: str, # This is initially a string
#         end_date: str,   # This is initially a string
#         spatial_scale: str,
#         scaled: bool,
#         ) -> list[str]:

#     scaled_text = 'scaled' if scaled else 'prelim'

#     """ documentation here"""
#     # These lines convert the input strings to Pendulum DateTime objects
#     processed_start_date = pendulum.from_format(start_date, 'YYYY-MM-DD')
#     processed_end_date = pendulum.from_format(end_date, 'YYYY-MM-DD')

#     # Manual month iteration
#     months_to_process = []
#     current_month = processed_start_date.start_of('month')
#     loop_until_date = processed_end_date.start_of('month')

#     while current_month <= loop_until_date:
#         months_to_process.append(current_month)
#         current_month = current_month.add(months=1)

#     # Corrected S3 path generation
#     file_names = [f"s3://{bucket}/EpiNOAA/v1-0-0/parquet/{spatial_scale}/YEAR={month.format('YYYY')}/STATUS={scaled_text}/{month.format('YYYYMM')}.parquet" for month in months_to_process]
    
#     print(f"Generated {len(file_names)} file names. First few: {file_names[:3] if file_names else 'None'}")
#     return file_names
    
# def load_nclimgrid_data(
#         start_date: str,
#         end_date: str,
#         spatial_scale: str = 'cty', # e.g., 'cty', 'ste', 'cen'
#         scaled: bool = True,
#         states: Union[List[str], str, None] = 'all', # List of state names or 'all'
#         counties: Union[List[str], str, None] = 'all', # List of FIPS codes or 'all'
#         variables: List[str] = ['tavg'] # Requested variables, e.g., ['tavg', 'prcp']
#         ) -> Optional[Table]: # Return type is Arrow Table or None if error

#     file_names = make_file_names(
#             bucket = 'noaa-nclimgrid-daily-pds',
#             start_date = start_date,
#             end_date = end_date,
#             spatial_scale= spatial_scale,
#             scaled = scaled
#             )

#     if not file_names:
#         print("No file names generated by make_file_names, nothing to load.")
#         return None

#     fs = s3fs.S3FileSystem(anon=True)
#     print(f"s3fs.S3FileSystem initialized with anon=True. Type: {type(fs)}")
    
#     known_good_schema = None
#     actual_schema_column_names = []

#     # --- Read the first file to confirm readability and get the actual schema ---
#     if file_names:
#         first_file_to_test = file_names[0]
#         print(f"Attempting to directly read first file with pyarrow.parquet.read_table: {first_file_to_test}")
#         try:
#             table = pq.read_table(first_file_to_test, filesystem=fs)
#             known_good_schema = table.schema
#             actual_schema_column_names = [name.lower() for name in known_good_schema.names] # Store lowercase names
#             print(f"Successfully read first file! Schema: {known_good_schema}")
#             print(f"Actual column names (lowercase): {actual_schema_column_names}")
#             print(f"Number of rows in first file: {table.num_rows}")
#         except Exception as e:
#             print(f"Error reading first file with pyarrow.parquet.read_table: {type(e).__name__} - {e}")
#             print("Cannot proceed without reading at least one file to determine schema.")
#             return None
#     else:
#         print("file_names list is empty, cannot proceed.")
#         return None

#     # --- Create the full dataset ---
#     try:
#         print(f"\nAttempting to create dataset with ds.dataset for {len(file_names)} files...")
#         dataset = ds.dataset(
#             source=file_names,
#             filesystem=fs,
#             format="parquet",
#             schema=known_good_schema # Provide the schema obtained from the first file
#         )
#         print(f"Successfully created dataset object. Schema from dataset: {dataset.schema}")

#         # --- Determine columns to select based on actual schema and requested variables ---
#         columns_to_select = ["date"] # Always include 'date'

#         if spatial_scale == 'cty':
#             if "state_name" in actual_schema_column_names:
#                 columns_to_select.append("state_name")
#             if "fips" in actual_schema_column_names: 
#                 columns_to_select.append("fips")
#             elif "region_name" in actual_schema_column_names: 
#                 columns_to_select.append("region_name") 
#         elif spatial_scale in ['ste', 'cen']: 
#             if "state_name" in actual_schema_column_names:
#                 columns_to_select.append("state_name")
#             if spatial_scale == 'cen' and "region_name" in actual_schema_column_names:
#                  if "region_name" not in columns_to_select: columns_to_select.append("region_name")

#         processed_variables = []
#         for var_req in variables:
#             var_req_lower = var_req.lower()
#             if var_req_lower in actual_schema_column_names:
#                 original_case_var = known_good_schema.names[actual_schema_column_names.index(var_req_lower)]
#                 processed_variables.append(original_case_var)
#             else:
#                 print(f"Warning: Requested variable '{var_req}' (as '{var_req_lower}') not found in schema columns: {actual_schema_column_names}")
        
#         columns_to_select.extend(processed_variables)
#         columns_to_select = list(dict.fromkeys(columns_to_select)) 

#         print(f"Columns to select for scanner: {columns_to_select}")

#         # --- Construct filters based on actual schema ---
#         active_filters = []
#         if states != 'all' and "state_name" in actual_schema_column_names:
#             print(f"Applying state filter for: {states}")
#             states_list = states if isinstance(states, list) else [states]
#             active_filters.append(ds.field("state_name").isin(states_list))

#         if spatial_scale == 'cty' and counties != 'all' and "fips" in actual_schema_column_names:
#             print(f"Applying county (FIPS) filter for: {counties}")
#             counties_list = counties if isinstance(counties, list) else [counties]
#             active_filters.append(ds.field("fips").isin(counties_list))
        
#         combined_filter = None
#         if active_filters:
#             combined_filter = active_filters[0]
#             for i in range(1, len(active_filters)):
#                 combined_filter = combined_filter & active_filters[i]
#             print(f"Combined filter to apply: {combined_filter}")
#         else:
#             print("No filters to apply.")

#         # --- Create scanner and read table ---
#         print(f"\nAttempting to create scanner with selected columns and filters...")
#         scanned_ds = dataset.scanner(
#             columns=columns_to_select,
#             filter=combined_filter
#         )
#         print("Successfully created scanner.")
            
#         print("\nAttempting to read full table from scanner...")
#         # MODIFICATION: Added use_threads=False
#         final_table = scanned_ds.to_table(use_threads=False)
#         print(f"Successfully read full table from scanner. Total rows: {final_table.num_rows}, Schema: {final_table.schema}")
            
#         return final_table

#     except Exception as e:
#         print(f"Error during dataset creation or scanning: {type(e).__name__} - {e}")
#         if 'dataset' in locals() and dataset: # Check if dataset variable exists and is not None
#              print(f"Dataset schema at time of error (if available): {dataset.schema}")
#         return None
    
#     print("Reached end of function unexpectedly.") 
#     return None

import polars as pl # Not used in these functions, but user has it imported
import pyarrow.parquet as pq
import s3fs
import pyarrow.dataset as ds
import pyarrow # Added for pyarrow.concat_tables
import pendulum
from typing import Optional, Union, List
from pyarrow import Table


def make_file_names(
        bucket: str,
        start_date: str, # This is initially a string
        end_date: str,   # This is initially a string
        spatial_scale: str,
        scaled: bool,
        ) -> list[str]:

    scaled_text = 'scaled' if scaled else 'prelim'

    # These lines convert the input strings to Pendulum DateTime objects
    processed_start_date = pendulum.from_format(start_date, 'YYYY-MM-DD')
    processed_end_date = pendulum.from_format(end_date, 'YYYY-MM-DD')

    # Manual month iteration
    months_to_process = []
    current_month = processed_start_date.start_of('month')
    loop_until_date = processed_end_date.start_of('month')

    while current_month <= loop_until_date:
        months_to_process.append(current_month)
        current_month = current_month.add(months=1)

    # Corrected S3 path generation
    file_names = [f"s3://{bucket}/EpiNOAA/v1-0-0/parquet/{spatial_scale}/YEAR={month.format('YYYY')}/STATUS={scaled_text}/{month.format('YYYYMM')}.parquet" for month in months_to_process]
    
    print(f"Generated {len(file_names)} file names. First few: {file_names[:3] if file_names else 'None'}")
    return file_names
    
def load_nclimgrid_data(
        start_date: str,
        end_date: str,
        spatial_scale: str = 'cty', # e.g., 'cty', 'ste', 'cen'
        scaled: bool = True,
        states: Union[List[str], str, None] = 'all', # List of state names or 'all'
        counties: Union[List[str], str, None] = 'all', # List of FIPS codes or 'all'
        variables: List[str] = ['tavg'] # Requested variables, e.g., ['tavg', 'prcp']
        ) -> Optional[Table]: # Return type is Arrow Table or None if error

    file_names = make_file_names(
            bucket = 'noaa-nclimgrid-daily-pds',
            start_date = start_date,
            end_date = end_date,
            spatial_scale= spatial_scale,
            scaled = scaled
            )

    if not file_names:
        print("No file names generated by make_file_names, nothing to load.")
        return None

    fs = s3fs.S3FileSystem(anon=True)
    print(f"s3fs.S3FileSystem initialized. Type: {type(fs)}")
    
    known_good_schema = None
    actual_schema_column_names = []

    # --- Read the first file to confirm readability and get the actual schema ---
    if file_names:
        first_file_to_test = file_names[0]
        print(f"Attempting to directly read first file with pyarrow.parquet.read_table: {first_file_to_test}")
        try:
            table = pq.read_table(first_file_to_test, filesystem=fs)
            known_good_schema = table.schema
            actual_schema_column_names = [name.lower() for name in known_good_schema.names] # Store lowercase names
            print(f"Successfully read first file! Schema: {known_good_schema}")
            print(f"Actual column names (lowercase): {actual_schema_column_names}")
            print(f"Number of rows in first file: {table.num_rows}")
        except Exception as e:
            print(f"Error reading first file with pyarrow.parquet.read_table: {type(e).__name__} - {e}")
            print("Cannot proceed without a valid schema from the first file.")
            return None
    else:
        print("file_names list is empty, cannot proceed.")
        return None

    # --- Determine columns to select based on actual schema and requested variables ---
    columns_to_select = ["date"] 
    if spatial_scale == 'cty':
        if "state_name" in actual_schema_column_names: columns_to_select.append("state_name")
        if "fips" in actual_schema_column_names: columns_to_select.append("fips")
        elif "region_name" in actual_schema_column_names: columns_to_select.append("region_name") 
    elif spatial_scale in ['ste', 'cen']: 
        if "state_name" in actual_schema_column_names: columns_to_select.append("state_name")
        if spatial_scale == 'cen' and "region_name" in actual_schema_column_names:
             if "region_name" not in columns_to_select: columns_to_select.append("region_name")

    processed_variables = []
    for var_req in variables:
        var_req_lower = var_req.lower()
        if var_req_lower in actual_schema_column_names:
            original_case_var = known_good_schema.names[actual_schema_column_names.index(var_req_lower)]
            processed_variables.append(original_case_var)
        else:
            print(f"Warning: Requested variable '{var_req}' (as '{var_req_lower}') not found in schema: {actual_schema_column_names}")
    
    columns_to_select.extend(processed_variables)
    columns_to_select = list(dict.fromkeys(columns_to_select)) 
    print(f"Columns to select for processing: {columns_to_select}")

    # --- Construct filters based on actual schema ---
    active_filters = []
    if states != 'all' and "state_name" in actual_schema_column_names:
        states_list = states if isinstance(states, list) else [states]
        active_filters.append(ds.field("state_name").isin(states_list))
    if spatial_scale == 'cty' and counties != 'all' and "fips" in actual_schema_column_names:
        counties_list = counties if isinstance(counties, list) else [counties]
        active_filters.append(ds.field("fips").isin(counties_list))
    
    combined_filter = None
    if active_filters:
        combined_filter = active_filters[0]
        for i in range(1, len(active_filters)):
            combined_filter = combined_filter & active_filters[i]
        print(f"Combined filter to apply: {combined_filter}")
    else:
        print("No filters to apply.")

    # --- Process files one by one ---
    all_tables = []
    print(f"\nAttempting to process {len(file_names)} files individually...")
    for i, file_path in enumerate(file_names):
        print(f"Processing file {i+1}/{len(file_names)}: {file_path}")
        try:
            single_file_dataset = ds.dataset(
                source=file_path, 
                filesystem=fs,
                format="parquet",
                schema=known_good_schema 
            )
            
            scanner = single_file_dataset.scanner(
                columns=columns_to_select,
                filter=combined_filter
            )
            table_chunk = scanner.to_table() 
            print(f"  Successfully read {table_chunk.num_rows} rows from {file_path} after filtering.")
            if table_chunk.num_rows > 0:
                all_tables.append(table_chunk)
        except IndexError as ie: # Catching the specific IndexError we saw before, just in case
            print(f"  CRITICAL: IndexError for file {file_path}: {ie}")
            print(f"  Skipping this file due to IndexError.")
        except Exception as e:
            print(f"  OTHER error processing file {file_path}: {type(e).__name__} - {e}")
            
    if not all_tables:
        print("No data collected after processing all files (possibly all filtered out or errors encountered).")
        return None

    print(f"\nConcatenating {len(all_tables)} tables...")
    try:
        # MODIFICATION: Use pyarrow.concat_tables directly
        final_table = pyarrow.concat_tables(all_tables)
        print(f"Successfully concatenated tables. Total rows: {final_table.num_rows}, Schema: {final_table.schema}")
        return final_table
    except Exception as e:
        print(f"Error concatenating tables: {type(e).__name__} - {e}")
        print("Attempting to inspect schemas of collected tables if concatenation failed:")
        for i, t in enumerate(all_tables):
            print(f"  Table {i} schema: {t.schema}")
        return None

    print("Reached end of function unexpectedly.") 
    return None
