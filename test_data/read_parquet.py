import polars as pl
import os

def inspect_parquet_file_with_filters(file_path: str, target_state: str, target_fips: str):
    """
    Reads a Parquet file, prints its schema, a sample of its content,
    and checks for specific state and FIPS code.

    Args:
        file_path (str): The full path to the .parquet file.
        target_state (str): The state name to filter by (e.g., "Texas").
        target_fips (str): The FIPS code to filter by (e.g., "48201").
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found at '{file_path}'")
        return

    print(f"--- Inspecting Parquet File: {file_path} ---")
    try:
        # Read the Parquet file into a Polars DataFrame
        df = pl.read_parquet(file_path)

        # Print the schema
        print("\nSchema:")
        for col_name, dtype in df.schema.items():
            print(f"  - {col_name}: {dtype}")

        # Print the number of rows and columns
        print(f"\nShape (rows, columns): {df.shape}")

        # Print the first 5 rows of the DataFrame
        print("\nFirst 5 rows (overall):")
        print(df.head(5))

        # Check if required columns exist
        if "state_name" not in df.columns:
            print(f"\nError: 'state_name' column not found in the Parquet file.")
            return
        if "fips" not in df.columns:
            print(f"\nError: 'fips' column not found in the Parquet file.")
            return

        # Filter for the target state
        print(f"\n--- Filtering for state_name: '{target_state}' ---")
        state_df = df.filter(pl.col("state_name") == target_state)
        print(f"Number of rows found for state '{target_state}': {state_df.height}")

        if state_df.height > 0:
            print(f"\nFirst 5 rows for state '{target_state}':")
            print(state_df.head(5))

            # Filter the state data for the target FIPS code
            print(f"\n--- Filtering for fips: '{target_fips}' (within state '{target_state}') ---")
            county_df = state_df.filter(pl.col("fips") == target_fips)
            print(f"Number of rows found for fips '{target_fips}' in state '{target_state}': {county_df.height}")

            if county_df.height > 0:
                print(f"\nFirst 5 rows for fips '{target_fips}' in state '{target_state}':")
                print(county_df.head(5))
                print("\nRelevant data found for the specified state and county/FIPS in this file.")
            else:
                print(f"\nNo data found for fips '{target_fips}' within state '{target_state}' in this file.")
        else:
            print(f"\nNo data found for state '{target_state}', so cannot check for fips '{target_fips}'.")

    except Exception as e:
        print(f"\nAn error occurred while reading or inspecting the Parquet file: {e}")

if __name__ == "__main__":
    # --- IMPORTANT ---
    # Replace this with the actual path to your downloaded .parquet file
    local_file_path = "195102.parquet" # Assuming it's in the same directory as this script
    # Or if it's elsewhere:
    # local_file_path = "C:/path/to/your/downloaded/195102.parquet"

    # Define the state and FIPS code you are looking for
    state_to_check = "Texas"
    fips_to_check = "48201" # FIPS for Harris County, TX

    inspect_parquet_file_with_filters(local_file_path, state_to_check, fips_to_check)
