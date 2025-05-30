
### This script moves parquet files from the user's Downloads folder to a structured
### directory based on year and month. It creates a folder structure if it doesn't exist

### SO NOTE: THIS FILE SHOULD BE PLACED IN THE DOWNLOADS FOLDER.


import os
import shutil
from pathlib import Path

def get_downloads_folder():
    """Gets the path to the user's Downloads folder."""
    return Path.home() / "Downloads"

def move_parquet_files(start_year, end_year):
    """
    Moves parquet files from the Downloads folder to a specified destination structure.

    Args:
        start_year (int): The first year in the range to process.
        end_year (int): The last year in the range to process.
    """
    downloads_folder = get_downloads_folder()
    # IMPORTANT: This is the base destination folder as specified.
    # If you are not 'rickg' or your path is different, you'll need to change this.
    base_destination_root = Path(r"C:\Users\rickg\OneDrive\Documents\GitHub\TVC_analysis_crop_yields\02_data\noaa-nclimgrid-daily")

    if not downloads_folder.exists():
        print(f"Error: Downloads folder not found at '{downloads_folder}'")
        return
    if not base_destination_root.parent.exists(): # Check if the parent of the base destination exists
        print(f"Error: The parent directory of the destination path does not exist: '{base_destination_root.parent}'")
        print("Please ensure the base path up to '.../noaa-nclimgrid-daily' is accessible or modifiable.")
        return

    print(f"Searching for parquet files in: {downloads_folder}")
    print(f"Moving files to subfolders within: {base_destination_root}\n")

    for year in range(start_year, end_year + 1):
        year_str = str(year)
        year_destination_folder = base_destination_root / year_str

        # Create the year-specific destination folder if it doesn't exist
        try:
            os.makedirs(year_destination_folder, exist_ok=True)
            print(f"Ensured destination folder exists: {year_destination_folder}")
        except OSError as e:
            print(f"Error creating directory {year_destination_folder}: {e}")
            print(f"Skipping year {year_str} due to directory creation error.")
            continue # Skip this year if its directory can't be created

        files_moved_for_year = 0
        for month in range(1, 13):
            month_str = f"{month:02d}"  # Format month as 01, 02, ..., 12
            parquet_filename = f"{year_str}{month_str}.parquet"
            source_file_path = downloads_folder / parquet_filename
            destination_file_path = year_destination_folder / parquet_filename

            if source_file_path.exists():
                try:
                    shutil.move(str(source_file_path), str(destination_file_path))
                    print(f"  Moved: '{source_file_path.name}' to '{destination_file_path}'")
                    files_moved_for_year += 1
                except Exception as e:
                    print(f"  Error moving file '{source_file_path.name}': {e}")
            else:
                # Only print "File not found" if we expect files for this year/month range
                # This can be noisy if many files are missing, so consider adjusting verbosity if needed
                # print(f"  File not found: '{source_file_path}'")
                pass
        
        if files_moved_for_year == 0:
            print(f"No files found or moved for the year {year_str} from {downloads_folder}.")
        print("-" * 40) # Separator for years

    print("\nFile moving process complete! 🎉")

if __name__ == "__main__":
    print("Parquet File Mover")
    print("This script will move '.parquet' files from your Downloads folder")
    print("to a structured directory based on year and month.\n")

    # --- Configuration ---
    # If your Downloads folder or target destination is different,
    # please adjust the paths within the 'move_parquet_files' function.
    # The current base destination is:
    # C:\Users\rickg\OneDrive\Documents\GitHub\TVC_analysis_crop_yields\02_data\noaa-nclimgrid-daily
    # ---------------------

    while True:
        try:
            input_start_year = int(input("Enter the start year (e.g., 1951): "))
            input_end_year = int(input("Enter the end year (e.g., 2023): "))
            if input_start_year <= input_end_year:
                break
            else:
                print("Start year must be less than or equal to end year. Please try again.")
        except ValueError:
            print("Invalid input. Please enter numeric values for years.")

    move_parquet_files(input_start_year, input_end_year)