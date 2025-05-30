import os
from pathlib import Path

def check_monthly_files(base_path_str, start_year, end_year):
    """
    Checks for the presence of 12 monthly parquet files (YEAR01.parquet to YEAR12.parquet)
    for each year in the specified range within the base_path.

    Args:
        base_path_str (str): The root directory containing year folders.
        start_year (int): The first year to check.
        end_year (int): The last year to check.
    """
    base_path = Path(base_path_str)

    if not base_path.exists() or not base_path.is_dir():
        print(f"Error: Base directory '{base_path}' not found or is not a directory.")
        return

    print(f"Starting file check in: {base_path}")
    print(f"Checking years from {start_year} to {end_year}.\n")

    all_years_summary = {} # To store summary data for each year

    for year in range(start_year, end_year + 1):
        year_str = str(year)
        year_folder_path = base_path / year_str
        
        # Initialize status for the current year
        current_year_status = {
            "found_count": 0, 
            "missing_files": [], 
            "total_expected": 12,
            "status_message": ""
        }
        all_years_summary[year_str] = current_year_status

        print(f"--- Checking Year: {year_str} ---")

        if not year_folder_path.exists() or not year_folder_path.is_dir():
            print(f"  Folder '{year_folder_path.name}' not found.")
            current_year_status["status_message"] = "Folder not found"
            # If folder is missing, all 12 files are considered missing
            for month in range(1, 13):
                month_str_formatted = f"{month:02d}"
                expected_filename = f"{year_str}{month_str_formatted}.parquet"
                current_year_status["missing_files"].append(expected_filename)
            print(f"  Expected 12 files, found 0.")
            continue # Move to the next year

        # Folder exists, now check files
        files_found_this_year = 0
        missing_files_this_year_list = []

        for month in range(1, 13):
            month_str_formatted = f"{month:02d}"  # Format month as 01, 02, ..., 12
            expected_filename = f"{year_str}{month_str_formatted}.parquet"
            file_path = year_folder_path / expected_filename

            if file_path.exists() and file_path.is_file():
                files_found_this_year += 1
            else:
                missing_files_this_year_list.append(expected_filename)
        
        current_year_status["found_count"] = files_found_this_year
        current_year_status["missing_files"] = missing_files_this_year_list

        if not missing_files_this_year_list:
            print(f"  All 12 monthly files are present.")
            current_year_status["status_message"] = "All files present"
        else:
            print(f"  Found {files_found_this_year}/{current_year_status['total_expected']} files.")
            print(f"  Missing files for {year_str}:")
            for missing_file in missing_files_this_year_list:
                print(f"    - {missing_file}")
            current_year_status["status_message"] = f"{len(missing_files_this_year_list)} missing"
        
    # Overall Summary
    print("\n\n--- Overall Summary ---")
    perfect_years = []
    incomplete_years_details = {}

    for year_s, data in all_years_summary.items():
        if data["status_message"] == "All files present":
            perfect_years.append(year_s)
        elif data["status_message"] == "Folder not found":
            incomplete_years_details[year_s] = "Folder not found"
        else: # Some files missing
            # Extract month numbers from filenames like '195101.parquet' -> '01'
            missing_months_str = ', '.join([mf[len(year_s):-len(".parquet")] for mf in data['missing_files']])
            incomplete_years_details[year_s] = f"{len(data['missing_files'])} file(s) missing (months: {missing_months_str})"


    if perfect_years:
        print(f"\nYears with all 12 monthly files ({len(perfect_years)} years):")
        # Print in groups for better readability
        for i in range(0, len(perfect_years), 10): # Print 10 years per line
             print(f"  {', '.join(perfect_years[i:i+10])}")
    else:
        print("\nNo years found with all 12 monthly files in the specified range.")

    if incomplete_years_details:
        print(f"\nYears with missing files or missing folders ({len(incomplete_years_details)} years):")
        for year_s, reason in sorted(incomplete_years_details.items()): # Sort by year for clarity
            print(f"  - {year_s}: {reason}")
    else:
        # This case might occur if all specified years had all files or if the range was empty/invalid
        # but the above "perfect_years" check would cover the "all files present" scenario.
        # This means all specified years were checked and had no missing files (and weren't missing folders).
        if not perfect_years: # If no perfect years either, something is off or range was empty.
             print("\nNo issues found, or no year folders were processed based on input.")
        else: # Only perfect years were found
             print("\nAll checked years have complete data (no missing files or folders).")


    print("\nFile verification complete!")


if __name__ == "__main__":
    # --- Configuration ---
    # This is the base folder path from your previous setup.
    DEFAULT_BASE_PATH = r"C:\Users\rickg\OneDrive\Documents\GitHub\EpiNOAA-Python\_noaa_nclimgrid_local"
    
    print("Parquet File Verifier")
    print("This script checks for the presence of 12 monthly .parquet files per year.\n")
    print(f"Default base data directory is: {DEFAULT_BASE_PATH}")
    
    use_default_path_input = input("Use this default base path? (Y/n): ").strip().lower()
    if use_default_path_input == 'n':
        data_directory = input("Enter the full path to your base data directory: ").strip()
    else:
        data_directory = DEFAULT_BASE_PATH

    while True:
        try:
            # As you mentioned "from 1951 till 2024"
            s_year_default = 1951
            e_year_default = 2024
            s_year = int(input(f"Enter the start year to check (e.g., {s_year_default}): ") or s_year_default)
            e_year = int(input(f"Enter the end year to check (e.g., {e_year_default}): ") or e_year_default)
            
            if s_year <= e_year:
                break
            else:
                print("Start year must be less than or equal to end year. Please try again.")
        except ValueError:
            print("Invalid input. Please enter numeric values for years, or press Enter to use defaults.")

    check_monthly_files(data_directory, s_year, e_year)