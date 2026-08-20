import os

# Define the start and end years
start_year = 1961
end_year = 2025

# Loop through the years and create a folder for each
for year in range(start_year, end_year + 1):
    folder_name = str(year)
    try:
        os.makedirs(folder_name, exist_ok=True)
        print(f"Folder '{folder_name}' created successfully.")
    except OSError as error:
        print(f"Error creating folder '{folder_name}': {error}")

print("\nFolder creation process complete!")