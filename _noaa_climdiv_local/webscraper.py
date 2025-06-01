import requests
import os

# Define the URLs to download
urls = [
    "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/climdiv-tmaxcy-v1.0.0-20250506",
    "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/climdiv-pcpncy-v1.0.0-20250506"
]

# Download and save each file using the name from the URL
for url in urls:
    filename = os.path.basename(url)  # Extracts the filename from the URL
    print(f"Downloading {filename}...")
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"Saved as {filename}")
    else:
        print(f"Failed to download {filename}. Status code: {response.status_code}")
