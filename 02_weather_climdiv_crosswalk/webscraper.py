import requests
import os

#define the URLs to download
urls = [
    "https://www.ncei.noaa.gov/monitoring-content/data/us/climdiv/monthly/current/climdiv-tmaxcy-v1.0.0-20260806",
    "https://www.ncei.noaa.gov/monitoring-content/data/us/climdiv/monthly/current/climdiv-pcpncy-v1.0.0-20260806"
]

OUTPUT_DIR = "extracted_noaa_climdiv_data"

#downlaod and save each file using the name from the URL
for url in urls:
    filename = os.path.basename(url) + ".txt"  #extracts the filename from the URL
    output_path = os.path.join(OUTPUT_DIR, filename)
    print(f"Downloading {filename}...")
    response = requests.get(url)
    if response.status_code == 200:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"Saved as {output_path}")
    else:
        print(f"Failed to download {filename}. Status code: {response.status_code}")
