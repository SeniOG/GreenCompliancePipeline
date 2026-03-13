import requests
import zipfile
import io
import os
import pandas as pd

def sync_gleif_data(url, output_path):
    print(f"Fetching GLEIF Global LEI data from {url}...")
    headers = {'User-Agent': 'GreenCompliancePipeline/1.0 (LocalTest)'}
    
    # Use allow_redirects=True to follow the API to the storage bucket
    response = requests.get(url, headers=headers, stream=True, allow_redirects=True)
    
    # Check if the request actually worked
    if response.status_code != 200:
        raise Exception(f"GLEIF Download failed with status {response.status_code}")

    zip_path = "data/temp_gleif.zip"
    os.makedirs("data", exist_ok=True)

    # Save ZIP
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print("Unzipping GLEIF Golden Copy...")
    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_file = [name for name in z.namelist() if name.endswith('.csv')][0]
        with z.open(csv_file) as f:
            # We take 2,000 rows for the LEI directory to increase match chances
            df = pd.read_csv(f, nrows=2000, low_memory=False)
            df.columns = [c.replace(' ', '_').lower() for c in df.columns]
            df.to_csv(output_path, index=False)

    os.remove(zip_path)
    print(f"GLEIF Sample saved to {output_path}")