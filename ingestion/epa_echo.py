import requests
import zipfile
import os
import pandas as pd

def sync_epa_data(url, output_path):
    zip_path = "data/temp_epa.zip"
    os.makedirs("data", exist_ok=True)

    print(f"Downloading {url}...")
    # Stream the download so we don't choke the RAM
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                # Simple progress indicator
                if f.tell() % (1024 * 1024 * 10) == 0: # Every 10MB
                    print(f"Downloaded: {f.tell() // (1024 * 1024)} MB...")

    print("Unzipping and sampling first 1,000 rows...")
    with zipfile.ZipFile(zip_path, 'r') as z:
        csv_file = z.namelist()[0]
        with z.open(csv_file) as f:
            # We ONLY read 1000 rows locally to keep it fast
            df = pd.read_csv(f, nrows=1000, low_memory=False)
            df.columns = [c.replace(' ', '_').lower() for c in df.columns]
            df.to_csv(output_path, index=False)

    os.remove(zip_path) # Clean up the massive ZIP
    print(f"Done! Sample saved to {output_path}")