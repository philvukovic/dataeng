import os
import gzip
import shutil
import requests
from pathlib import Path
from typing import Dict, List

DATASETS = {
    'green': [2019, 2020],
    'yellow': [2019, 2020],
    'fhv': [2019]
}

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

def download(dataset: str, year: int, month: int, output_dir: Path) -> None:
    """Download a single month of taxi data"""

    filename = f"{dataset}_tripdata_{year}-{month:02d}.csv.gz"
    url = f"{BASE_URL}/{dataset}/{filename}"
    output_path = output_dir / filename
    
    if output_path.with_suffix('.csv').exists():
        print(f"Skipping {filename} - already exists")
        return
        
    print(f"Downloading {filename}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            f.write(response.content)
            
        with gzip.open(output_path, 'rb') as f_in:
            with open(output_path.with_suffix('.csv'), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        output_path.unlink()
        
    except requests.RequestException as e:
        print(f"Failed to download {filename}: {e}")
    except Exception as e:
        print(f"Error processing {filename}: {e}")

def main():
    for dataset, years in DATASETS.items():
        data_dir = Path(f"data/{dataset}")
        data_dir.mkdir(parents=True, exist_ok=True)
        
        for year in years:
            for month in range(1, 13):
                download(dataset, year, month, data_dir)

if __name__ == "__main__":
    main()
