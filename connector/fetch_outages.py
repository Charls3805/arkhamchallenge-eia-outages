import os, time, logging # For debug and environment variable management
from typing import Any, Dict, List # For type hinting
import requests # For making API requests
import pandas as pd # For data manipulation and analysis
from dotenv import load_dotenv # For loading environment variables from a .env file

# Load environment variables from .env file
load_dotenv()

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Define the datasets
DATASETS = {
    # Each dataset has a base URL for the API, a list of required fields, and an output path for the Parquet file
    "facility": {
        "base_url": "https://api.eia.gov/v2/nuclear-outages/facility-nuclear-outages/data/",
        "required": ["period", "facility", "facilityName", "outage", "outage-units"],
        "out_parquet": "data/facility.parquet",
    },
     "generator": {
        "base_url": "https://api.eia.gov/v2/nuclear-outages/generator-nuclear-outages/data/",
        "required": ["period", "facility", "facilityName", "generator", "outage", "outage-units"],
        "out_parquet": "data/generator.parquet",
    },
    "us": {
        "base_url": "https://api.eia.gov/v2/nuclear-outages/us-nuclear-outages/data/",
        "required": ["period", "outage", "outage-units"],
        "out_parquet": "data/us.parquet",
    },
}

# Function to fetch a page of data from the EIA API
def fetch_page(base_url: str, api_key: str, offset: int, length: int) -> Dict[str, Any]:
    # Parameters for the API request
    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[0]": "outage",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": offset,
        "length": length,
    }

    # Attempt to fetch data with retries for network issues or API errors
    for attempt in range(2):
        # Make the API request and handle potential errors
        try:
            r = requests.get(base_url, params=params, timeout=30)
            if r.status_code in (401, 403):
                raise RuntimeError("Invalid API key or unauthorized (401/403).")
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            logging.warning(f"Network/API error attempt={attempt+1}: {e}")
            if attempt == 0:
                time.sleep(1.5)
            else:
                raise

# Function to validate that each row of data contains all required fields
def validate_rows(rows: List[Dict[str, Any]], required_fields: List[str]) -> List[Dict[str, Any]]:
    valid = []
    # Check each row for the presence of all required fields and log any missing fields
    for row in rows:
        if all(f in row for f in required_fields):
            valid.append(row)
        else:
            missing = [f for f in required_fields if f not in row]
            logging.warning(f"Skipping row missing={missing}")
    return valid

# Function to extract all data for a given dataset key, handling pagination and accumulating results
def extract_all(dataset_key: str, api_key: str, length: int = 5000) -> List[Dict[str, Any]]:
    # Get the configuration for the specified dataset key, including the base URL and required fields
    cfg = DATASETS[dataset_key]
    base_url = cfg["base_url"]
    required = cfg["required"]

    offset = 0
    all_rows: List[Dict[str, Any]] = []
    total = None

    # Loop to fetch data in batches, handling pagination and accumulating results
    while True:
        payload = fetch_page(base_url, api_key, offset=offset, length=length)
        response = payload.get("response", {})
        rows = response.get("data", [])
        # If the total number of records is not yet known, extract it from the response and log it
        if total is None:
            total_raw = response.get("total")
            total = int(total_raw) if total_raw is not None else None
            logging.info(f"[{dataset_key}] API total={total}")
        # If no rows are returned, break the loop as we have fetched all available data
        if not rows:
            logging.info(f"[{dataset_key}] No more data to fetch.")
            break
        # Validate the fetched rows to ensure they contain required fields, and accumulate valid rows
        rows = validate_rows(rows, required)
        all_rows.extend(rows)
        logging.info(f"[{dataset_key}] batch={len(rows)} accumulated={len(all_rows)} offset={offset}")
        # Update the offset for the next batch, and if we have reached or exceeded the total number of records, break the loop
        offset += length
        if total is not None and offset >= total:
            break

    return all_rows

# Main function to run the data extraction and save the results to a Parquet file
def run_and_save(dataset_key: str, api_key: str) -> int:
    rows = extract_all(dataset_key, api_key)
    df = pd.DataFrame(rows)
    os.makedirs("data", exist_ok=True)
    out_path = DATASETS[dataset_key]["out_parquet"]
    df.to_parquet(out_path, index=False)
    logging.info(f"[{dataset_key}] saved rows={len(df)} -> {out_path}")
    return len(df)
# Checks for the API key and runs the data extraction and saving process for each dataset key
if __name__ == "__main__":
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        raise RuntimeError("Missing EIA_API_KEY environment variable.")
    # Loop through each dataset key and run the data extraction and saving process
    for k in ["facility", "generator", "us"]:
        run_and_save(k, api_key)
