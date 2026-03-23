import os # For file system operations
from typing import Optional # For type hinting optional parameters

import pandas as pd # For data manipulation and analysis
from fastapi import FastAPI, HTTPException # For creating the API and handling HTTP exceptions
from dotenv import load_dotenv # For loading environment variables from a .env file


# Load environment variables from .env file
from connector.fetch_outages import run_and_save

load_dotenv()
app = FastAPI(title="Arkham EIA Nuclear Outages API")
# Mapping dataset keys to their corresponding Parquet file paths
DATASET_TO_PATH = {
    "facility": "data/facility.parquet",
    "generator": "data/generator.parquet",
    "us": "data/us.parquet",
}

# Endpoint to trigger data fetching and saving for a specified dataset
def load_df(dataset: str) -> pd.DataFrame:
    path = DATASET_TO_PATH.get(dataset)
    # If the dataset key is invalid, raise an HTTP 400 exception with a descriptive error message
    if not path:
        raise HTTPException(status_code=400, detail="Invalid dataset. Use facility, generator, or us.")
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_parquet(path)

# Endpoint to refresh the data for a specified dataset by fetching new data from the EIA API and saving it to a Parquet file
@app.post("/refresh")
def refresh(dataset: str = "facility"):
    api_key = os.getenv("EIA_API_KEY")
    # If the API key is missing from the environment variables, raise an HTTP 500 exception 
    if not api_key:
        raise HTTPException(status_code=500, detail="Missing EIA_API_KEY")
    # If the dataset key is invalid, raise an HTTP 400 exception 
    if dataset not in DATASET_TO_PATH:
        raise HTTPException(status_code=400, detail="Invalid dataset. Use facility, generator, or us.")
    
    # Run the data fetching and saving process for the specified dataset and return a response indicating the status, dataset, number of rows loaded, and the path where the data is stored
    rows = run_and_save(dataset, api_key)
    return {"status": "ok", "dataset": dataset, "rows_loaded": rows, "stored": DATASET_TO_PATH[dataset]}

# Endpoint to retrieve data for a specified dataset with optional filtering parameters 
@app.get("/data")
def get_data(
    dataset: str = "facility", # The dataset to retrieve
    limit: int = 100,
    offset: int = 0,
    start_date: Optional[str] = None, # Start date for filtering (YYYY-MM-DD format)
    end_date: Optional[str] = None, # End date for filtering (YYYY-MM-DD format)
    facility: Optional[int] = None, 
    generator: Optional[int] = None,
):
    
    df = load_df(dataset)
    # If the dataset is empty, return an empty response with total=0 and an empty items list
    if df.empty:
        return {"total": 0, "limit": limit, "offset": offset, "items": []}
    
    # If the 'period' column exists in the DataFrame, convert it to datetime format and extract the date
    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"], errors="coerce").dt.date

    # Filtering based on the provided start_date and end_date parameters, if they are specified
    if start_date:
        sd = pd.to_datetime(start_date).date()
        df = df[df["period"] >= sd]
    if end_date:
        ed = pd.to_datetime(end_date).date()
        df = df[df["period"] <= ed]

    # Additional filtering based on the 'facility' and 'generator' parameters, if they are specified
    if facility is not None and "facility" in df.columns:
        df = df[df["facility"] == facility]
    if generator is not None and "generator" in df.columns:
        df = df[df["generator"] == generator]

    total = len(df)

    # Paginate the filtered DataFrame based on the provided limit and offset parameters
    # , and convert the paginated data to a list of dictionaries for the response
    page = df.iloc[offset: offset + limit].to_dict(orient="records")

    return {"total": total, "limit": limit, "offset": offset, "items": page}
