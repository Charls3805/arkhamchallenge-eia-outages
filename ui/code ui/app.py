import requests # For making HTTP requests to the EIA API
import streamlit as st # For building the user interface
import pandas as pd # For data manipulation and analysis

# Base URL for the API endpoints
API_BASE = "http://127.0.0.1:8000"

# Set up the Streamlit page configuration and title
st.set_page_config(page_title="Nuclear Outages Preview", layout="wide")
st.title("Nuclear Outages - Data Preview")

# Create a select box for choosing the dataset to preview
dataset = st.selectbox("Dataset", ["facility", "generator", "us"], index=0)

# Create input fields for pagination and date filtering parameters
col1, col2, col3, col4 = st.columns(4)
with col1:
    limit = st.number_input("Limit", min_value=10, max_value=500, value=50, step=10)
with col2:
    offset = st.number_input("Offset", min_value=0, value=0, step=50)
with col3:
    start_date = st.date_input("Start date", value=None)
with col4:
    end_date = st.date_input("End date", value=None)

# Initialize optional filters for facility and generator, which will be displayed based on the selected dataset
facility = None
generator = None
if dataset in ("facility", "generator"):
    c5, c6 = st.columns(2)
    with c5:
        facility = st.number_input("Facility (id) optional", min_value=0, step=1, format="%d")
    with c6:
        if dataset == "generator":
            generator = st.text_input("Generator (id) optional")

# Create a button to trigger the data refresh process
if st.button("Refresh data from EIA"):
    with st.spinner("Refreshing..."):
        # Make a POST request to the /refresh endpoint of the API to trigger data fetching and saving for the selected dataset. 
        try:
            r = requests.post(f"{API_BASE}/refresh", params={"dataset": dataset}, timeout=1000)
            r.raise_for_status()
            st.success(f"Refresh OK: {r.json()}")
        # If the refresh process fails, catch the exception and display an error message to the user
        except Exception as e:
            st.error(f"Refresh failed: {e}")

# Prepare parameters for the API request to retrieve data based on the selected dataset and filters
params = {"dataset": dataset, "limit": int(limit), "offset": int(offset)}
if start_date:
    params["start_date"] = start_date.isoformat()
if end_date:
    params["end_date"] = end_date.isoformat()
# If the facility filter is provided 
if facility:
    # Attempt to convert it to an integer and add it to the parameters for the API request
    try:
        params["generator"] = int(facility)
    except:
        st.warning("Facility must be an integer.")
# If the generator filter is provided and the selected dataset is "generator"  
if generator and dataset == "generator":
    # Attempt to convert it to an integer and add it to the parameters for the API request
    try:
        params["generator"] = int(generator)
    # If the conversion fails, display a warning message to the user
    except:
        st.warning("Generator must be an integer.")

# Add a divider and a subheader for the data section of the UI
st.divider()
st.subheader("Data")
# Use a spinner to indicate that data is being loaded while making the API request 
with st.spinner("Loading data..."):
    # Make a GET request to the /data endpoint of the API with the prepared parameters to retrieve the data. 
    try:
        resp = requests.get(f"{API_BASE}/data", params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        items = payload.get("items", [])
        total = payload.get("total", 0)
        # If no data is found for the selected filters, display an informational message to the user
        if total == 0 or not items:
            st.info("No data found for the selected filters.")
        # If data is found, display the total number of matching rows and show the data in a dataframe format
        else:
            st.caption(f"Total matching rows: {total}")
            df = pd.DataFrame(items)
            st.dataframe(df, use_container_width=True)
    # If the API request fails, catch the exception and display an error message to the user
    except Exception as e:
        st.error(f"API error: {e}")

