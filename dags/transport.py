from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
import pendulum
import json
from google.cloud import bigquery
from google.cloud import storage
import time

# Author: Melvin & Xin Tong, edited by Kai Jun

local_tz = pendulum.timezone("Asia/Singapore")

PROJECT_ID = "is3107-group-8" 

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 14, tzinfo=local_tz),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

logger = LoggingMixin().log

# helper function to get Onemap Token
# Author: Kai Jun
def extract_onemap_token(email, password):
    auth_url = "https://www.onemap.gov.sg/api/auth/post/getToken"
    auth_data = {"email": email, "password": password}

    auth_response = requests.post(auth_url, json=auth_data)
    auth_token = auth_response.json().get("access_token")

    logger.info(f"Auth Token: {auth_token}")
    return auth_token

# Global mapping dictionary for postal prefix to district
postal_to_district = {
    '01': '01',
    '02': '01',
    '03': '01',
    '04': '01',
    '05': '01',
    '06': '01',
    '07': '02', 
    '08': '02',
    '14': '03',
    '15': '03',
    '16': '03',
    '09': '04',
    '10': '04',
    '11': '05',
    '12': '05',
    '13': '05',
    '17': '06',
    '18': '07',
    '19': '07',
    '20': '08',
    '21': '08',
    '22': '09',
    '23': '09',
    '24': '10', 
    '25': '10',
    '26': '10',
    '27': '10',
    '28': '11',
    '29': '11',
    '30': '11',
    '31': '12', 
    '32': '12',
    '33': '12',
    '34': '13',
    '35': '13', 
    '36': '13',
    '37': '13',
    '38': '14',
    '39': '14',
    '40': '14',
    '41': '14',
    '42': '15',
    '43': '15', 
    '44': '15', 
    '45': '15',
    '46': '16',
    '47': '16',
    '48': '16',
    '49': '17',
    '50': '17',
    '81': '17',
    '51': '18', 
    '52': '18',
    '53': '19',
    '54': '19',
    '55': '19',
    '82': '19',
    '56': '20',
    '57': '20',
    '58': '21',
    '59': '21',
    '60': '22', 
    '61': '22',
    '62': '22', 
    '63': '22',
    '64': '22',
    '65': '23',
    '66': '23',
    '67': '23',
    '68': '23',
    '69': '24',
    '70': '24',
    '71': '24',
    '72': '25',
    '73': '25',
    '77': '26',
    '78': '26',
    '75': '27',
    '76': '27',
    '79': '28',
    '80': '28'
}

def infer_district(postal):
    """
    Returns the district corresponding to a postal code.
    - If the postal code is not 6 digits, left-pads it with zeros.
    - Uses the first two digits of the postal code to look up the district.
    - If the postal is missing or invalid, logs a warning and returns None.
    """
    if postal and isinstance(postal, str) and len(postal) >= 2:
        # Left-pad if needed
        if len(postal) != 6:
            postal = postal.zfill(6)
        postal_prefix = postal[:2]
        return postal_to_district.get(postal_prefix)
    else:
        logger.warning("Postal code missing or invalid: %s", postal)
        return None

# Helper function for mapping
# Author: Sok Yang
def get_postal_code_and_coords(address, token):
    """
    Fetch postal code, latitude, longitude, and district for a given address using OneMap API.
    
    Parameters:
        address (str): The address to query.
        token (str): Bearer token for authentication.
        
    Returns:
        pd.Series: [postal, latitude, longitude, district]
    """
    base_url = "https://www.onemap.gov.sg/api/common/elastic/search"
    params = {
        "searchVal": address,
        "returnGeom": "Y",
        "getAddrDetails": "Y",
        "pageNum": "1"
    }
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        response = requests.get(base_url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data.get("found", 0) > 0:
                result = data["results"][0]
                postal = result.get("POSTAL")
                latitude = result.get("LATITUDE")
                longitude = result.get("LONGITUDE")
                district = None
                if postal and isinstance(postal, str) and len(postal) >= 2:
                    # If postal is not 6 digits long, left-pad with zeros
                    if len(postal) != 6:
                        postal = postal.zfill(6)
                    postal_prefix = postal[:2]
                    district = postal_to_district.get(postal_prefix)
                else:
                    logger.warning(f"Address: {address} | Postal code missing or invalid.")
                return pd.Series([address, postal, latitude, longitude, district]) 
            else:
                logger.debug(f"No results found for address: {address}")
        else:
            logger.debug(f"Failed to fetch data for address: {address}. Status code: {response.status_code}")
    except Exception as e:
        logger.warning(f"Exception for address {address}: {e}") 
    return pd.Series([None, None, None, None, None]) # failcase returns None for all

# Helper function for batching (flow control due to API rate limit for one map)
# Author: Sok Yang, edited by Kai Jun
def process_batches(df, token, batch_size=50, rate_limit=250):
    """
    Processes the DataFrame in batches -> flow control 
    
    Parameters:
      df (DataFrame): DataFrame containing an 'address' column.
      batch_size (int): Number of rows to process per batch.
      token (str): OneMap API token.
      rate_limit (int): Maximum number of requests per minute.
      
    Returns:
      DataFrame: A DataFrame with columns ['address', 'postalCode', 'latitude', 'longitude', 'district']
    """
    logger.info("Running process_batches")
    num_batches = (len(df) // batch_size) + (1 if len(df) % batch_size != 0 else 0)
    results = []
    
    for i in range(num_batches):
        batch_df = df.iloc[i * batch_size : (i + 1) * batch_size]
        logger.info(f"Processing batch {i + 1}/{num_batches} with {len(batch_df)} addresses...")
        
        # Apply the get_postal_code_and_coords function to each address in the batch
        batch_results = batch_df['address'].apply(lambda addr: get_postal_code_and_coords(addr, token))
        results.append(batch_results)
        
        # Check the total number of requests processed so far
        if ((i + 1) * batch_size) % rate_limit == 0:
            logger.info("Rate limit reached, pausing for 60 seconds...")
            time.sleep(60)
    
    # Concatenate results from all batches into a single DataFrame
    final_results = pd.concat(results, ignore_index=True)
    final_results.columns = ['address', 'postalCode', 'latitude', 'longitude', 'district']
    return final_results

# Helper function for mapping
# Author: Kai Jun
def get_postal_code_districts(latitude, longitude, token):
    """
    Fetch postal code, latitude, longitude, and district for a given address using OneMap API.
    
    Parameters:
        address (str): The address to query.
        token (str): Bearer token for authentication.
        
    Returns:
        pd.Series: [postal, latitude, longitude, district]
    """
    base_url = f"https://www.onemap.gov.sg/api/public/revgeocode?location={latitude},{longitude}&buffer=50&addressType=All&otherFeatures=N"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(base_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            print(data)
            if len(data.get("GeocodeInfo", [])) > 0:
                result = data["GeocodeInfo"][0]
                postal = result.get("POSTALCODE")
                print(postal)
                return postal
            else:
                logger.debug(f"No results found for: {latitude} , {longitude}")
        else:
            logger.debug(f"Failed to fetch data for: {latitude} , {longitude}. Status code: {response.status_code}")
    except Exception as e:
        logger.warning(f"Exception for address {latitude} , {longitude}: {e}") 
    return None

# In-place batching function to process the DataFrame
def process_batches_inplace(df, token, batch_size=250, pause_seconds=60):
    """
    Process the DataFrame in batches, querying the postal code for each row using OneMap API.
    After each batch (of size batch_size), the function pauses for pause_seconds seconds.
    Once the postal code is updated, it calculates the district using infer_district.
    
    Parameters:
        df (pd.DataFrame): DataFrame with at least 'Latitude' and 'Longitude' columns.
        token (str): OneMap API token.
        batch_size (int): Number of rows to process in each batch.
        pause_seconds (int or float): Number of seconds to pause between batches.
        
    Returns:
        pd.DataFrame: The same DataFrame updated with 'postalCode' and 'district' columns.
    """
    total_rows = len(df)
    logger.info("Processing %d rows in batches of %d...", total_rows, batch_size)
    num_batches = (len(df) // batch_size) + (1 if len(df) % batch_size != 0 else 0)

    # Process each batch
    for i in range(0, total_rows, batch_size):
        batch_index = df.index[i : i + batch_size]
        logger.info(f"Processing batch {(i//batch_size) + 1}/{num_batches}...")
        # Query postal code for each row in the batch
        df.loc[batch_index, 'postalCode'] = df.loc[batch_index].apply(
            lambda row: get_postal_code_districts(row['Latitude'], row['Longitude'], token),
            axis=1
        )
        # Once we have the postal code, derive the district for each row in the batch
        df.loc[batch_index, 'district'] = df.loc[batch_index, 'postalCode'].apply(infer_district)
        
        # If there are more batches to process, pause to respect API rate limits
        if i + batch_size < total_rows:
            logger.info("Batch processed. Pausing for %s seconds...", pause_seconds)
            time.sleep(pause_seconds)
    
    logger.info("Completed processing all batches.")
    return df

def get_ter_school_details_google(school_name):
    TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    # Now using Variable.get for GOOGLE_API_KEY:
    api_key = Variable.get("GOOGLE_API_KEY")
    query = school_name + " Singapore"
    params = {"query": query, "key": api_key}
    response = requests.get(TEXT_SEARCH_URL, params=params)
    data = response.json()
    results_list = []
    if data["status"] == "OK":
        for result in data["results"]:
            results_list.append({
                "schoolName": school_name,
                "google_school_name": result["name"],
                "address": result["formatted_address"]
            })
    if not results_list:
        results_list.append({
            "schoolName": school_name,
            "google_school_name": None,
            "address": None
        })
    return results_list

# ============================================================================
# Helpers for tertiary extraction (Wikipedia scraping)
# ============================================================================
def get_university_names():
    UNI_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_universities_in_Singapore"
    logger.info("Extracting university names from Wikipedia")
    response = requests.get(UNI_WIKI_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    university_list = []
    section_div = soup.find("div", id="Universities_in_Singapore145")
    if section_div:
        stop_section = soup.find("div", id="Education_in_Singapore254")
        categories = section_div.find_all_next("th", scope="row", class_="navbox-group")
        for category in categories:
            if stop_section and category.find_previous("div") == stop_section:
                break
            next_td = category.find_next_sibling("td")
            if next_td:
                for link in next_td.find_all("a"):
                    university_name = link.get_text(strip=True)
                    university_list.append({"schoolName": university_name, "category": "University"})
    return university_list

def get_polytechnics():
    POLY_WIKI_URL = "https://en.wikipedia.org/wiki/Education_in_Singapore"
    logger.info("Extracting polytechnic names from Wikipedia")
    response = requests.get(POLY_WIKI_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    polytechnic_list = []
    polytechnic_section = soup.find("h2", {"id": "Polytechnics"})
    if polytechnic_section:
        para = polytechnic_section.find_next("p")
        for link in para.find_all("a"):
            polytechnic_name = link.get_text(strip=True)
            polytechnic_list.append({"schoolName": polytechnic_name, "category": "Polytechnic"})
    return polytechnic_list

def get_ite_names():
    ITE_WIKI_URL = "https://en.wikipedia.org/wiki/Institute_of_Technical_Education#Colleges"
    logger.info("Extracting ITE college names from Wikipedia")
    response = requests.get(ITE_WIKI_URL)
    soup = BeautifulSoup(response.text, "html.parser")
    ite_list = []
    ite_colleges_header = soup.find("h2", {"id": "Colleges"})
    if ite_colleges_header:
        ul = ite_colleges_header.find_next("ul")
        if ul:
            for link in ul.find_all("a"):
                college_name = link.get_text(strip=True)
                ite_list.append({"schoolName": college_name, "category": "ITE College"})
    return ite_list

# ============================================================================
# Extraction Functions
# ============================================================================
def extract_preschools(**kwargs):
    """
    Extracts preschools data from data.gov.sg using the specified dataset.
    Paginates through all records and pushes the extracted JSON to XCom.
    """
    logger.info("Starting extraction of preschools...")
    preschools_dataset_id = "d_696c994c50745b079b3684f0e90ffc53"
    api_url = f"https://data.gov.sg/api/action/datastore_search?resource_id={preschools_dataset_id}"
    offset = 0
    records = []
    while True:
        url = f"{api_url}&offset={offset}"
        logger.info("Fetching preschools data with offset: %d", offset)
        response = requests.get(url)
        data = response.json()
        result = data.get("result", {})
        batch = result.get("records", [])
        total = result.get("total", 0)
        logger.info("Fetched %d records; Total available: %d", len(batch), total)
        if not batch:
            logger.info("No more records returned. Ending extraction loop for preschools.")
            break
        records.extend(batch)
        offset += len(batch)
        if offset >= total:
            logger.info("Offset has reached total available records.")
            break
    logger.info("Completed extraction of preschools: %d records", len(records))
    # Push the result to XCom as a JSON string.
    ti = kwargs['ti']
    ti.xcom_push(key='preschools_extracted', value=json.dumps(records))

def extract_moe_schools(**kwargs):
    """
    Extracts MOE schools data from data.gov.sg using the specified dataset.
    Paginates through all records and pushes the extracted JSON to XCom.
    """
    logger.info("Starting extraction of MOE schools...")
    moe_schools_dataset_id = "d_688b934f82c1059ed0a6993d2a829089"
    api_url = f"https://data.gov.sg/api/action/datastore_search?resource_id={moe_schools_dataset_id}"
    offset = 0
    records = []
    while True:
        url = f"{api_url}&offset={offset}"
        logger.info("Fetching MOE schools data with offset: %d", offset)
        response = requests.get(url)
        data = response.json()
        result = data.get("result", {})
        batch = result.get("records", [])
        total = result.get("total", 0)
        logger.info("Fetched %d records; Total available: %d", len(batch), total)
        if not batch:
            logger.info("No more records returned. Ending extraction loop for MOE schools.")
            break
        records.extend(batch)
        offset += len(batch)
        if offset >= total:
            logger.info("Offset has reached total available records.")
            break
    logger.info("Completed extraction of MOE schools: %d records", len(records))
    ti = kwargs['ti']
    ti.xcom_push(key='moe_schools_extracted', value=json.dumps(records))

def extract_tertiary(**kwargs):
    """
    Extracts tertiary institutions data by scraping Wikipedia pages.
    Combines university, polytechnic and ITE college data.
    Pushes the combined data to XCom.
    """
    logger.info("Starting extraction of tertiary institutions from Wikipedia...")
    tertiary = get_university_names() + get_polytechnics() + get_ite_names()
    logger.info("Extracted %d tertiary institution records", len(tertiary))
    ti = kwargs['ti']
    ti.xcom_push(key='tertiary_extracted', value=json.dumps(tertiary))

# ============================================================================
# Transformation Functions
# ============================================================================
def transform_preschools(**kwargs):
    """
    Transforms extracted preschools data.
    Keeps relevant fields, removes duplicates and adds latitude, longitude,
    and district by calling get_postal_code_and_coords.
    Pushes the transformed data to XCom.
    """
    ti = kwargs['ti']
    records_json = ti.xcom_pull(key='preschools_extracted')
    preschools = pd.DataFrame(json.loads(records_json))
    logger.info("Transforming preschools. Initial shape: %s", preschools.shape)
    logger.info("Transforming preschools. Initial shape: %s", preschools.columns.to_list())
    preschools = preschools[preschools['centre_name'].notnull()].drop_duplicates(subset=['centre_name', 'centre_address'])
    preschools_extracted = preschools[['centre_name', 'centre_address']].copy()
    preschools_extracted.rename(columns={'centre_name': 'schoolName', 'centre_address': 'address'}, inplace=True)
    df_info = preschools_extracted[['address']].drop_duplicates()
    logger.info(f"Running API calls on unique names: {df_info.shape}")
    email = Variable.get("onemap_email")
    password = Variable.get("onemap_password")
    auth_token = extract_onemap_token(email, password)
    trasnformed_columns = process_batches(df_info, auth_token)
    preschools_extracted = preschools_extracted.merge(trasnformed_columns, on="address", how="outer")
    preschools_extracted['postalCode'] = preschools_extracted['postalCode'].fillna(preschools['postal_code'])
    logger.info(f"New Data shape: {preschools_extracted.shape}")
    preschools_extracted['category'] = 'Preschool'

    ti.xcom_push(key='preschools_transformed', value=preschools_extracted.to_json(orient='records', lines=True))
    logger.info("Transformed preschools. Final shape: %s", preschools_extracted.shape)

def transform_moe_schools(**kwargs):
    """
    Transforms extracted MOE schools data.
    Keeps relevant fields, removes duplicates and adds latitude, longitude,
    and district by calling get_postal_code_and_coords.
    Pushes the transformed data to XCom.
    """
    ti = kwargs['ti']
    records_json = ti.xcom_pull(key='moe_schools_extracted')
    moe_schools = pd.DataFrame(json.loads(records_json))
    logger.info("Transforming MOE schools. Initial shape: %s", moe_schools.shape)
    
    moe_schools = moe_schools.drop_duplicates(subset=['school_name', 'postal_code'])
    moe_schools = moe_schools[['school_name', 'address', 'postal_code', 'mainlevel_code']].copy()
    moe_schools.rename(columns={'school_name': 'schoolName', 'mainlevel_code': 'category', 'postal_code': 'postalCode'}, inplace=True)
    df_info = moe_schools[['address']].drop_duplicates()
    logger.info(f"Running API calls on unique names: {df_info.shape}")
    email = Variable.get("onemap_email")
    password = Variable.get("onemap_password")
    auth_token = extract_onemap_token(email, password)
    trasnformed_columns = process_batches(df_info, auth_token)
    moe_schools = moe_schools.merge(trasnformed_columns, on="address", how="outer")
    moe_schools.rename(columns={"postalCode_y": "postalCode"}, inplace=True)
    moe_schools = moe_schools.drop(columns=['postalCode_x'])
    logger.info(f"New Data shape: {moe_schools.shape}")

    
    ti.xcom_push(key='moe_schools_transformed', value=moe_schools.to_json(orient='records', lines=True))
    logger.info("Transformed MOE schools. Final shape: %s", moe_schools.shape)

def transform_tertiary(**kwargs):
    """
    Transforms extracted tertiary institutions data.
    Uses get_ter_school_details_google to enrich each school record with location details.
    Pushes the transformed data to XCom.
    """
    ti = kwargs['ti']
    tertiary_json = ti.xcom_pull(key='tertiary_extracted')
    tertiary = pd.DataFrame(json.loads(tertiary_json))
    logger.info("Transforming tertiary institutions. Initial shape: %s", tertiary.shape)
    
    loc_details = []
    for _, row in tertiary.iterrows():
        # including fields such as address from google maps
        loc_details.extend(get_ter_school_details_google(row['schoolName']))
    df_loc = pd.DataFrame(loc_details)
    tertiary = tertiary.merge(df_loc, on='schoolName', how='left')
    tertiary = tertiary[tertiary['google_school_name'].notnull()].drop(columns=['google_school_name'])
    tertiary = tertiary[tertiary['schoolName'] != '[71]']
    df_info = tertiary[['address']].drop_duplicates()
    logger.info(f"Running API calls on unique names: {df_info.shape}")
    email = Variable.get("onemap_email")
    password = Variable.get("onemap_password")
    auth_token = extract_onemap_token(email, password)
    trasnformed_columns = process_batches(df_info, auth_token)
    tertiary = tertiary.merge(trasnformed_columns, on="address", how="outer")
    logger.info(f"New Data shape: {tertiary.shape}")
    
    ti.xcom_push(key='tertiary_transformed', value=tertiary.to_json(orient='records', lines=True))
    logger.info("Transformed tertiary institutions. Final shape: %s", tertiary.shape)

# ============================================================================
# Load Function
# ============================================================================
def load_schools_to_bq(**kwargs):
    """
    Loads all transformed schools data by combining preschools, MOE schools, and tertiary schools.
    Standardizes string fields (trimming and converting to uppercase) and drops rows missing key values.
    Finally, pushes the combined data to XCom.
    """
    ti = kwargs['ti']
    preschools_json = ti.xcom_pull(key='preschools_transformed')
    moe_schools_json = ti.xcom_pull(key='moe_schools_transformed')
    tertiary_json = ti.xcom_pull(key='tertiary_transformed')
    
    df_preschools = pd.read_json(preschools_json, lines=True)
    df_moe = pd.read_json(moe_schools_json, lines=True)
    df_tertiary = pd.read_json(tertiary_json, lines=True)
    
    df_combined = pd.concat([df_preschools, df_moe, df_tertiary], ignore_index=True)
    logger.info("Combined data shape before standardization: %s", df_combined.shape)
    # One more validation before parsing
    if 'postalCode' in df_combined.columns:
        df_combined['postalCode'] = df_combined['postalCode'].astype(str)
        
        # Fix postal codes with less than 6 digits by left-padding with zeros
        df_combined['postalCode'] = df_combined['postalCode'].apply(lambda x: x.zfill(6) if isinstance(x, str) and len(x) < 6 else x)

        if 'district' not in df_combined.columns:
            df_combined['district'] = pd.NA

        missing_district = df_combined['district'].isna()
        df_combined.loc[missing_district, 'district'] = df_combined.loc[missing_district, 'postalCode'].apply(infer_district).astype('Int64')

    # Ensure correct data types
    int_columns = ['district']
    non_critical_columns = ['district']  # Columns we allow to have nulls

    for col in int_columns:
        if col in df_combined.columns:
            df_combined[col] = pd.to_numeric(df_combined[col], errors='coerce')
            null_count = df_combined[col].isna().sum()
            if col in non_critical_columns:
                logger.info(f"{col}: {null_count} rows will be retained with NULL values")
            else:
                logger.info(f"{col}: {null_count} rows will be dropped due to NaN")
                df_combined = df_combined.dropna(subset=[col])
            df_combined[col] = df_combined[col].astype('Int64')  # Nullable integer dtype
    
    float_columns = ['latitude', 'longitude']
    for col in float_columns:
        if col in df_combined.columns:
            df_combined[col] = pd.to_numeric(df_combined[col], errors='coerce').astype('float64')
    
    logger.info(f"Rows after unit conversion: {df_combined.shape[0]}")
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.school_data.schools_table"
    load_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    load_job = client.load_table_from_dataframe(df_combined, table_id, job_config=load_config)
    load_job.result()
    logger.info(f"Loaded {len(df_combined)} rows into BigQuery table {table_id}.")

# Bus
def extract_bus_routes(**kwargs):
    import pandas as pd
    import requests

    ti = kwargs['ti']
    headers = {
        "AccountKey": Variable.get("DATAMALL_API_KEY"),
        "accept": "application/json"
    }

    URL = "http://datamall2.mytransport.sg/ltaodataservice/BusRoutes"
    bus_routes = []
    skip = 0

    while True:
        response = requests.get(f"{URL}?$skip={skip}", headers=headers)
        data = response.json().get("value", [])
        if not data:
            break
        bus_routes.extend(data)
        skip += 500

    ti.xcom_push(key='bus_routes', value=bus_routes)

def extract_bus_stops(**kwargs):
    import pandas as pd
    import requests

    ti = kwargs['ti']
    headers = {
        "AccountKey": Variable.get("DATAMALL_API_KEY"),
        "accept": "application/json"
    }

    URL = "http://datamall2.mytransport.sg/ltaodataservice/BusStops"
    bus_stops = []
    skip = 0

    while True:
        response = requests.get(f"{URL}?$skip={skip}", headers=headers)
        data = response.json().get("value", [])
        if not data:
            break
        bus_stops.extend(data)
        skip += 500

    ti.xcom_push(key='bus_stops', value=bus_stops) 

def transform_bus_data(**kwargs):
    ti = kwargs['ti']

    bus_routes = pd.DataFrame(ti.xcom_pull(task_ids='extract_bus_routes_task', key='bus_routes'))
    bus_stops = pd.DataFrame(ti.xcom_pull(task_ids='extract_bus_stops_task', key='bus_stops'))

    bus_routes["BusStopCode"] = bus_routes["BusStopCode"].astype(str)
    bus_stops["BusStopCode"] = bus_stops["BusStopCode"].astype(str)

    bus_routes = bus_routes.merge(bus_stops, on="BusStopCode", how="inner") 

    bus_routes = pd.DataFrame(bus_routes, columns=["ServiceNo", "Operator", "Direction", "BusStopCode", "RoadName", "Description", "Latitude", "Longitude"])

    bus_stop_service_count = bus_routes.groupby("BusStopCode")["ServiceNo"].nunique().reset_index()
    bus_stop_service_count.rename(columns={"ServiceNo": "NumBusServices"}, inplace=True)
    bus_stop_details = bus_routes.drop_duplicates(subset=["BusStopCode"])[
        ["BusStopCode", "RoadName", "Description", "Latitude", "Longitude"]
    ]

    bus_stop_summary = bus_stop_details.merge(bus_stop_service_count, on="BusStopCode", how="left")
    bus_stop_summary["BusStopCode"] = bus_stop_summary["BusStopCode"].astype(str)
    bus_stop_summary_sorted = bus_stop_summary.sort_values(by="BusStopCode")

    bus_stop_summary_sorted["BusStopCode"] = bus_stop_summary_sorted["BusStopCode"].astype(str)
    bus_routes["BusStopCode"] = bus_routes["BusStopCode"].astype(str)

    bus_stop_services = bus_routes.groupby("BusStopCode")["ServiceNo"].apply(list).reset_index()
    bus_stop_services.rename(columns={"ServiceNo": "BusServicesList"}, inplace=True)
    bus_stop_summary_with_services = bus_stop_summary_sorted.merge(bus_stop_services, on="BusStopCode", how="left")

    bus_stop_summary_with_services.to_csv("Bus_Stop_With_NumOfBus_And_ServicesList.csv", index=False)

    bus_stop_services_cleaned = bus_routes.groupby("BusStopCode")["ServiceNo"].apply(lambda x: list(set(x))).reset_index()
    bus_stop_services_cleaned.rename(columns={"ServiceNo": "BusServicesList"}, inplace=True)

    bus_stop_services_cleaned["NumBusServices"] = bus_stop_services_cleaned["BusServicesList"].apply(len)

    bus_stop_summary_cleaned = bus_stop_summary_sorted.drop(columns=["NumBusServices"]).merge(
    bus_stop_services_cleaned, on="BusStopCode", how="left"
    )
    logger.info("Enriching bus stops with postal codes from OneMap API")
    email = Variable.get("onemap_email")
    password = Variable.get("onemap_password")
    auth_token = extract_onemap_token(email, password)
    bus_stop_summary_cleaned = process_batches_inplace(bus_stop_summary_cleaned, auth_token, batch_size=50, pause_seconds=60)

    ti.xcom_push(key='transformed_bus_data', value=bus_stop_summary_cleaned.to_json())

# MRT & LRT
def extract_mrt_lrt_data(**kwargs):
    """Extracts MRT and LRT station data from OneMap API"""
    station_codes = [f"EW{i}" for i in range(1, 34)] + ["CG1", "CG2"] + \
                    [f"NS{i}" for i in range(1, 29) if i != 6] + \
                    [f"CC{i}" for i in range(1, 30) if i != 18] + ["CE1", "CE2"] + \
                    [f"TE{i}" for i in range(1, 32) if i not in [10, 21]] + \
                    [f"DT{i}" for i in range(1, 38)] + \
                    ["Punggol LRT Station"] + [f"PE{i}" for i in range(1, 8)] + [f"PW{i}" for i in range(1, 8)] + \
                    ["STC"] + [f"SE{i}" for i in range(1, 6)] + [f"SW{i}" for i in range(1, 9)] + \
                    [f"BP{i}" for i in range(1, 14)]
    
    mrt_lrt_data = []
    for code in station_codes:
        url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={code}&returnGeom=Y&getAddrDetails=Y&pageNum=1"
        response = requests.get(url)
        data = response.json()
        if "results" in data and isinstance(data["results"], list):
            for result in data["results"]:
                if "SEARCHVAL" in result and ("MRT" in result["SEARCHVAL"] or "LRT" in result["SEARCHVAL"]):
                    mrt_lrt_data.append(result)
                    break

    kwargs['ti'].xcom_push(key='mrt_lrt_data', value=mrt_lrt_data)

def transform_mrt_lrt_data(**kwargs):
    """Transforms MRT and LRT data"""
    ti = kwargs['ti']
    mrt_lrt_data = pd.DataFrame(ti.xcom_pull(task_ids='extract_mrt_lrt_data_task', key='mrt_lrt_data'))

    mrt_lrt_data = mrt_lrt_data.drop_duplicates()
    mrt_lrt_data.drop(columns=['SEARCHVAL', 'BLK_NO', 'X', 'Y'], inplace=True)
    mrt_lrt_data.rename(columns={'ROAD_NAME': 'roadName', 'BUILDING': 'building', 'ADDRESS': 'address',
                                 'POSTAL': 'postalCode', 'LATITUDE': 'latitude', 'LONGITUDE': 'longitude'}, inplace=True)
    mrt_lrt_data['district'] = mrt_lrt_data['postalCode'].apply(infer_district)
    ti.xcom_push(key='transformed_mrt_lrt_data', value=mrt_lrt_data.to_json())

def load_bus_data_to_bq(**kwargs):
    """Loads transformed bus data into BigQuery"""
    ti = kwargs['ti']
    transformed_data_json = ti.xcom_pull(task_ids='transform_bus_data_task', key='transformed_bus_data')
    df = pd.read_json(transformed_data_json)
    logger.info("Combined data shape before standardization: %s", df.shape)
    # One more validation before parsing
    if 'postalCode' in df.columns:
        df['postalCode'] = df['postalCode'].astype(str)
        
        # Fix postal codes with less than 6 digits by left-padding with zeros
        df['postalCode'] = df['postalCode'].apply(lambda x: x.zfill(6) if isinstance(x, str) and len(x) < 6 else x)
        
        if 'district' not in df.columns:
            df['district'] = pd.NA

        missing_district = df['district'].isna()
        df.loc[missing_district, 'district'] = df.loc[missing_district, 'postalCode'].apply(infer_district).astype('Int64')

    # Ensure correct data types
    int_columns = ['district']
    non_critical_columns = ['district']  # Columns we allow to have nulls

    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            null_count = df[col].isna().sum()
            if col in non_critical_columns:
                logger.info(f"{col}: {null_count} rows will be retained with NULL values")
            else:
                logger.info(f"{col}: {null_count} rows will be dropped due to NaN")
                df = df.dropna(subset=[col])
            df[col] = df[col].astype('Int64')  # Nullable integer dtype
    
    float_columns = ['latitude', 'longitude']
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
    
    logger.info(f"Rows after unit conversion: {df.shape[0]}")
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.transport.bus_stop_table"
    load_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    load_job = client.load_table_from_dataframe(df, table_id, job_config=load_config)
    load_job.result()
    logger.info(f"Loaded {len(df)} rows into BigQuery table {table_id}.")

def load_mrt_lrt_data_to_bq(**kwargs):
    """Loads transformed MRT and LRT data into BigQuery"""
    ti = kwargs['ti']
    transformed_data_json = ti.xcom_pull(task_ids='transform_mrt_lrt_data_task', key='transformed_mrt_lrt_data')
    df = pd.read_json(transformed_data_json)
    logger.info("Combined data shape before standardization: %s", df.shape)
    # One more validation before parsing
    if 'postalCode' in df.columns:
        df['postalCode'] = df['postalCode'].astype(str)
        
        # Fix postal codes with less than 6 digits by left-padding with zeros
        df['postalCode'] = df['postalCode'].apply(lambda x: x.zfill(6) if isinstance(x, str) and len(x) < 6 else x)
        
        if 'district' not in df.columns:
            df['district'] = pd.NA

        missing_district = df['district'].isna()
        df.loc[missing_district, 'district'] = df.loc[missing_district, 'postalCode'].apply(infer_district).astype('Int64')

    # Ensure correct data types
    int_columns = ['district']
    non_critical_columns = ['district']  # Columns we allow to have nulls

    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            null_count = df[col].isna().sum()
            if col in non_critical_columns:
                logger.info(f"{col}: {null_count} rows will be retained with NULL values")
            else:
                logger.info(f"{col}: {null_count} rows will be dropped due to NaN")
                df = df.dropna(subset=[col])
            df[col] = df[col].astype('Int64')  # Nullable integer dtype
    
    float_columns = ['latitude', 'longitude']
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
    
    logger.info(f"Rows after unit conversion: {df.shape[0]}")
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.transport.mrt_lrt_table"
    load_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    load_job = client.load_table_from_dataframe(df, table_id, job_config=load_config)
    load_job.result()
    logger.info(f"Loaded {len(df)} rows into BigQuery table {table_id}.")

with DAG(
    dag_id="transport_dag",
    schedule_interval="@monthly",
    default_args=default_args, 
    catchup=False, 
    tags=["transport"],
) as dag:

    extract_bus_routes_task = PythonOperator(
        task_id="extract_bus_routes_task",
        python_callable=extract_bus_routes,
        provide_context=True
    )

    extract_bus_stops_task = PythonOperator(
        task_id="extract_bus_stops_task",
        python_callable=extract_bus_stops,
        provide_context=True
    )

    extract_mrt_lrt_data_task = PythonOperator(
        task_id="extract_mrt_lrt_data_task",
        python_callable=extract_mrt_lrt_data,
        provide_context=True
    )

    transform_bus_data_task = PythonOperator(
        task_id="transform_bus_data_task",
        python_callable=transform_bus_data,
        provide_context=True
    )

    transform_mrt_lrt_data_task = PythonOperator(
        task_id="transform_mrt_lrt_data_task",
        python_callable=transform_mrt_lrt_data,
        provide_context=True
    )

    load_bus_data_task = PythonOperator(
        task_id="load_bus_data_task",
        python_callable=load_bus_data_to_bq,
        provide_context=True
    )

    load_mrt_lrt_data_task = PythonOperator(
        task_id="load_mrt_lrt_data_task",
        python_callable=load_mrt_lrt_data_to_bq,
        provide_context=True
    )

    extract_preschools_task = PythonOperator(
        task_id="extract_preschools_task",
        python_callable=extract_preschools,
        provide_context=True
    )

    extract_moe_schools_task = PythonOperator(
        task_id="extract_moe_schools_task",
        python_callable=extract_moe_schools,
        provide_context=True
    )

    extract_tertiary_task = PythonOperator(
        task_id="extract_tertiary_task",
        python_callable=extract_tertiary,
        provide_context=True
    )

    transform_preschools_task = PythonOperator(
        task_id="transform_preschools_task",
        python_callable=transform_preschools,
        provide_context=True
    )

    transform_moe_schools_task = PythonOperator(
        task_id="transform_moe_schools_task",
        python_callable=transform_moe_schools,
        provide_context=True
    )

    transform_tertiary_task = PythonOperator(
        task_id="transform_tertiary_task",
        python_callable=transform_tertiary,
        provide_context=True
    )

    load_schools_task = PythonOperator(
        task_id="load_schools_task",
        python_callable=load_schools_to_bq,
        provide_context=True
    )

    [extract_bus_routes_task, extract_bus_stops_task] >> transform_bus_data_task >> load_bus_data_task
    extract_mrt_lrt_data_task >> transform_mrt_lrt_data_task >> load_mrt_lrt_data_task
    extract_preschools_task >> transform_preschools_task
    extract_moe_schools_task >> transform_moe_schools_task
    extract_tertiary_task >> transform_tertiary_task
    [transform_preschools_task, transform_moe_schools_task, transform_tertiary_task] >> load_schools_task