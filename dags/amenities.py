from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum
import requests
import pandas as pd
import time
import json
from google.cloud import bigquery
from google.cloud import storage
from bs4 import BeautifulSoup
import geopandas as gpd
import re 
from io import StringIO
import zipfile
import io

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

# Function for webscrapping ActiveSG data
# Author: Shi Ying, edited by Kai Jun
def extract_activeSG_data(**kwargs):
    """
    EXTRACT task: Webscrapes ActiveSG website for list of facilities.
    """
    logger.info("Starting activeSG data extraction...")
    # Base URL for pages 2 onwards
    base_url = "https://www.activesgcircle.gov.sg/facilities?page={}"

    # List to store scraped data
    facilities_data = []

    # Function to scrape a given URL
    def scrape_page(page_num):
        url = base_url.format(page_num)
        logger.info(f"Scraping page {page_num}...")  # Debugging output
        
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            logger.warning(f"❌ Failed to retrieve page {page_num}, skipping...")
            return None  # Return None to indicate failure

        # Parse the page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all facility blocks
        cst_cnt_divs = soup.find_all('div', class_='cst-cnt')

        for cst_cnt_div in cst_cnt_divs:
            # Extract facility name (filter out unwanted h4 tags)
            h4_tags = cst_cnt_div.find_all('h4')
            excluded_texts = {"Operating Hours", "Phone Number", "Address"}
            facility_names = [h4.get_text(strip=True) for h4 in h4_tags if h4.get_text(strip=True) not in excluded_texts]

            # Extract additional details
            address = cst_cnt_div.find('div', class_='cst-address')
            type_of_facility = cst_cnt_div.find('div', class_='cst-type-of-facility')
            direction = cst_cnt_div.find('div', class_='cst-direction')

            # Extract text safely
            address_text = address.get_text(strip=True) if address else "N/A"
            type_text = type_of_facility.get_text(strip=True) if type_of_facility else "N/A"
            direction_text = direction.get_text(strip=True) if direction else "N/A"

            # Add data to list
            for facility_name in facility_names:
                facilities_data.append({
                    "name": facility_name,
                    "address": address_text,
                    "facilityType": type_text,
                    "direction": direction_text
                })
        
        return soup  # Return soup object for pagination detection

    # Scrape first page separately
    soup = scrape_page(1)
    logger.info(soup)
    if soup:
        # Find the div containing the last page link (using the nested structure provided)
        post_nav = soup.find('div', id='customPagination')
        if post_nav:
            last_nav = post_nav.find('a', class_='next-posts-link last-nav')
            if last_nav and 'href' in last_nav.attrs:
                # Extract the page number from the href attribute (e.g., "?page=40")
                last_page = int(last_nav.attrs['href'].split('=')[-1])
            else:
                last_page = 1  # If the last page navigation is not found, assume there's only one page
        else:
            last_page = 1  # If pagination div is not found, assume one page

        logger.info(f"Total pages detected: {last_page}")

        # Scrape remaining pages dynamically
        for page in range(2, last_page + 1):
            scrape_page(page)
            time.sleep(1)  # Be polite! Avoid being blocked

    df = pd.DataFrame(facilities_data)

    kwargs['ti'].xcom_push(key='extracted_activeSG_amenities', value=df.to_json(orient='records'))
    logger.info(f"Extracted {len(df)} rows from ActiveSG amenities data.")

# Function for transforming ActiveSG data
# Author: Shi Ying, edited by Kai Jun
def transform_activeSG_data(auth_token, **kwargs):
    """
    TRANSFORM task: pulls the raw extracted data from XCom, applies transformations,
    then pushes the transformed data (as JSON) back to XCom.
    """
    logger.info("Starting activeSG data transformation...")
    ti = kwargs['ti']
    extracted_json = ti.xcom_pull(task_ids='extract_activeSG_task', key='extracted_activeSG_amenities')
    if extracted_json:
        df = pd.read_json(extracted_json, orient='records')
    else:
        df = pd.DataFrame()

    df_info = df[['address']].drop_duplicates()
    logger.info(f"Running API calls on unique names: {df_info.shape}")
    trasnformed_columns = process_batches(df_info, auth_token)
    df = df.merge(trasnformed_columns, on="address", how="outer")
    df['id'] = df.apply(lambda row: f"{row['name']} {row['address']}", axis=1)
    logger.info(f"Total rows before deduplication: {df.shape[0]}")
    df.drop_duplicates(subset=["id"], inplace=True)
    logger.info(f"Rows after deduplication in Python: {df.shape[0]}")
    transformed_json = df.to_json(orient='records')
    ti.xcom_push(key='transformed_activeSG', value=transformed_json)
    logger.info(f"transformed_activeSG_data shape: {df.shape}")

# Function for loading to BQ
# Author: Kai Jun
def load_activeSG_data_to_bq(**kwargs):
    logger.info("Starting to load ActiveSG data into BigQuery...")
    ti = kwargs['ti']
    transformed_json = ti.xcom_pull(task_ids='transform_activeSG_task', key='transformed_activeSG')
    if transformed_json:
        df = pd.read_json(transformed_json, orient='records')
    else:
        df = pd.DataFrame()
    logger.info(f"Total rows in activeSG: {df.shape[0]}")
    # One more validation before parsing
    if 'postalCode' in df.columns:
        df['postalCode'] = df['postalCode'].astype(str)
        
        # Fix postal codes with less than 6 digits by left-padding with zeros
        df['postalCode'] = df['postalCode'].apply(lambda x: x.zfill(6) if isinstance(x, str) and len(x) < 6 else x)
        
        # Fill district if missing or incorrect
        def infer_district(postal):
            if postal and isinstance(postal, str) and len(postal) == 6 and postal[:2].isdigit():
                return postal_to_district.get(postal[:2])
            return pd.NA

        if 'district' not in df.columns:
            df['district'] = pd.NA

        missing_district = df['district'].isna()
        df.loc[missing_district, 'district'] = df.loc[missing_district, 'postalCode'].apply(infer_district).astype(str)


    # # Ensure correct data types
    str_columns = ['district']
    non_critical_columns = ['district']  # Columns we allow to have nulls

    for col in str_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            null_count = df[col].isna().sum()
            if col in non_critical_columns:
                logger.info(f"{col}: {null_count} rows will be retained with NULL values")
            else:
                logger.info(f"{col}: {null_count} rows will be dropped due to NaN")
                df = df.dropna(subset=[col])
            df[col] = df[col].astype(str)
    
    float_columns = ['latitude', 'longitude']
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
    
    logger.info(f"Rows after unit conversion: {df.shape[0]}")
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.amenities.activeSG_table"
    load_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    load_job = client.load_table_from_dataframe(df, table_id, job_config=load_config)
    load_job.result()
    logger.info(f"Loaded {len(df)} rows into BigQuery table {table_id}.")

# Function for extracting healthcare data
# Author: Shi Ying, edited by Kai Jun
def extract_healthcare_data(url, **kwargs):
    response = requests.get(url)
    # if error
    if response.status_code != 200:
        logger.error(f"Failed to fetch from URL, attempting to fetch from GCS bucket.")
        # Step 1: Pull the GeoJSON from GCS if URL fails
        bucket_name = "dag-related"
        destination_blob_name = "hotosm_sgp_health_facilities.geojson"
        storage_client = storage.Client()

        # Get the GCS bucket and blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Download the file content
        geojson_data = blob.download_as_text()

        # Read the GeoJSON into GeoDataFrame using geopandas
        gdf = gpd.read_file(io.StringIO(geojson_data))

    # Step 2: If not error, Unzip in memory
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # Find the .geojson file inside
        geojson_file = [f for f in z.namelist() if f.endswith(".geojson")][0]
        
        # Step 3: Read the GeoJSON file with geopandas
        with z.open(geojson_file) as geojson:
            gdf = gpd.read_file(geojson)
    gdf = gdf[['name','amenity','geometry']]
    # Extract longitude and latitude from the geometry column
    gdf['longitude'] = gdf['geometry'].apply(lambda point: point.x)  # Longitude is the x-coordinate
    gdf['latitude'] = gdf['geometry'].apply(lambda point: point.y)   # Latitude is the y-coordinate

    # Drop the 'geometry' column now 
    gdf = gdf.drop(columns=['geometry'])
    kwargs['ti'].xcom_push(key='extracted_healthcare_amenities', value=gdf.to_json(orient='records'))
    logger.info(f"Extracted {len(gdf)} rows from Healthcare geojson amenities data.")

# Helper Function to perform reverse geocoding with OneMap API
# Author: Shi Ying, edited by Kai Jun
def fetch_onemap_facility(auth_token, lat, lon):
    url = f"https://www.onemap.gov.sg/api/public/revgeocode?location={lat},{lon}&buffer=40&addressType=All&otherFeatures=N"
    headers = {'Authorization': f'Bearer {auth_token}'}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if 'GeocodeInfo' in data and len(data['GeocodeInfo']) > 0:
                info = data['GeocodeInfo'][0]
                return {
                    'building_name': info.get('BUILDINGNAME'),
                    'block': info.get('BLOCK'),
                    'road': info.get('ROAD'),
                    'postal': info.get('POSTALCODE')
                }
    except Exception as e:
        print(f"Error fetching OneMap info for lat={lat}, lon={lon}: {e}")

    return {} # If no address found

# Function for transforming healthcare data
# Author: Shi Ying, edited by Kai Jun
def transform_healthcare_data(auth_token, batch_size=50, rate_limit=250, **kwargs):
    """
    TRANSFORM task: pulls the raw extracted data from XCom, applies transformations,
    then pushes the transformed data (as JSON) back to XCom.
    """
    logger.info("Starting healthcare data transformation...")
    ti = kwargs['ti']
    extracted_json = ti.xcom_pull(task_ids='extract_healthcare_task', key='extracted_healthcare_amenities')
    if extracted_json:
        gdf = pd.read_json(extracted_json, orient='records')
    else:
        gdf = pd.DataFrame()
    gdf = gdf.copy()

    # logic for batching
    total_rows = len(gdf)
    num_batches = (total_rows // batch_size) + (1 if total_rows % batch_size else 0)
    
    print(f"Starting enrichment in {num_batches} batches of {batch_size}")
    
    request_count = 0

    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_rows)
        batch_df = gdf.iloc[start_idx:end_idx]

        for index, row in batch_df.iterrows():
            lat, lon = row['latitude'], row['longitude']
            info = fetch_onemap_facility(auth_token, lat, lon)
            request_count += 1

            # Fill in name if missing
            if pd.isna(row.get('name')) and info.get('building_name'):
                gdf.at[index, 'name'] = info['building_name']
            
            # Construct address
            block = info.get('block', '')
            road = info.get('road', '')
            postal = info.get('postal')
            if block or road or postal:
                gdf.at[index, 'address'] = f"{block} {road} {postal}".strip()
            
            # Add postalCode
            if postal:
                gdf.at[index, 'postalCode'] = postal

                # Map to district
                if isinstance(postal, str) and len(postal) >= 2:
                    postal_prefix = postal[:2]
                    district = postal_to_district.get(postal_prefix)
                    gdf.at[index, 'district'] = district

            # Rate limiting pause if needed
            if request_count >= rate_limit:
                print(f"Rate limit of {rate_limit} reached. Sleeping for 60 seconds...")
                time.sleep(60)
                request_count = 0  # reset after cooldown

        print(f"Completed batch {i + 1}/{num_batches}")

    # Check for duplicates --> not based on name because could be same hospital/clinics with mulitple outlets then these count as distinct
    gdf = gdf.drop_duplicates(subset=["name", 'longitude', 'latitude'])
    gdf['id'] = gdf.apply(lambda row: f"{row['name']} {row['address']}", axis=1)

    transformed_json = gdf.to_json(orient='records')
    ti.xcom_push(key='transformed_healthcare', value=transformed_json)
    logger.info(f"transformed_healthcare shape: {gdf.shape}")

# Function for loading to BQ
# Author: Kai Jun
def load_healthcare_data_to_bq(**kwargs):
    logger.info("Starting to load healthcare data into BigQuery...")
    ti = kwargs['ti']
    transformed_json = ti.xcom_pull(task_ids='transform_healthcare_task', key='transformed_healthcare')
    if transformed_json:
        df = pd.read_json(transformed_json, orient='records')
    else:
        df = pd.DataFrame()
    logger.info(f"Total rows in healthcare: {df.shape[0]}")
    # One more validation before parsing
    if 'postalCode' in df.columns:
        df['postalCode'] = df['postalCode'].astype(str)
        
        # Fix postal codes with less than 6 digits by left-padding with zeros
        df['postalCode'] = df['postalCode'].apply(lambda x: x.zfill(6) if isinstance(x, str) and len(x) < 6 else x)
        
        # Fill district if missing or incorrect
        def infer_district(postal):
            if postal and isinstance(postal, str) and len(postal) == 6 and postal[:2].isdigit():
                return postal_to_district.get(postal[:2])
            return pd.NA

        if 'district' not in df.columns:
            df['district'] = pd.NA

        missing_district = df['district'].isna()
        df.loc[missing_district, 'district'] = df.loc[missing_district, 'postalCode'].apply(infer_district).astype(str)


    # # Ensure correct data types
    str_columns = ['district']
    non_critical_columns = ['district']  # Columns we allow to have nulls

    for col in str_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            null_count = df[col].isna().sum()
            if col in non_critical_columns:
                logger.info(f"{col}: {null_count} rows will be retained with NULL values")
            else:
                logger.info(f"{col}: {null_count} rows will be dropped due to NaN")
                df = df.dropna(subset=[col])
            df[col] = df[col].astype(str)
    
    float_columns = ['latitude', 'longitude']
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
    
    logger.info(f"Rows after unit conversion: {df.shape[0]}")
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.amenities.healthcare_table"
    load_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    load_job = client.load_table_from_dataframe(df, table_id, job_config=load_config)
    load_job.result()
    logger.info(f"Loaded {len(df)} rows into BigQuery table {table_id}.")

# Function for webscraping mall data
# Author: Xin Tong, edited by Kai Jun
def extract_malls_wikipedia(**kwargs):

    ti = kwargs['ti']
    wiki_url = "https://en.wikipedia.org/wiki/List_of_shopping_malls_in_Singapore"
    wiki_response = requests.get(wiki_url, timeout = 5)
    content = BeautifulSoup(wiki_response.content, "html.parser")
    #---- 1. Extract mall names from main section
    mall_names_main_section = []

    # Mall names can be found under each region header
    regions = content.find_all("h2")

    for region in regions:
        region_text = region.get_text(strip=True)

        # Skip 'Contents' and 'References' Section
        if region_text in ["Contents", "References"]:
            continue

        div_col = region.find_next("div", class_="div-col")
        
        if div_col:
            malls = div_col.find_all("li")  # Extract malls from <li> tags

            for mall in malls:
                # Some malls are in <a> tags, some are plain text
                mall_name = mall.get_text(strip=True)
                if mall_name:
                    mall_names_main_section.append(mall_name)

    #---- 2. Extract mall names from references section (Current malls)

    # Find the box containing current mall names
    mall_names_ref_section = []
    wiki_ref_mall_section = content.find("td", class_="navbox-list-with-group navbox-list navbox-odd hlist")

    if wiki_ref_mall_section:
        # Locate only the <ul> lists inside the section to avoid the headers
        for ul in wiki_ref_mall_section.find_all("ul"):
            # Extract mall names from <a> tags
            for a in ul.find_all("a"):
                mall_name = a.text.strip()
                mall_names_ref_section.append(mall_name)
    else: 
        print("Current malls section under references not found.")

    #---- 3. Combine mall names obtained from both sections to get a complete list

    combined_mall_names = mall_names_main_section.copy()
    combined_mall_names.extend(mall_names_ref_section)
    combined_mall_names.sort()

    #---- 4. Cleaning of web-scrapped data
    cleansed_wiki_mall_names = combined_mall_names.copy()

    #--  Standardise all to be uppercase
    cleansed_wiki_mall_names = [mall.upper() for mall in cleansed_wiki_mall_names]

    #-- Remove whitespace
    cleansed_wiki_mall_names = [mall.strip() for mall in cleansed_wiki_mall_names]

    #-- Clean mall names 
    # e.g. GRID (FORMERLY POMO)[1], KINEX (FORMERLY ONEKM)
    def clean_mall_names(dataset):
        result = []
        for i in dataset:
            # Remove content inside square brackets [ ] including the brackets
            i = re.sub(r"\[.*?\]", "", i)
            # Remove the bracketed part for 'formerly' cases
            i = re.sub(r"\s*\(FORMERLY .*?\)", "", i, flags=re.IGNORECASE)
            result.append(i.strip())
        return result

    cleansed_wiki_mall_names = clean_mall_names(cleansed_wiki_mall_names)

    #-- Remove duplicates
    def remove_duplicates(mall_list):
        result = []
        for i in mall_list:
            if i not in result:
                result.append(i)
        return result

    cleansed_wiki_mall_names = remove_duplicates(cleansed_wiki_mall_names)

    #---- 5. Manual Cleaning of web-scrapped data
    wiki_replacement_dict = {
        'PAYA LEBAR QUARTER (PLQ)': 'PLQ MALL',
        'THE PARAGON': 'PARAGON SHOPPING CENTRE',
        'DJITSUN MALL BEDOK': 'DJITSUN MALL'
    }
    cleansed_wiki_mall_names = [wiki_replacement_dict.get(mall, mall) for mall in cleansed_wiki_mall_names]

    wiki_malls_to_remove = {'TENGAH MALL (2027)', 'FAIRPRICE HUB', 'MARINA BAY SANDS', 'HOLLAND VILLAGE SHOPPING MALL', 'OD MALL', 'HOUGANG GREEN SHOPPING MALL'}
    cleansed_wiki_mall_names = [mall for mall in cleansed_wiki_mall_names if mall not in wiki_malls_to_remove]
    ti.xcom_push(key='wiki_malls', value=cleansed_wiki_mall_names)

# Helper function for extracting mall data using google API
# Author: Xin Tong, edited by Kai Jun
def get_mall_names_text_search(GOOGLE_API_KEY, TEXT_SEARCH_URL):
    malls = []
    next_page_token = None

    while True:
        params = {
            "query": "shopping malls in Singapore",
            "region": "sg",
            "key": GOOGLE_API_KEY
        }
        if next_page_token:
            params["pagetoken"] = next_page_token

        response = requests.get(TEXT_SEARCH_URL, params=params)
        data = response.json()

        if "results" in data:
            for result in data["results"]:
                malls.append(result.get("name", ""))  # Only store the name

        next_page_token = data.get("next_page_token")
        if not next_page_token:
            break

        time.sleep(3)  # To handle rate limits

    return malls

# Function for extracting mall data using google API
# Author: Xin Tong, edited by Kai Jun
def extract_malls_google(**kwargs):
    ti = kwargs['ti']
    GOOGLE_API_KEY = Variable.get("GOOGLE_API_KEY")
    TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    text_search_mall_names = get_mall_names_text_search(GOOGLE_API_KEY, TEXT_SEARCH_URL)

    # Manual cleaning
    text_search_malls_to_remove = ['CLARKE QUAY', 'UNITED SQUARE SHOPPING MALL', 'GREAT WORLD CITY', 'IMM BUILDING', 'TAKASHIMAYA SHOPPING CENTRE']
    text_search_mall_names = [mall.upper() for mall in text_search_mall_names]
    text_search_mall_names = [mall for mall in text_search_mall_names if mall not in text_search_malls_to_remove]
    ti.xcom_push(key='google_malls', value=text_search_mall_names)

# Helper Function for postal code, coords and address using name
# Author: Xin Tong, edited by Kai Jun
def get_postal_code_address_and_coords(name, token):
    base_url = "https://www.onemap.gov.sg/api/common/elastic/search"
    params = {
        "searchVal": name,
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
                address = result.get("ADDRESS")
                district = None
                if postal and isinstance(postal, str) and len(postal) >= 2:
                    # If postal is not 6 digits long, left-pad with zeros
                    if len(postal) != 6:
                        postal = postal.zfill(6)
                    postal_prefix = postal[:2]
                    district = postal_to_district.get(postal_prefix)
                return pd.Series([name, address, postal, latitude, longitude, district])
    except Exception as e:
        logger.warning(f"Exception for address {name}: {e}")
    return pd.Series([None, None, None, None, None, None])

# Helper Function for batching API calls to get postal code, coords and address using name
# Author: edited by Kai Jun
def process_batches_name(df, token, batch_size=50, rate_limit=250):
    logger.info("Running process_batches_name")
    num_batches = (len(df) // batch_size) + (1 if len(df) % batch_size != 0 else 0)
    results = []

    for i in range(num_batches):
        batch_df = df.iloc[i * batch_size : (i + 1) * batch_size]
        logger.info(f"Processing batch {i + 1}/{num_batches} with {len(batch_df)} addresses...")

        batch_results = batch_df['name'].apply(lambda name: get_postal_code_address_and_coords(name, token))
        results.append(batch_results)

        if ((i + 1) * batch_size) % rate_limit == 0:
            logger.info("Rate limit reached, pausing for 60 seconds...")
            time.sleep(60)

    final_results = pd.concat(results, ignore_index=True)
    final_results.columns = ['name', 'address', 'postalCode', 'latitude', 'longitude', 'district']
    return final_results

# Function to transform mall data
# Author: Kai Jun
def transform_malls_data(auth_token, **kwargs):
    ti = kwargs['ti']
    wiki = ti.xcom_pull(task_ids='extract_malls_wikipedia_task', key='wiki_malls')
    google = ti.xcom_pull(task_ids='extract_malls_google_task', key='google_malls')
    combined = list(set(wiki + google))  # deduplicate
    df = pd.DataFrame(combined, columns=['name'])
    logger.info(f"Running API calls on unique names: {df.shape}")
    tramsformed_columns = process_batches_name(df, auth_token)
    df = df.merge(tramsformed_columns, on="name", how="outer")

    logger.info(f"Total rows before deduplication: {df.shape[0]}")
    df.drop_duplicates(subset=["name"], inplace=True)
    df['id'] = df.apply(lambda row: f"{row['name']} {row['address']}", axis=1)
    df.drop_duplicates(subset=["id"], inplace=True)
    logger.info(f"Rows after deduplication in Python: {df.shape[0]}")
    ti.xcom_push(key='transformed_malls', value=df.to_json(orient='records'))

# Function for loading to BQ
# Author: Kai Jun
def load_malls_data_to_bq(**kwargs):
    logger.info("Starting to load malls data into BigQuery...")
    ti = kwargs['ti']
    transformed_json = ti.xcom_pull(task_ids='transform_malls_task', key='transformed_malls')
    if transformed_json:
        df = pd.read_json(transformed_json, orient='records')
    else:
        df = pd.DataFrame()
    df = df.drop_duplicates(subset=["name"])
    logger.info(f"Total rows in malls: {df.shape[0]}")

    # Hard code some special cases
    # Define the hardcoded name-to-postalCode mapping
    manual_postal_map = {
        "ANCHORVALE VILLAGE": "540339",
        "ADMIRALTY PLACE": "731678",
        "CLARKE QUAY CENTRAL": "059817"
    }

    # Apply mapping only to rows that match and preserve others
    df['postalCode'] = df.apply(
        lambda row: manual_postal_map.get(row['name'], row['postalCode']),
        axis=1
    )

    # One more validation before parsing
    if 'postalCode' in df.columns:
        df['postalCode'] = df['postalCode'].astype(str)
        
        # Fix postal codes with less than 6 digits by left-padding with zeros
        df['postalCode'] = df['postalCode'].apply(lambda x: x.zfill(6) if isinstance(x, str) and len(x) < 6 else x)
        
        # Fill district if missing or incorrect
        def infer_district(postal):
            if postal and isinstance(postal, str) and len(postal) == 6 and postal[:2].isdigit():
                return postal_to_district.get(postal[:2])
            return pd.NA

        if 'district' not in df.columns:
            df['district'] = pd.NA

        missing_district = df['district'].isna()
        df.loc[missing_district, 'district'] = df.loc[missing_district, 'postalCode'].apply(infer_district).astype(str)

    # # Ensure correct data types
    str_columns = ['district']
    non_critical_columns = ['district']  # Columns we allow to have nulls

    for col in str_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            null_count = df[col].isna().sum()
            if col in non_critical_columns:
                logger.info(f"{col}: {null_count} rows will be retained with NULL values")
            else:
                logger.info(f"{col}: {null_count} rows will be dropped due to NaN")
                df = df.dropna(subset=[col])
            df[col] = df[col].astype(str)
    
    float_columns = ['latitude', 'longitude']
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
    
    logger.info(f"Rows after unit conversion: {df.shape[0]}")
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.amenities.malls_table"
    load_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    load_job = client.load_table_from_dataframe(df, table_id, job_config=load_config)
    load_job.result()
    logger.info(f"Loaded {len(df)} rows into BigQuery table {table_id}.")

# Functions for extractions
# Author: Kai Jun
def extract_amenities_data(dataset_id, data_type, **kwargs):
    """
    EXTRACT task: Uses the public API from data.gov.sg to download raw amenities data.
    
    :param dataset_id: The dataset identifier.
    :param data_type: A string label (e.g. "hawkerCentre", "communityClub") used to tag the data.
    """
    logger.info(f"Starting extraction for {data_type} with dataset id {dataset_id}...")
    url = f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/poll-download"
    response = requests.get(url)
    json_data = response.json()
    if json_data.get('code') != 0:
        logger.error(f"Error: {json_data.get('errMsg')}")
        return
    download_url = json_data['data']['url']
    response = requests.get(download_url)
    raw_data = response.text
    # Push the raw JSON (or CSV text) to XCom with a key specific to the data_type.
    kwargs['ti'].xcom_push(key=f"extracted_{data_type}", value=raw_data)
    logger.info(f"Extraction for {data_type} completed. Data length: {len(raw_data)} characters.")

# Helper function for pharsing GEOJSON data
# Author: Shi Ying
def extract_description_info(description):
        soup = BeautifulSoup(description, "html.parser")
        data = {}

        # Extract the table rows from the HTML
        rows = soup.find_all('tr')

        # Loop through each row and extract the column names and values
        for row in rows:
            th_elements = row.find_all('th')
            td_elements = row.find_all('td')
            
            # Check if both <th> and <td> exist before extracting
            if th_elements and td_elements:
                key = th_elements[0].get_text(strip=True)
                value = td_elements[0].get_text(strip=True)
                data[key] = value

        return data

# Functions for transformations
# Author: Shi Ying, edited by Kai Jun

# Construct the address column 
def build_address(row):
    parts = []
    if pd.notna(row.get('blockNumber')):
        parts.append(str(row['blockNumber']).strip())
    if pd.notna(row.get('streetName')):
        parts.append(str(row['streetName']).strip())

    postal = row.get('postalCode')
    # only include postal if it's a non‑empty string of digits
    if pd.notna(postal):
        postal_str = str(postal).strip()
        if postal_str.isdigit():
            parts.append(str(int(postal_str)))  # drop any leading zeros, if you like
        else:
            logger.debug("Skipping invalid postalCode: %r", postal_str)

    return " ".join(parts)


def transform_amenities_data(data_type, auth_token, **kwargs):
    """
    TRANSFORM task: Pulls raw extracted data from XCom, applies transformation logic,
    then pushes the transformed data (as JSON) back to XCom.

    For example, if data_type is "hawkerCentre", it assumes the raw JSON is a GeoJSON
    FeatureCollection, extracts each feature's properties, parses the HTML in the "Description"
    field (if present), and standardizes the columns to ['name', 'address', 'postalCode', 'latitude', 'longitude'].
    """
    logger.info(f"Starting transformation for {data_type}...")
    ti = kwargs['ti']
    raw_json = ti.xcom_pull(task_ids=f"extract_{data_type}_task", key=f"extracted_{data_type}")
    gdf = gpd.read_file(raw_json)
    description_data = gdf['Description'].apply(extract_description_info)

    # Convert the extracted data into a DataFrame
    description_df = pd.json_normalize(description_data)

    # Combine the new columns with the original GeoDataFrame (without overwriting the existing ones)
    gdf = pd.concat([gdf, description_df], axis=1)
 
    if data_type == "hawkerCentre":
        gdf = gdf.rename(columns={
                'NAME': 'name',
                'ADDRESSBLOCKHOUSENUMBER': 'blockNumber', 
                'ADDRESSSTREETNAME': 'streetName',
                'ADDRESSPOSTALCODE': 'postalCode',
                'DESCRIPTION': 'hawkerStatus'
            })
        # Add address column 
        gdf['address'] = gdf.apply(build_address, axis=1)

        # Rearrange columns
        gdf = gdf[['name', 'address', 'postalCode', 'hawkerStatus']]

        # Sort the GeoDataFrame by name alphabetically
        gdf = gdf.sort_values(by='name', ascending=True)
    elif data_type == "communityClub":
        gdf = gdf.rename(columns={
                'ADDRESSBLOCKHOUSENUMBER': 'blockNumber',
                'NAME': 'name',
                'ADDRESSSTREETNAME': 'streetName',
                'ADDRESSPOSTALCODE': 'postalCode',
                'DESCRIPTION' : 'description'
            })
        # Add address column 
        gdf['address'] = gdf.apply(build_address, axis=1)

        # Rearrange columns
        gdf = gdf[['name', 'address', 'postalCode', 'description']]

        # Sort the GeoDataFrame by name alphabetically
        gdf = gdf.sort_values(by='name', ascending=True)
    elif data_type == "supermarket":
        gdf = gdf.rename(columns={
            'LIC_NAME': 'name',
            'BLK_HOUSE': 'blockNumber',
            'STR_NAME': 'streetName',
            'POSTCODE': 'postalCode',
        })
        # Add address column 
        gdf['address'] = gdf.apply(build_address, axis=1)

        # Rearrange columns
        gdf = gdf[['name', 'address', 'postalCode']]

        # Sort the GeoDataFrame by name alphabetically
        gdf = gdf.sort_values(by='name', ascending=True)
    else:
        logger.info("No special transformation applied for this data type.")

    df_info = gdf[['address']].drop_duplicates()
    logger.info(f"Running API calls on unique names: {df_info.shape}")
    trasnformed_columns = process_batches(df_info, auth_token)
    gdf = gdf.merge(trasnformed_columns, on="address", how="outer")
    gdf = gdf.rename(columns={'postalCode_x': 'postalCode'})
    gdf = gdf.drop(columns='postalCode_y')
    gdf['id'] = gdf.apply(lambda row: f"{row['name']} {row['address']}", axis=1)
    logger.info(f"Total rows before deduplication: {gdf.shape[0]}")
    gdf.drop_duplicates(subset=["id"], inplace=True)
    logger.info(f"Rows after deduplication in Python: {gdf.shape[0]}")
    transformed_json = gdf.to_json(orient='records')
    ti.xcom_push(key=f"transformed_{data_type}", value=transformed_json)
    logger.info(f"Transformation for {data_type} completed. Shape: {gdf.shape}")

# Function for loading to BQ
# Author: Kai Jun
def load_amenities_data_to_bq(data_type, **kwargs):
    """
    LOAD task: Pulls the transformed data from XCom and loads it into BigQuery.
    Uses the BigQuery client which automatically picks up credentials if
    GOOGLE_APPLICATION_CREDENTIALS is set.
    """
    logger.info(f"Starting to load {data_type} data into BigQuery...")
    ti = kwargs['ti']
    transformed_json = ti.xcom_pull(task_ids=f"transform_{data_type}_task", key=f"transformed_{data_type}")
    if transformed_json:
        df = pd.read_json(transformed_json, orient='records')
    else:
        df = pd.DataFrame()
    logger.info(f"Total rows in {data_type}: {df.shape[0]}")

    # One more validation before parsing
    if 'postalCode' in df.columns:
        df['postalCode'] = df['postalCode'].astype(str)
        
        # Fix postal codes with less than 6 digits by left-padding with zeros
        df['postalCode'] = df['postalCode'].apply(lambda x: x.zfill(6) if isinstance(x, str) and len(x) < 6 else x)
        
        # Fill district if missing or incorrect
        def infer_district(postal):
            if postal and isinstance(postal, str) and len(postal) == 6 and postal[:2].isdigit():
                return postal_to_district.get(postal[:2])
            return pd.NA

        if 'district' not in df.columns:
            df['district'] = pd.NA

        missing_district = df['district'].isna()
        df.loc[missing_district, 'district'] = df.loc[missing_district, 'postalCode'].apply(infer_district).astype(str)

    # Ensure correct data types
    str_columns = ['district']
    non_critical_columns = ['district']  # Columns we allow to have nulls

    for col in str_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            null_count = df[col].isna().sum()
            if col in non_critical_columns:
                logger.info(f"{col}: {null_count} rows will be retained with NULL values")
            else:
                logger.info(f"{col}: {null_count} rows will be dropped due to NaN")
                df = df.dropna(subset=[col])
            df[col] = df[col].astype(str)

    float_columns = ['latitude', 'longitude']
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
    
    logger.info(f"Rows after unit conversion: {df.shape[0]}")
    
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.amenities.{data_type}_table"
    load_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_dataframe(df, table_id, job_config=load_config)
    load_job.result()  # Wait for the load job to complete
    logger.info(f"Loaded {len(df)} rows into BigQuery table {table_id}.")

def transform_all_amenities(**kwargs):
    ti = kwargs['ti']
    data_types = ['activeSG', 'hawkerCentre', 'communityClub', 'supermarket', 'healthcare', 'malls']
    combined_data = []

    for dtype in data_types:
        transformed_json = ti.xcom_pull(task_ids=f"transform_{dtype}_task", key=f"transformed_{dtype}")
        if transformed_json:
            df = pd.read_json(transformed_json, orient='records')
            df['amenityType'] = dtype
            df = df[["id", "name", "address", "longitude", "latitude", "district", "postalCode", "amenityType"]]
            combined_data.append(df)

    df_combined = pd.concat(combined_data, ignore_index=True) if combined_data else pd.DataFrame()
    ti.xcom_push(key='transformed_amenities', value=df_combined.to_json(orient='records'))

def load_all_amenities_data_into_bq(**kwargs):
    ti = kwargs['ti']
    transformed_json = ti.xcom_pull(task_ids='transform_all_amenities_task', key='transformed_amenities')
    if transformed_json:
        df = pd.read_json(transformed_json, orient='records')
    else:
        df = pd.DataFrame()
    logger.info(f"Total rows in combined amenities: {df.shape[0]}")

    # One more validation before parsing
    if 'postalCode' in df.columns:
        df['postalCode'] = df['postalCode'].astype(str)
        
        # Fix postal codes with less than 6 digits by left-padding with zeros
        df['postalCode'] = df['postalCode'].apply(lambda x: x.zfill(6) if isinstance(x, str) and len(x) < 6 else x)
        
        # Fill district if missing or incorrect
        def infer_district(postal):
            if postal and isinstance(postal, str) and len(postal) == 6 and postal[:2].isdigit():
                return postal_to_district.get(postal[:2])
            return pd.NA

        if 'district' not in df.columns:
            df['district'] = pd.NA

        missing_district = df['district'].isna()
        df.loc[missing_district, 'district'] = df.loc[missing_district, 'postalCode'].apply(infer_district).astype(str)

    # Ensure correct data types
    str_columns = ['district']
    non_critical_columns = ['district']  # Columns we allow to have nulls

    for col in str_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            null_count = df[col].isna().sum()
            if col in non_critical_columns:
                logger.info(f"{col}: {null_count} rows will be retained with NULL values")
            else:
                logger.info(f"{col}: {null_count} rows will be dropped due to NaN")
                df = df.dropna(subset=[col])
            df[col] = df[col].astype(str)

    float_columns = ['latitude', 'longitude']
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
    
    logger.info(f"Rows after unit conversion: {df.shape[0]}")
    
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.amenities.combined_amenities"
    load_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_dataframe(df, table_id, job_config=load_config)
    load_job.result()  # Wait for the load job to complete
    logger.info(f"Loaded {len(df)} rows into BigQuery table {table_id}.")

with DAG(
    dag_id='amenities_data_ingestion_pipeline',
    default_args=default_args,
    schedule_interval="0 0 1 1,4,7,10 *",  # Runs every 3 months (each quarter)
    catchup=False
) as dag:
    extract_activeSG_task = PythonOperator(
        task_id='extract_activeSG_task',
        python_callable=extract_activeSG_data,
        provide_context=True
    )

    def transform_activeSG(**kwargs):
        email = Variable.get("onemap_email")
        password = Variable.get("onemap_password")
        auth_token = extract_onemap_token(email, password)
        transform_activeSG_data(auth_token, **kwargs)

    transform_activeSG_task = PythonOperator(
        task_id='transform_activeSG_task',
        python_callable=transform_activeSG,
        provide_context=True
    )

    load_activeSG_task = PythonOperator(
        task_id='load_activeSG_task',
        python_callable=load_activeSG_data_to_bq,
        provide_context=True
    )

    extract_hawker_task = PythonOperator(
        task_id="extract_hawkerCentre_task",
        python_callable=extract_amenities_data,
        op_kwargs={"dataset_id": "d_4a086da0a5553be1d89383cd90d07ecd", "data_type": "hawkerCentre"},
        provide_context=True
    )

    def transform_hawker(**kwargs):
        email = Variable.get("onemap_email")
        password = Variable.get("onemap_password")
        auth_token = extract_onemap_token(email, password)
        transform_amenities_data("hawkerCentre", auth_token, **kwargs)

    transform_hawker_task = PythonOperator(
        task_id="transform_hawkerCentre_task",
        python_callable=transform_hawker,
        provide_context=True
    )

    load_hawker_task = PythonOperator(
        task_id="load_hawkerCentre_task",
        python_callable=load_amenities_data_to_bq,
        op_kwargs={"data_type": "hawkerCentre"},
        provide_context=True
    )

    extract_community_task = PythonOperator(
        task_id="extract_communityClub_task",
        python_callable=extract_amenities_data,
        op_kwargs={"dataset_id": "d_f706de1427279e61fe41e89e24d440fa", "data_type": "communityClub"},
        provide_context=True
    )

    def transform_community(**kwargs):
        email = Variable.get("onemap_email")
        password = Variable.get("onemap_password")
        auth_token = extract_onemap_token(email, password)
        transform_amenities_data("communityClub", auth_token, **kwargs)

    transform_community_task = PythonOperator(
        task_id="transform_communityClub_task",
        python_callable=transform_community,
        provide_context=True
    )

    load_community_task = PythonOperator(
        task_id="load_communityClub_task",
        python_callable=load_amenities_data_to_bq,
        op_kwargs={"data_type": "communityClub"},
        provide_context=True
    )

    extract_supermarket_task = PythonOperator(
        task_id="extract_supermarket_task",
        python_callable=extract_amenities_data,
        op_kwargs={"dataset_id": "d_cac2c32f01960a3ad7202a99c27268a0", "data_type": "supermarket"},
        provide_context=True
    )

    def transform_supermarket(**kwargs):
        email = Variable.get("onemap_email")
        password = Variable.get("onemap_password")
        auth_token = extract_onemap_token(email, password)
        transform_amenities_data("supermarket", auth_token, **kwargs)

    transform_supermarket_task = PythonOperator(
        task_id="transform_supermarket_task",
        python_callable=transform_supermarket,
        provide_context=True
    )

    load_supermarket_task = PythonOperator(
        task_id="load_supermarket_task",
        python_callable=load_amenities_data_to_bq,
        op_kwargs={"data_type": "supermarket"},
        provide_context=True
    )

    extract_healthcare_task = PythonOperator(
        task_id="extract_healthcare_task",
        python_callable=extract_healthcare_data,
        op_kwargs={"url": "https://s3.dualstack.us-east-1.amazonaws.com/production-raw-data-api/ISO3/SGP/health_facilities/points/hotosm_sgp_health_facilities_points_geojson.zip"},
        provide_context=True
    )

    def transform_healthcare(**kwargs):
        email = Variable.get("onemap_email")
        password = Variable.get("onemap_password")
        auth_token = extract_onemap_token(email, password)
        transform_healthcare_data(auth_token, **kwargs)

    transform_healthcare_task = PythonOperator(
        task_id="transform_healthcare_task",
        python_callable=transform_healthcare,
        provide_context=True
    )

    load_healthcare_task = PythonOperator(
        task_id="load_healthcare_task",
        python_callable=load_healthcare_data_to_bq,
        provide_context=True
    )

    extract_malls_wikipedia_task = PythonOperator(
        task_id='extract_malls_wikipedia_task',
        python_callable=extract_malls_wikipedia,
        provide_context=True
    )

    extract_malls_google_task = PythonOperator(
        task_id='extract_malls_google_task',
        python_callable=extract_malls_google,
        provide_context=True
    )

    def transform_malls(**kwargs):
        email = Variable.get("onemap_email")
        password = Variable.get("onemap_password")
        auth_token = extract_onemap_token(email, password)
        transform_malls_data(auth_token, **kwargs)

    transform_malls_task = PythonOperator(
        task_id="transform_malls_task",
        python_callable=transform_malls,
        provide_context=True
    )

    load_malls_task = PythonOperator(
        task_id="load_malls_task",
        python_callable=load_malls_data_to_bq,
        provide_context=True
    )

    transform_all_amenities_task = PythonOperator(
        task_id='transform_all_amenities_task',
        python_callable=transform_all_amenities,
        provide_context=True
    )

    load_all_amenities_task = PythonOperator(
        task_id='load_all_amenities_task',
        python_callable=load_all_amenities_data_into_bq,
        provide_context=True
    )

    ## Set dependencies:
    extract_activeSG_task >> transform_activeSG_task >> load_activeSG_task
    extract_hawker_task >> transform_hawker_task >> load_hawker_task
    extract_community_task >> transform_community_task >> load_community_task
    extract_supermarket_task >> transform_supermarket_task >> load_supermarket_task
    extract_healthcare_task >> transform_healthcare_task >> load_healthcare_task
    [extract_malls_wikipedia_task, extract_malls_google_task] >> transform_malls_task >> load_malls_task
    [transform_activeSG_task, transform_hawker_task, transform_community_task,
    transform_supermarket_task, transform_healthcare_task, transform_malls_task] >> transform_all_amenities_task >> load_all_amenities_task