from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum
import requests
import pandas as pd
import time
import json
from google.cloud import bigquery

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

# Functions for extractions
# Author: Kai Jun
def extract_hdb_data(**kwargs):
    logger.info(f"Starting extract_hdb_data()")
    dataset_id = "d_c9f57187485a850908655db0e8cfe651"
    base_url = f"https://data.gov.sg/api/action/datastore_search?resource_id={dataset_id}"
    offset = 0
    all_records = []

    # Loop until no records are returned or offset >= total
    logger.info("Starting HDB data extraction...")
    while True:
        url = f"{base_url}&offset={offset}"
        logger.info(f"Fetching data with offset: {offset}")
        response = requests.get(url)
        data = response.json()
        result = data.get("result", {})
        records = result.get("records", [])
        total = result.get("total", 0)
        
        logger.info(f"Fetched {len(records)} records; Total available: {total}")
        
        if not records:
            logger.info("No more records returned. Terminating extraction loop.")
            break  # Terminate when no more records are returned
        
        all_records.extend(records)
        offset += len(records) 
    
        if offset >= total:
            logger.info("Offset has reached or exceeded total available records. Terminating extraction loop.")
            break

    logger.info(f"Data extraction completed. Total records extracted: {len(all_records)}")

    processed = []
    for rec in all_records:
        flat_type = rec.get("flat_type", "").upper()
        # Skip executive flats as they are in URA dataset
        if "EXECUTIVE" in flat_type:
            continue

        # Extract number of rooms (e.g., from "4-ROOM")
        num_rooms = None
        if "-" in flat_type:
            try:
                num_rooms = int(flat_type.split("-")[0]) # int(4)
            except Exception as e:
                logger.debug(f"Error extracting rooms: {e}")
                num_rooms = None

        # Extract Year (eg 2025-02 YYYY-MM)
        rent_approval = rec.get("rent_approval_date", "").upper()
        rent_year = None
        if "-" in rent_approval:
            try:
                rent_year = int(rent_approval.split("-")[0]) # int(2025)
            except Exception as e:
                logger.debug(f"Error extracting rooms: {e}")
                rent_year = None

        # Form Name from block and street_name
        name = rec.get("block", "").strip() + " " + rec.get("street_name", "").strip()
        id = name + " " + rec.get("rent_approval_date", "").strip() + " Rooms " + str(num_rooms)
        rent = int(rec.get("monthly_rent", None))
        processed.append({
            "id": id,
            "address": name, # duplicate to have consistency with other datasets -> used for mapping
            "name": name,
            "rent": rent,
            "propertyType": "Public", # hardcoded as all from HDB
            "noOfBedRoom": num_rooms,
            "rentYear": rent_year
        })

    df = pd.DataFrame(processed)
    return df

# Functions for transformations
# Author: Sok Yang, edited by Kai Jun
def transform_property_data(df, auth_token, source, **kwargs):
    logger.info(f"Starting transform_property_data() for {source}")
    df_info = df[['address']].drop_duplicates()
    logger.info(f"Running API calls on unique names: {df_info.shape}")
    trasnformed_columns = process_batches(df_info, auth_token)
    df = df.merge(trasnformed_columns, on="address", how="outer")
    logger.info(f"New Data shape: {df.shape}")
    # df.to_csv(f"full_output_{source}.csv")
    return df

# Author: Sok Yang, edited by Kai Jun
def transform_and_load_URA_data(combined_file_path, auth_token, **kwargs):
    logger.info(f"Starting transform_URA_data()")
    # combined json file contains a list of the outputs from the API calls to URA endpoint
    with open(combined_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = []
    for response_obj in data:
        # For each chunk in the combined file:
        status = response_obj.get("Status")
        results = response_obj.get("Result", [])

        if status == "Success" and results:
            # Convert 'Result' into a DataFrame
            df_chunk = pd.DataFrame(results)
            df.append(df_chunk)
        else:
            logger.debug(f"Skipping response with status: {status}")

    if df: # there are results in the list
        df = pd.concat(df, ignore_index=True) # combine all the list elements together
        # strcuture is that each project/ street has an array of rental contracts for this property
        df_exploded = df.explode("rental", ignore_index=True)
        # drop rows where 'rental' is null -> not useful and only minority
        df_exploded = df_exploded[df_exploded["rental"].notnull()].reset_index(drop=True)

        # flatten nested dict in the 'rental' column { "leaseDate": , "propertyType": , "areaSqm": , "areaSqft": , "rent": , "district": , "noOfBedRoom": }
        rental_flat = pd.json_normalize(df_exploded["rental"])

        # concat flattened rental columns with the rest of df
        df_intermediate = pd.concat(
            [df_exploded.drop(columns=["rental"]), rental_flat],
            axis=1
        ) # axis = 1 to add columns 
        logger.info(f"Pre Transformed shape: {df_intermediate.shape}")
        logger.info(df_intermediate['propertyType'].unique())
        df_intermediate["id"] = df_intermediate.apply(lambda row: f"{row['project']} {row['noOfBedRoom']} area {row['areaSqft']} {row['areaSqm']} {row['leaseDate']} rent {row['rent']}", axis=1) # trial and error: excluded geographical data such as distirct and x & y as it is unlikely to be different -> those are likely duplicates
        df_intermediate = df_intermediate.rename(columns={"project": "name"})
        df_intermediate["address"] = df_intermediate["street"]
        df_intermediate["rent"] = df_intermediate["rent"].apply(lambda x: int(x))
        df_intermediate["rentYear"] = df_intermediate['leaseDate'].apply(lambda x: int(x) % 100 + 2000) # mmyy
        df_intermediate_unique = pd.DataFrame(df_intermediate["address"].drop_duplicates(), columns=["address"])
        logger.info(f"Running {len(df_intermediate_unique)} API calls on unique names")
        trasnformed_columns = process_batches(df_intermediate_unique, auth_token)

        df_final = df_intermediate.merge(trasnformed_columns, on="address", how="outer")
        logger.info(f"Shape after transform: {df_final.shape}")

        # debug
        logger.info(f"Null counts:\n{df_final[['district_x', 'district_y']].isnull().sum()}")

        # df_final = df_final[df_final['district_x'] == df_final['district_y']]
        df_final = df_final.rename(columns={"district_x": "district"}) # use district x which is from the code and drop URA one (district y) as URA has much more NULL values i.e. district_y NULL values 427451 
        # df_visualisation = df_final.copy()
        df_final['meanAreaSqm'] = df_final['areaSqm'].apply(
            lambda x: (int(x.split("-")[0]) + int(x.split("-")[1])) / 2 if isinstance(x, str) and "-" in x else None
        )
        df_final['meanAreaSqft'] = df_final['areaSqft'].apply(
            lambda x: (int(x.split("-")[0]) + int(x.split("-")[1])) / 2 if isinstance(x, str) and "-" in x else None
        )
        df_final = df_final.drop(columns=['x', 'y', 'street', 'leaseDate', 'district_y'])
        # df_final = df_final.drop(columns=['x', 'y', 'street', 'areaSqm', 'leaseDate', 'areaSqft', 'district_y'])

        logger.info(f"Loading df_final to URA_table")
        logger.info(f"Total rows before deduplication: {df_final.shape[0]}")
        df_final.drop_duplicates(subset=["id"], inplace=True)
        logger.info(f"Rows in URA_table after deduplication in Python: {df_final.shape[0]}")

        # One more validation before parsing
        if 'postalCode' in df_final.columns:
            df_final['postalCode'] = df_final['postalCode'].astype(str)
            
            # Fix postal codes with less than 6 digits by left-padding with zeros
            df_final['postalCode'] = df_final['postalCode'].apply(lambda x: x.zfill(6) if isinstance(x, str) and len(x) < 6 else x)
            
            # Fill district if missing or incorrect
            def infer_district(postal):
                if postal and isinstance(postal, str) and len(postal) == 6 and postal[:2].isdigit():
                    return postal_to_district.get(postal[:2])
                return pd.NA

            if 'district' not in df_final.columns:
                df_final['district'] = pd.NA

            missing_district = df_final['district'].isna()
            df_final.loc[missing_district, 'district'] = df_final.loc[missing_district, 'postalCode'].apply(infer_district).astype(str)

        # Ensure correct data types
        int_columns = ['noOfBedRoom', 'rentYear']
        non_critical_columns = ['noOfBedRoom', 'rentYear']  # Columns we allow to have nulls

        for col in int_columns:
            if col in df_final.columns:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce')
                null_count = df_final[col].isna().sum()
                if col in non_critical_columns:
                    logger.info(f"{col}: {null_count} rows will be retained with NULL values")
                else:
                    logger.info(f"{col}: {null_count} rows will be dropped due to NaN")
                    df_final = df_final.dropna(subset=[col])
                df_final[col] = df_final[col].astype('Int64')  # Nullable integer dtype
        
        float_columns = ['latitude', 'longitude', 'rent', 'areaSqm', 'areaSqft']
        for col in float_columns:
            if col in df_final.columns:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce').astype('float64')

        string_columns = ['id', 'address', 'name', 'propertyType', 'district']
        for col in string_columns:
            if col in df_final.columns:
                df_final[col] = df_final[col].astype(str)
        
        logger.info(f"Rows in df_visualisation after unit conversion: {df_final.shape[0]}")
        
        client = bigquery.Client(project=PROJECT_ID)
        
        staging_table_id = f"{PROJECT_ID}.rental.URA_staging_table"
        prod_table_id = f"{PROJECT_ID}.rental.URA_table"
        
        # Step 1: Load combined_df into the staging table (replace previous data).
        load_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = client.load_table_from_dataframe(df_final, staging_table_id, job_config=load_config)
        load_job.result()  # Wait for the job to complete
        logger.info(f"Loaded staging table: {staging_table_id} with {df_final.shape[0]} rows.")
        
        # Step 2: Append the staging data to the final table.
        # Note: WRITE_APPEND adds to any existing rows.
        append_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        append_job = client.load_table_from_dataframe(df_final, prod_table_id, job_config=append_config)
        append_job.result()  # Wait for the job to complete
        logger.info(f"Appended staging data to final table: {prod_table_id}.")
        
        # Step 3: Merge new data from the staging table into the final table using MERGE
        merge_query = f"""
        MERGE `{prod_table_id}` T
        USING `{staging_table_id}` S
        ON T.id = S.id
        WHEN NOT MATCHED THEN
        INSERT (id, address, name, rent, propertyType, noOfBedRoom, rentYear, postalCode, areaSqm, areaSqft, meanAreaSqm, meanAreaSqft, latitude, longitude, district)
        VALUES(S.id, S.address, S.name, S.rent, S.propertyType, S.noOfBedRoom, S.rentYear, S.postalCode, S.areaSqm, S.areaSqft, S.meanAreaSqm, S.meanAreaSqft, S.latitude, S.longitude, S.district)
        """
        merge_job = client.query(merge_query)
        merge_job.result()
        logger.info(f"MERGE completed into {prod_table_id}: {df_final.shape[0]} rows processed.")
        # logger.info(f"Final df (to load) shape: {df_final.shape}")
        # return {'forML': df_final, 'forDashboard': df_visualisation}
    else:
        logger.info("No valid records found.")

# # Function for loading to BQ
# # Author: Kai Jun
# def load_URA_rental_data_to_bq(**kwargs):
#     """
#     Loads combined transformed data into BigQuery by first loading all data into a staging table,
#     then appending it to the final table, and finally deduplicating the final table based on the 'id' column.
#     """
#     ti = kwargs['ti']
#     ura_json = ti.xcom_pull(task_ids='transform_ura_task', key='transformed_ura_rental_data_for_dashboard')
    
#     # Convert JSON strings to DataFrames (if a dataset is missing, create an empty DF)
#     df_ura = pd.read_json(ura_json, orient="records") if ura_json else pd.DataFrame()
    
#     logger.info(f"Total rows before deduplication: {df_ura.shape[0]}")
#     df_ura.drop_duplicates(subset=["id"], inplace=True)
#     logger.info(f"Rows after deduplication in Python: {df_ura.shape[0]}")

#     # One more validation before parsing
#     if 'postalCode' in df_ura.columns:
#         df_ura['postalCode'] = df_ura['postalCode'].astype(str)
        
#         # Fix postal codes with less than 6 digits by left-padding with zeros
#         df_ura['postalCode'] = df_ura['postalCode'].apply(lambda x: x.zfill(6) if isinstance(x, str) and len(x) < 6 else x)
        
#         # Fill district if missing or incorrect
#         def infer_district(postal):
#             if postal and isinstance(postal, str) and len(postal) == 6 and postal[:2].isdigit():
#                 return postal_to_district.get(postal[:2])
#             return pd.NA

#         if 'district' not in df_ura.columns:
#             df_ura['district'] = pd.NA

#         missing_district = df_ura['district'].isna()
#         df_ura.loc[missing_district, 'district'] = df_ura.loc[missing_district, 'postalCode'].apply(infer_district).astype(str)

#     # Ensure correct data types
#     int_columns = ['noOfBedRoom', 'rentYear']
#     non_critical_columns = ['noOfBedRoom', 'rentYear']  # Columns we allow to have nulls

#     for col in int_columns:
#         if col in df_ura.columns:
#             df_ura[col] = pd.to_numeric(df_ura[col], errors='coerce')
#             null_count = df_ura[col].isna().sum()
#             if col in non_critical_columns:
#                 logger.info(f"{col}: {null_count} rows will be retained with NULL values")
#             else:
#                 logger.info(f"{col}: {null_count} rows will be dropped due to NaN")
#                 df_ura = df_ura.dropna(subset=[col])
#             df_ura[col] = df_ura[col].astype('Int64')  # Nullable integer dtype
    
#     float_columns = ['latitude', 'longitude', 'rent', 'areaSqm', 'areaSqft']
#     for col in float_columns:
#         if col in df_ura.columns:
#             df_ura[col] = pd.to_numeric(df_ura[col], errors='coerce').astype('float64')
    
#     logger.info(f"Rows after unit conversion: {df_ura.shape[0]}")
    
#     client = bigquery.Client(project=PROJECT_ID)
    
#     staging_table_id = f"{PROJECT_ID}.rental.URA_staging_table"
#     prod_table_id = f"{PROJECT_ID}.rental.URA_table"
    
#     # Step 1: Load combined_df into the staging table (replace previous data).
#     load_config = bigquery.LoadJobConfig(
#         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#     )
#     load_job = client.load_table_from_dataframe(df_ura, staging_table_id, job_config=load_config)
#     load_job.result()  # Wait for the job to complete
#     logger.info(f"Loaded staging table: {staging_table_id} with {df_ura.shape[0]} rows.")
    
#     # Step 2: Append the staging data to the final table.
#     # Note: WRITE_APPEND adds to any existing rows.
#     append_config = bigquery.LoadJobConfig(
#         write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
#     )
#     append_job = client.load_table_from_dataframe(df_ura, prod_table_id, job_config=append_config)
#     append_job.result()  # Wait for the job to complete
#     logger.info(f"Appended staging data to final table: {prod_table_id}.")
    
#     # Step 3: Deduplicate the final table.
#     # This query uses ROW_NUMBER() to keep only one row per id.
#     # Adjust the ORDER BY clause if you want to prefer a particular row in case of duplicates.
#     dedup_query = f"""
#     CREATE OR REPLACE TABLE `{prod_table_id}` AS
#     SELECT * EXCEPT(rn)
#     FROM (
#       SELECT *,
#              ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) as rn
#       FROM `{prod_table_id}`
#     )
#     WHERE rn = 1
#     """
#     dedup_job = client.query(dedup_query)
#     dedup_job.result()  # Wait for the deduplication job to complete
#     logger.info(f"Deduplication complete on final table: {prod_table_id}.")

# Function for loading to BQ
# Author: Kai Jun
def load_HDB_rental_data_to_bq(**kwargs):
    """
    Loads combined transformed data into BigQuery by first loading all data into a staging table,
    then appending it to the final table, and finally deduplicating the final table based on the 'id' column.
    """
    ti = kwargs['ti']
    hdb_json = ti.xcom_pull(task_ids='transform_hdb_task', key='transformed_hdb_rental_data')
    
    # Convert JSON strings to DataFrames (if a dataset is missing, create an empty DF)
    df = pd.read_json(hdb_json, orient="records") if hdb_json else pd.DataFrame()
    
    logger.info(f"Total rows before deduplication: {df.shape[0]}")
    df.drop_duplicates(subset=["id"], inplace=True)
    logger.info(f"Rows after deduplication in Python: {df.shape[0]}")

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
    int_columns = ['noOfBedRoom', 'rentYear']
    non_critical_columns = ['noOfBedRoom', 'rentYear']  # Columns we allow to have nulls

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
    
    float_columns = ['latitude', 'longitude', 'rent']
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')

    string_columns = ['id', 'address', 'name', 'propertyType', 'district']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    logger.info(f"Rows after unit conversion: {df.shape[0]}")
    
    client = bigquery.Client(project=PROJECT_ID)
    
    staging_table_id = f"{PROJECT_ID}.rental.HDB_staging_table"
    prod_table_id = f"{PROJECT_ID}.rental.HDB_table"
    
    # Step 1: Load combined_df into the staging table (replace previous data).
    load_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_dataframe(df, staging_table_id, job_config=load_config)
    load_job.result()  # Wait for the job to complete
    logger.info(f"Loaded staging table: {staging_table_id} with {df.shape[0]} rows.")
    
    # Step 2: Append the staging data to the final table.
    # Note: WRITE_APPEND adds to any existing rows.
    append_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    append_job = client.load_table_from_dataframe(df, prod_table_id, job_config=append_config)
    append_job.result()  # Wait for the job to complete
    logger.info(f"Appended staging data to final table: {prod_table_id}.")
    
    # Step 3: Deduplicate the final table.
    # This query uses ROW_NUMBER() to keep only one row per id.
    # Adjust the ORDER BY clause if you want to prefer a particular row in case of duplicates.
    dedup_query = f"""
    CREATE OR REPLACE TABLE `{prod_table_id}` AS
    SELECT * EXCEPT(rn)
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) as rn
      FROM `{prod_table_id}`
    )
    WHERE rn = 1
    """
    dedup_job = client.query(dedup_query)
    dedup_job.result()  # Wait for the deduplication job to complete
    logger.info(f"Deduplication complete on final table: {prod_table_id}.")

def load_all_rental_data_to_bq(**kwargs):
    """
    Loads combined transformed data into BigQuery by first loading all data into a staging table,
    then appending it to the final table, and finally deduplicating the final table based on the 'id' column.
    """
    HDB_TABLE = f"{PROJECT_ID}.rental.HDB_table"
    URA_TABLE = f"{PROJECT_ID}.rental.URA_table"
    PROD_TABLE = f"{PROJECT_ID}.rental.HDB_URA_table"

    client = bigquery.Client(project=PROJECT_ID)

    merge_query = f"""
    MERGE `{PROD_TABLE}` T
    USING (
        SELECT DISTINCT id, address, name, rent, propertyType, noOfBedRoom, rentYear, postalCode, latitude, longitude, district FROM `{HDB_TABLE}`
        UNION DISTINCT
        SELECT DISTINCT id, address, name, rent, propertyType, noOfBedRoom, rentYear, postalCode, latitude, longitude, district FROM `{URA_TABLE}`
    ) S
    ON T.id = S.id
    WHEN NOT MATCHED THEN
        INSERT (id, address, name, rent, propertyType, noOfBedRoom, rentYear, postalCode, latitude, longitude, district)
        VALUES (S.id, S.address, S.name, S.rent, S.propertyType, S.noOfBedRoom, S.rentYear, S.postalCode, S.latitude, S.longitude, S.district)
    """

    merge_job = client.query(merge_query)
    merge_job.result()

    logger.info("MERGE completed successfully.")

    # ti = kwargs['ti']
    
    # # Pull each dataset's JSON string from XCom (adjust task_ids/keys as needed)
    # hdb_json = ti.xcom_pull(task_ids='transform_hdb_task', key='transformed_hdb_rental_data')
    # ura_json = ti.xcom_pull(task_ids='transform_ura_task', key='transformed_ura_rental_data')
    
    # # Convert JSON strings to DataFrames (if a dataset is missing, create an empty DF)
    # df_hdb = pd.read_json(hdb_json, orient="records") if hdb_json else pd.DataFrame()
    # df_ura = pd.read_json(ura_json, orient="records") if ura_json else pd.DataFrame()
    
    # # Combine the DataFrames and remove duplicates on the primary key "id"
    # combined_df = pd.concat([df_hdb, df_ura], ignore_index=True)
    # logger.info(f"Total rows before deduplication: {combined_df.shape[0]}")
    # combined_df.drop_duplicates(subset=["id"], inplace=True)
    # logger.info(f"Rows after deduplication in Python: {combined_df.shape[0]}")

    # # One more validation before parsing
    # if 'postalCode' in combined_df.columns:
    #     combined_df['postalCode'] = combined_df['postalCode'].astype(str)
        
    #     # Fix postal codes with less than 6 digits by left-padding with zeros
    #     combined_df['postalCode'] = combined_df['postalCode'].apply(lambda x: x.zfill(6) if isinstance(x, str) and len(x) < 6 else x)
        
    #     # Fill district if missing or incorrect
    #     def infer_district(postal):
    #         if postal and isinstance(postal, str) and len(postal) == 6 and postal[:2].isdigit():
    #             return postal_to_district.get(postal[:2])
    #         return pd.NA

    #     if 'district' not in combined_df.columns:
    #         combined_df['district'] = pd.NA

    #     missing_district = combined_df['district'].isna()
    #     combined_df.loc[missing_district, 'district'] = combined_df.loc[missing_district, 'postalCode'].apply(infer_district).astype(str)

    # # Ensure correct data types
    # int_columns = ['noOfBedRoom', 'rentYear']
    # non_critical_columns = ['noOfBedRoom', 'rentYear']  # Columns we allow to have nulls

    # for col in int_columns:
    #     if col in combined_df.columns:
    #         combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
    #         null_count = combined_df[col].isna().sum()
    #         if col in non_critical_columns:
    #             logger.info(f"{col}: {null_count} rows will be retained with NULL values")
    #         else:
    #             logger.info(f"{col}: {null_count} rows will be dropped due to NaN")
    #             combined_df = combined_df.dropna(subset=[col])
    #         combined_df[col] = combined_df[col].astype('Int64')  # Nullable integer dtype
    
    # float_columns = ['latitude', 'longitude', 'rent']
    # for col in float_columns:
    #     if col in combined_df.columns:
    #         combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').astype('float64')
    
    # string_columns = ['id', 'address', 'name', 'propertyType', 'district']
    # for col in string_columns:
    #     if col in combined_df.columns:
    #         combined_df[col] = combined_df[col].astype(str)
    
    # logger.info(f"Rows after unit conversion: {combined_df.shape[0]}")
    
    # client = bigquery.Client(project=PROJECT_ID)
    
    # staging_table_id = f"{PROJECT_ID}.rental.staging_table"
    # prod_table_id = f"{PROJECT_ID}.rental.HDB_URA_table"
    
    # # Step 1: Load combined_df into the staging table (replace previous data).
    # load_config = bigquery.LoadJobConfig(
    #     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    # )
    # load_job = client.load_table_from_dataframe(combined_df, staging_table_id, job_config=load_config)
    # load_job.result()  # Wait for the job to complete
    # logger.info(f"Loaded staging table: {staging_table_id} with {combined_df.shape[0]} rows.")
    
    # # Step 2: Merge new data from the staging table into the final table using MERGE
    # merge_query = f"""
    # MERGE `{prod_table_id}` T
    # USING `{staging_table_id}` S
    # ON T.id = S.id
    # WHEN MATCHED THEN
    #   UPDATE SET T.id = T.id
    # WHEN NOT MATCHED THEN
    #   INSERT (id, address, name, rent, propertyType, noOfBedRoom, rentYear, postalCode, latitude, longitude, district)
    #   VALUES(S.id, S.address, S.name, S.rent, S.propertyType, S.noOfBedRoom, S.rentYear, S.postalCode, S.latitude, S.longitude, S.district)
    # """
    # merge_job = client.query(merge_query)
    # merge_job.result()
    # logger.info(f"MERGE completed into {prod_table_id}: {combined_df.shape[0]} rows processed.")

with DAG(
    dag_id='rental_data_ingestion_pipeline',
    default_args=default_args,
    schedule_interval="0 2 * * *",  # Runs daily at 2 AM Singapore time
    catchup=False
) as dag:
    def hdb_extract_task(**kwargs):
        df = extract_hdb_data()
        csv_path = "/opt/airflow/data/hdb_extracted.csv"
        df.to_csv(csv_path, index=False) 
        logger.info(f"HDB extraction completed: {len(df)} rows saved to {csv_path}")

    extract_hdb_task = PythonOperator(
        task_id="extract_hdb_task",
        python_callable=hdb_extract_task,
        provide_context=True
    )

    def hdb_transform_task(**kwargs):
        ti = kwargs['ti']
        email = Variable.get("onemap_email")
        password = Variable.get("onemap_password")
        auth_token = extract_onemap_token(email, password)

        csv_path = "/opt/airflow/data/hdb_extracted.csv"
        df = pd.read_csv(csv_path)
        
        final_hdb_df = transform_property_data(df, auth_token, 'HDB')
        logger.info(f"HDB transformation completed, final shape: {final_hdb_df.shape}")
        ti.xcom_push(key='transformed_hdb_rental_data', value=final_hdb_df.to_json())

    transform_hdb_task = PythonOperator(
        task_id="transform_hdb_task",
        python_callable=hdb_transform_task,
        provide_context=True
    )

    # Run the URA extraction shell script first
    access_key = Variable.get("ura_access_key_password")

    extract_ura_task = BashOperator(
        task_id="extract_ura_task",
        bash_command="bash /opt/airflow/scripts/extract_URA_data.txt",
        env={"ACCESS_KEY": access_key} # to prevent jinja2.exceptions.TemplateNotFound
    )

    def ura_transform_task(**kwargs):
        ti = kwargs['ti']
        email = Variable.get("onemap_email")
        password = Variable.get("onemap_password")
        auth_token = extract_onemap_token(email, password)
        transform_and_load_URA_data('/opt/airflow/data/URA_rental_combined.json', auth_token)
        # ML_ura_df = transform_and_load_URA_data('/opt/airflow/data/URA_rental_combined.json', auth_token)["forML"]
        # dashbaord_ura_df = transform_URA_data('/opt/airflow/data/URA_rental_combined.json', auth_token)["forDashboard"]
        # logger.info(f"URA transformation completed, final shape: {ML_ura_df.shape}")
        # ti.xcom_push(key='transformed_ura_rental_data', value=ML_ura_df.to_json())
        # ti.xcom_push(key='transformed_ura_rental_data_for_dashboard', value=dashbaord_ura_df.to_json())

    transform_and_load_URA_data_task = PythonOperator(
        task_id="transform_and_load_URA_data_task",
        python_callable=ura_transform_task,
        provide_context=True
    )

    load_hdb_task = PythonOperator(
        task_id="load_hdb_task",
        python_callable=load_HDB_rental_data_to_bq,
        provide_context=True,
    )

    load__all_rental_data_task = PythonOperator(
        task_id="load__all_rental_data_task",
        python_callable=load_all_rental_data_to_bq,
        provide_context=True,
    )

    # Set dependencies:
    # First, run the URA extraction shell script, then transform URA data.
    extract_ura_task >> transform_and_load_URA_data_task
    extract_hdb_task >> transform_hdb_task >> load_hdb_task
    [transform_and_load_URA_data_task, load_hdb_task] >> load__all_rental_data_task