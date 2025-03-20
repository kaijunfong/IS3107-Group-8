from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
import requests
import json
from datetime import datetime

# API Keys (Consider storing them securely using Airflow Variables or Secrets)
LTA_ACCOUNT_KEY = "your_lta_account_key"
ONEMAP_API_KEY = "your_onemap_api_key"

# GCS Bucket
GCS_BUCKET_NAME = "your-gcs-bucket"
LTA_FILE_NAME = "lta_transport_data.json"
ONEMAP_FILE_NAME = "onemap_transport_data.json"

# BigQuery Details
BQ_PROJECT_ID = "your_project"
BQ_DATASET = "rental_data"
BQ_LTA_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET}.lta_transport"
BQ_ONEMAP_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET}.onemap_transport"

# Function to fetch data from LTA DataMall API
def fetch_lta_data():
    url = "https://datamall2.mytransport.sg/ltaodataservice/PV/Train"
    headers = {"AccountKey": LTA_ACCOUNT_KEY, "accept": "application/json"}
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(LTA_FILE_NAME)
        blob.upload_from_string(json.dumps(data), content_type="application/json")
        print(f"LTA data saved to GCS: {LTA_FILE_NAME}")
    else:
        raise Exception(f"Failed to fetch LTA data: {response.status_code}")

# Function to fetch data from OneMap API
def fetch_onemap_data():
    url = "https://developers.onemap.sg/publicapi/publictransport/trainstations"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(ONEMAP_FILE_NAME)
        blob.upload_from_string(json.dumps(data), content_type="application/json")
        print(f"OneMap data saved to GCS: {ONEMAP_FILE_NAME}")
    else:
        raise Exception(f"Failed to fetch OneMap data: {response.status_code}")

# Define Airflow DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 1),
    "retries": 1
}

dag = DAG(
    "transport_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

# Airflow Tasks
fetch_lta_task = PythonOperator(
    task_id="fetch_lta_data",
    python_callable=fetch_lta_data,
    dag=dag
)

fetch_onemap_task = PythonOperator(
    task_id="fetch_onemap_data",
    python_callable=fetch_onemap_data,
    dag=dag
)

load_lta_to_bq = GCSToBigQueryOperator(
    task_id="load_lta_to_bq",
    bucket=GCS_BUCKET_NAME,
    source_objects=[LTA_FILE_NAME],
    destination_project_dataset_table=BQ_LTA_TABLE,
    source_format="NEWLINE_DELIMITED_JSON",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

load_onemap_to_bq = GCSToBigQueryOperator(
    task_id="load_onemap_to_bq",
    bucket=GCS_BUCKET_NAME,
    source_objects=[ONEMAP_FILE_NAME],
    destination_project_dataset_table=BQ_ONEMAP_TABLE,
    source_format="NEWLINE_DELIMITED_JSON",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

# Define Task Dependencies
fetch_lta_task >> load_lta_to_bq
fetch_onemap_task >> load_onemap_to_bq
