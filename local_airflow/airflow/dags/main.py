from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
import requests
import json
import pandas as pd
from datetime import datetime

# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'fetch_mrt_stations',
    default_args=default_args,
    description='Fetch MRT Stations Address in Singapore',
    schedule_interval='@daily',
)

def fetch_mrt_data():
    full_MRT_data = []

    #EWL Stations
    EWL_code_list = [f"EW{i}" for i in range(1, 34)]
    EWL_code_list.append("CG1")
    EWL_code_list.append("CG2")

    # North East Line Stations
    NEL_code_list = [f"NE{i}" for i in range(1, 19)]
    NEL_code_list.remove("NE2")

    # North-South Line Stations
    NSL_code_list = [f"NS{i}" for i in range(1, 29)]
    NSL_code_list.remove("NS6")

    # Circle Line Stations
    CCL_code_list = [f"CC{i}" for i in range(1, 30)]
    CCL_code_list.append("CE1")
    CCL_code_list.append("CE2")
    CCL_code_list.remove("CC18")

    # Thomson-EastCoast Line Stations
    TEL_code_list = [f"TE{i}" for i in range(1, 32)]
    TEL_code_list.remove("TE10")
    TEL_code_list.remove("TE21")

    # Downtown Line Stations
    DTL_code_list = [f"DT{i}" for i in range(1, 38)]

    full_mrt_code = EWL_code_list + NEL_code_list + NSL_code_list + CCL_code_list + TEL_code_list + DTL_code_list

    for codeName in full_mrt_code:
        url = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={codeName}&returnGeom=Y&getAddrDetails=Y&pageNum=1"
        response = requests.get(url)
        data = response.json()

        if "results" in data and isinstance(data["results"], list):
            for result in data["results"]:
                if "SEARCHVAL" in result and "MRT" in result["SEARCHVAL"]:
                    full_MRT_data.append(result)
                    break  # Stop after finding the first correct match

    # Print or store full MRT station data
    print(json.dumps(full_MRT_data, indent=4))
    df = pd.DataFrame(full_MRT_data)
    df = df.drop_duplicates()
    df.drop(columns=['SEARCHVAL', 'BLK_NO', 'X', 'Y'], inplace=True)

    # Save to CSV
    csv_filename = "/tmp/all_mrt_stations.csv"
    df.to_csv(csv_filename, index=False)

fetch_mrt_task = PythonOperator(
    task_id='fetch_mrt_data_task',
    python_callable=fetch_mrt_data,
    dag=dag,
)

load_mrt_to_bq = GCSToBigQueryOperator(
    task_id='load_mrt_to_bq',
    bucket='your-gcs-bucket',
    source_objects=['all_mrt_stations.csv'],
    destination_project_dataset_table='your_project.rental_data.mrt_stations',
    source_format='CSV',
    autodetect=True,
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

fetch_mrt_task >> load_mrt_to_bq