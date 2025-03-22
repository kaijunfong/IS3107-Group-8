from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from datetime import datetime
import os
from io import StringIO

PROJECT_ID = "is3107-group-8"
from dotenv import load_dotenv
load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

headers = {
    "AccountKey": os.getenv("DATAMALL_API_KEY"),
    "accept": "application/json"
}

# Bus
def extract_bus_routes(**kwargs):
    import pandas as pd
    import requests

    ti = kwargs['ti']
    headers = {
        "AccountKey": os.getenv("DATAMALL_API_KEY"),
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
        "AccountKey": os.getenv("DATAMALL_API_KEY"),
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

    ti.xcom_push(key='transformed_mrt_lrt_data', value=mrt_lrt_data.to_json())


def load_bus_data_to_bq(**kwargs):
    """Loads transformed bus data into BigQuery"""
    ti = kwargs['ti']
    transformed_data_json = ti.xcom_pull(task_ids='transform_bus_data_task', key='transformed_bus_data')

    df = pd.read_json(transformed_data_json)
    df.to_gbq(
        destination_table=f"{PROJECT_ID}.transport.bus_stop_summary",
        project_id=PROJECT_ID,
        if_exists="replace",
    )

def load_mrt_lrt_data_to_bq(**kwargs):
    """Loads transformed MRT and LRT data into BigQuery"""
    ti = kwargs['ti']
    transformed_data_json = ti.xcom_pull(task_ids='transform_mrt_lrt_data_task', key='transformed_mrt_lrt_data')

    df = pd.read_json(transformed_data_json)
    df.to_gbq(
        destination_table=f"{PROJECT_ID}.transport.mrt_lrt_summary",
        project_id=PROJECT_ID,
        if_exists="replace",
    )

with DAG(
    dag_id="transport_dag",
    schedule_interval="@daily",
    default_args=default_args, 
    catchup=False, 
    tags=["transport"],
) as dag:

    extract_bus_routes_task = PythonOperator(
        task_id="extract_bus_routes_task",
        python_callable=extract_bus_routes,
        provide_context=True,
    )

    extract_bus_stops_task = PythonOperator(
        task_id="extract_bus_stops_task",
        python_callable=extract_bus_stops,
        provide_context=True,
    )

    extract_mrt_lrt_data_task = PythonOperator(
        task_id="extract_mrt_lrt_data_task",
        python_callable=extract_mrt_lrt_data,
        provide_context=True,
    )

    transform_bus_data_task = PythonOperator(
        task_id="transform_bus_data_task",
        python_callable=transform_bus_data,
        provide_context=True,
    )

    transform_mrt_lrt_data_task = PythonOperator(
        task_id="transform_mrt_lrt_data_task",
        python_callable=transform_mrt_lrt_data,
        provide_context=True,
    )

    load_bus_data_task = PythonOperator(
        task_id="load_bus_data_task",
        python_callable=load_bus_data_to_bq,
        provide_context=True,
    )

    load_mrt_lrt_data_task = PythonOperator(
        task_id="load_mrt_lrt_data_task",
        python_callable=load_mrt_lrt_data_to_bq,
        provide_context=True,
    )

    [extract_bus_routes_task, extract_bus_stops_task] >> transform_bus_data_task >> load_bus_data_task
    extract_mrt_lrt_data_task >> transform_mrt_lrt_data_task >> load_mrt_lrt_data_task
