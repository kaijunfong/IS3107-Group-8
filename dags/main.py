from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import os
import time
from dotenv import load_dotenv
load_dotenv()
import json

PROJECT_ID = "is3107-group-8"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

headers = {
    "AccountKey": os.getenv("DATAMALL_API_KEY"),
    "accept": "application/json"
}

# Function to get latitude and longitude using OneMap API for postal code
def get_lat_lon_onemap(postal_code):

    ONEMAP_BASE_URL = "https://www.onemap.gov.sg/api/common/elastic/search"
    # Parameters for the API call (including your API Key)
    params = {
        'searchVal': postal_code,
        'returnGeom': 'Y',
        'getAddrDetails': 'Y'
    }
    response = requests.get(ONEMAP_BASE_URL, params=params)
    
    # Parse the response JSON
    data = response.json()
    
    # Check if the response contains results
    if data['found'] > 0:
        # Extract latitude and longitude from the response
        latitude = data['results'][0]['LATITUDE']
        longitude = data['results'][0]['LONGITUDE']
        return latitude, longitude
    else:
        # Return None if no results are found
        return None, None


def get_ter_school_details_google(school_name):
    TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
    query = school_name + " Singapore"
    params = {"query": query, "key": GOOGLE_API_KEY}
    response = requests.get(TEXT_SEARCH_URL, params=params)
    data = response.json()
    results_list = []
    if data["status"] == "OK":
        for result in data["results"]:
            results_list.append({
                "school_name": school_name,
                "google_school_name": result["name"],
                "address": result["formatted_address"],
                "longitude": result["geometry"]["location"]["lng"],
                "latitude": result["geometry"]["location"]["lat"]
            })
    if not results_list:
        results_list.append({
            "school_name": school_name,
            "google_school_name": None,
            "address": None,
            "longitude": None,
            "latitude": None
        })
    return results_list


def extract_schools(**kwargs):
    preschools = pd.read_csv('raw_dataset/preschool_loc_details_raw.csv', na_values=['na', 'NA', 'N/A', 'NULL'])
    print("PRESCHOOLS DATA FROM RAW:", preschools[:3])
    moe_schools = pd.read_csv('raw_dataset/moe_schools_raw.csv', na_values=['na', 'NA', 'N/A', 'NULL'])

    UNI_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_universities_in_Singapore"
    POLY_WIKI_URL = "https://en.wikipedia.org/wiki/Education_in_Singapore"
    ITE_WIKI_URL = "https://en.wikipedia.org/wiki/Institute_of_Technical_Education#Colleges"

    def get_university_names():
        response = requests.get(UNI_WIKI_URL)
        soup = BeautifulSoup(response.text, "html.parser")
        section_div = soup.find("div", id="Universities_in_Singapore145")
        university_list = []

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
                        university_list.append({"school_name": university_name, "category": "University"})
        return university_list

    def get_polytechnics():
        response = requests.get(POLY_WIKI_URL)
        soup = BeautifulSoup(response.text, "html.parser")
        polytechnic_section = soup.find("h2", {"id": "Polytechnics"})
        polytechnic_list = []

        if polytechnic_section:
            para = polytechnic_section.find_next("p")
            for link in para.find_all("a"):
                polytechnic_name = link.get_text(strip=True)
                polytechnic_list.append({"school_name": polytechnic_name, "category": "Polytechnic"})
        return polytechnic_list

    def get_ite_names():
        response = requests.get(ITE_WIKI_URL)
        soup = BeautifulSoup(response.text, "html.parser")
        ite_colleges_header = soup.find("h2", {"id": "Colleges"})
        ite_colleges_list = []

        if ite_colleges_header:
            ul = ite_colleges_header.find_next("ul")
            if ul:
                for link in ul.find_all("a"):
                    college_name = link.get_text(strip=True)
                    ite_colleges_list.append({"school_name": college_name, "category": "ITE College"})
        return ite_colleges_list

    tertiary = get_university_names() + get_polytechnics() + get_ite_names()

    preschools.to_json('preschools.json', orient='records', lines=True)
    moe_schools.to_json('moe_schools.json', orient='records', lines=True)
    with open('tertiary.json', 'w') as f:
        json.dump(tertiary, f)

def transform_schools(**kwargs):
    ti = kwargs['ti']

    # Load from temp files instead of XCom
    preschools = pd.read_json('preschools.json', lines=True)
    moe_schools = pd.read_json('moe_schools.json', lines=True)
    with open('tertiary.json', 'r') as f:
        tertiary = pd.DataFrame(json.load(f))

    print("PRESCHOOLS DATA FROM FILE:", preschools.head())

    preschools = preschools[preschools['centre_name'].notnull()].drop_duplicates(subset=['centre_name', 'postal_code'])
    preschools = preschools[['centre_name', 'centre_address', 'postal_code']].copy()
    preschools[['latitude', 'longitude']] = preschools['postal_code'].apply(get_lat_lon_onemap).apply(pd.Series)
    preschools['category'] = 'Preschool'
    preschools.rename(columns={'centre_name': 'school_name', 'centre_address': 'address'}, inplace=True)
    preschools.drop(columns=['postal_code'], inplace=True)

    moe_schools = moe_schools.drop_duplicates(subset=['school_name', 'postal_code'])
    moe_schools = moe_schools[['school_name', 'address', 'postal_code', 'mainlevel_code']].copy()
    moe_schools[['latitude', 'longitude']] = moe_schools['postal_code'].apply(get_lat_lon_onemap).apply(pd.Series)
    moe_schools.rename(columns={'mainlevel_code': 'category'}, inplace=True)
    moe_schools.drop(columns=['postal_code'], inplace=True)

    # Add coordinates to tertiary schools
    loc_details = []
    for _, row in tertiary.iterrows():
        loc_details.extend(get_ter_school_details_google(row['school_name']))
    df_loc = pd.DataFrame(loc_details)
    tertiary = tertiary.merge(df_loc, on='school_name', how='left')
    tertiary = tertiary[tertiary['google_school_name'].notnull()].drop(columns=['google_school_name'])
    tertiary = tertiary[tertiary['school_name'] != '[71]']

    # Combine all
    df_combined = pd.concat([
        preschools[['school_name', 'category', 'address', 'longitude', 'latitude']],
        moe_schools[['school_name', 'category', 'address', 'longitude', 'latitude']],
        tertiary[['school_name', 'category', 'address', 'longitude', 'latitude']]
    ], ignore_index=True)

    sch_df_combined = df_combined.map(lambda x: x.strip().upper() if isinstance(x, str) else x)
    sch_df_combined = df_combined.dropna()

    sch_df_combined.to_json('sch_df_combined.json', orient='records', lines=True)


def load_schools_to_bq(**kwargs):
    sch_df_combined = pd.read_json('sch_df_combined.json', lines=True)
    sch_df_combined.to_gbq(
    destination_table=f"{PROJECT_ID}.school_data.schools",  
    project_id=PROJECT_ID,  
    if_exists="replace",
)

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
    )

    extract_bus_stops_task = PythonOperator(
        task_id="extract_bus_stops_task",
        python_callable=extract_bus_stops,
    )

    extract_mrt_lrt_data_task = PythonOperator(
        task_id="extract_mrt_lrt_data_task",
        python_callable=extract_mrt_lrt_data,
    )

    transform_bus_data_task = PythonOperator(
        task_id="transform_bus_data_task",
        python_callable=transform_bus_data,
    )

    transform_mrt_lrt_data_task = PythonOperator(
        task_id="transform_mrt_lrt_data_task",
        python_callable=transform_mrt_lrt_data,
    )

    load_bus_data_task = PythonOperator(
        task_id="load_bus_data_task",
        python_callable=load_bus_data_to_bq,
    )

    load_mrt_lrt_data_task = PythonOperator(
        task_id="load_mrt_lrt_data_task",
        python_callable=load_mrt_lrt_data_to_bq,
    )

    extract_schools_task = PythonOperator(
        task_id="extract_schools_task",
        python_callable=extract_schools,
    )

    transform_schools_task = PythonOperator(
        task_id="transform_schools_task",
        python_callable=transform_schools,
    )

    load_schools_task = PythonOperator(
        task_id="load_schools_task",
        python_callable=load_schools_to_bq,
    )

    [extract_bus_routes_task, extract_bus_stops_task] >> transform_bus_data_task >> load_bus_data_task
    extract_mrt_lrt_data_task >> transform_mrt_lrt_data_task >> load_mrt_lrt_data_task
    extract_schools_task >> transform_schools_task >> load_schools_task
