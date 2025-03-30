# IS3107-Group-8
This is the Project source code for IS3107 group 8
Group members: Chian Xin Tong, Fong Kai Jun, Melvin Ang, Ng Shi Ying, Whang Sok Yang

To set up, copy the docker-compose.yaml file and initialise the environemnt in a folder you want to run the airflow in
```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
run the follwoing commands in order

```bash
 docker-compose up -d
```

```bash
 docker info  
```

```bash
 docker compose up airflow-init
```

```bash
 docker compose up  
```

Ensure that you have the requirements.txt file and dags, data and scripts folders available.

Please store your API keys in the variables to ensure that they are encrypted and to prevent comitting to Github

To power down, run

```bash
docker-compose down 
```
