# IS3107-Group-8
This is the Project source code for IS3107 group 8
Group members: Chian Xin Tong, Fong Kai Jun, Melvin Ang, Ng Shi Ying, Whang Sok Yang

# Airflow Setup Instructions

Follow these steps to set up and run Airflow locally using Docker Compose.

## Prerequisites
- Ensure Docker and Docker Compose are installed on your system.
- Prepare the following directories and files:

```
project-root/
├── dags/
├── logs/
├── plugins/
├── config/
│   └── google-service-account.json
├── scripts/
├── data/
├── requirements.txt
└── docker-compose.yaml
```

## Initial Setup

1. Create the required directories and `.env` file:

```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

2. Ensure your `docker-compose.yaml` file is in the root of your project folder.

## Starting Airflow

Run the following commands sequentially in your project folder:

```bash
docker-compose up -d
```

Verify Docker is running correctly:

```bash
docker info
```

Initialise Airflow (only needed during first-time setup or after resetting the environment):

```bash
docker-compose up airflow-init
```

Start Airflow services:

```bash
docker-compose up
```

## API Keys and Secrets

- Store your API keys securely using Airflow Variables to avoid exposure.
- **Do not** commit API keys or other sensitive data to GitHub.

## Google Service Account

- Add your Google service account JSON file to the `config/` folder.
- Update your Airflow connections or environment variables accordingly to reference this file.

## Shutting Down Airflow

To stop Airflow and related services, use:

```bash
docker-compose down
```

## Additional Notes

- Verify that your `requirements.txt` file is correctly set up with all Python dependencies for your DAGs.
- Confirm that your `dags/`, `scripts/`, and `data/` folders contain the necessary files.