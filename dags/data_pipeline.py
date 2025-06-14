from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import logging

DATA_PATH = "/opt/airflow/data"
API_URL = "https://open.er-api.com/v6/latest/USD"

logger = logging.getLogger(__name__) # important to capture the filename and line

def download_data(**kwargs):
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()
    os.makedirs(DATA_PATH, exist_ok=True)
    file_path = os.path.join(DATA_PATH, "raw_rates.json")
    with open(file_path, "w") as f:
        json.dump(data, f)
    # print(f"Downloaded exchange rates saved to {file_path}")
    logger.info(f"Downloaded exchange rates saved to {file_path}")

def process_data(**kwargs):
    file_path = os.path.join(DATA_PATH, "raw_rates.json")
    with open(file_path, "r") as f:
        raw_data = json.load(f)

    processed = {
        "base": raw_data["base_code"],
        "time_updated": raw_data["time_last_update_utc"],
        "rates": {
            "BRL": raw_data["rates"]["BRL"],
            "EUR": raw_data["rates"]["EUR"],
            "GBP": raw_data["rates"]["GBP"],
        }
    }

    processed_path = os.path.join(DATA_PATH, "processed_rates.json")
    with open(processed_path, "w") as f:
        json.dump(processed, f)

    # print(f"Processed rates saved to {processed_path}")
    logger.info(f"Processed rates saved to {processed_path}")

def upload_data(**kwargs):
    processed_path = os.path.join(DATA_PATH, "processed_rates.json")
    upload_dir = os.path.join(DATA_PATH, "upload")
    os.makedirs(upload_dir, exist_ok=True)
    upload_path = os.path.join(upload_dir, "processed_rates.json")

    with open(processed_path, "r") as src, open(upload_path, "w") as dst:
        dst.write(src.read())

    # print(f"Uploaded rates to {upload_path}")
    logger.info(f"Uploaded rates to {upload_path}")

def notify_success(**kwargs):
    # print("Currency data pipeline completed successfully!")
    logger.info("Currency data pipeline completed successfully!")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "currency_data_pipeline",
    default_args=default_args,
    description="ETL DAG to fetch and process currency exchange rates",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "currency"],
) as dag:

    download_task = PythonOperator(
        task_id="download_data",
        python_callable=download_data,
    )

    process_task = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    upload_task = PythonOperator(
        task_id="upload_data",
        python_callable=upload_data,
    )

    notify_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    download_task >> process_task >> upload_task >> notify_task
