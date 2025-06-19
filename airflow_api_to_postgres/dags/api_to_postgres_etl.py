from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2

API_URL = "https://jsonplaceholder.typicode.com/users"

POSTGRES_CONN = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

def extract():
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()

def transform(raw_data):
    # Pick only id, name, email
    return [(user["id"], user["name"], user["email"]) for user in raw_data]

def load_to_postgres(users):
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            name TEXT,
            email TEXT
        );
    """)
    cur.executemany("INSERT INTO users (id, name, email) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING;", users)
    conn.commit()
    cur.close()
    conn.close()

def etl():
    raw = extract()
    transformed = transform(raw)
    load_to_postgres(transformed)

with DAG(
    dag_id="api_to_postgres_etl",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=etl
    )

etl_task
