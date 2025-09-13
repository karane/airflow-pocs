# example_api_dag.py
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from api_sensors import APISensor

def process_api_data():
    print("Processing API data...")

with DAG(
    dag_id="api_sensor_example",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    wait_for_api = APISensor(
        task_id="wait_for_api",
        endpoint="https://jsonplaceholder.typicode.com/todos/1",
        expected_status=200,
        poke_interval=30,  # check every 30 seconds
        timeout=300,       # fail after 5 minutes
        mode="reschedule"  # frees worker while waiting
    )

    process_task = PythonOperator(
        task_id="process_api_data",
        python_callable=process_api_data
    )

    wait_for_api >> process_task
