from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import logging

logger = logging.getLogger(__name__)

def push_data(**context):
    value = random.randint(1, 100)
    logger.info(f"Pushing value to XCom: {value}")
    context['ti'].xcom_push(key='my_number', value=value)

def pull_data(**context):
    value = context['ti'].xcom_pull(key='my_number', task_ids='push_data')
    logger.info(f"Pulled value from XCom: {value}")

with DAG(
    dag_id='xcom_example',
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    push_task = PythonOperator(
        task_id='push_data',
        python_callable=push_data,
        provide_context=True,
    )

    pull_task = PythonOperator(
        task_id='pull_data',
        python_callable=pull_data,
        provide_context=True,
    )

    push_task >> pull_task
