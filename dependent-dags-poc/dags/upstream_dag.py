from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def push_message(**context):
    message = f"Hello from upstream at {context['ds']}"
    context["ti"].xcom_push(key="upstream_message", value=message)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="upstream_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    finalize_task = PythonOperator(
        task_id="finalize_task",
        python_callable=push_message,
        provide_context=True,
    )
