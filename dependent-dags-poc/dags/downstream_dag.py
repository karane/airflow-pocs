from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def read_message(**context):
    # pull XCom from upstream
    ti = context["ti"]
    message = ti.xcom_pull(
        key="upstream_message",
        task_ids="finalize_task",
        dag_id="upstream_dag"
    )
    print(f"Downstream DAG received: {message}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="downstream_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",   # same schedule as upstream
    catchup=False,
    default_args=default_args,
) as dag:

    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_upstream",
        external_dag_id="upstream_dag",
        external_task_id="finalize_task",  # wait for this task
        poke_interval=30,
        timeout=600,
        mode="poke", # you may also use "reschedule", which reschedules this dag itself
    )

    read_params = PythonOperator(
        task_id="read_params",
        python_callable=read_message,
        provide_context=True,
    )

    wait_for_upstream >> read_params
