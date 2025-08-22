from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def use_dict(**context):
    # Access the rendered templated values via templates_dict
    params = context["templates_dict"]
    print("Data interval start:", params["interval_start"])
    print("Data interval end:", params["interval_end"])

with DAG(
    "dict_example",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="show_interval",
        python_callable=use_dict,
        # Use templates_dict to enable Jinja rendering
        templates_dict={
            "interval_start": "{{ data_interval_start }}",
            "interval_end": "{{ data_interval_end }}",
        },
    )
