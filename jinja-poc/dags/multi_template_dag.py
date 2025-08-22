from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# Python callable using templates_dict
def print_params(**context):
    params = context["templates_dict"]
    print("Data interval start:", params["interval_start"])
    print("Data interval end:", params["interval_end"])
    print("Report filename:", params["report_filename"])
    print("Custom message:", params["message"])

# Python callable using op_kwargs
def print_op_kwargs(interval_start, interval_end, message):
    print("Op_kwargs interval start:", interval_start)
    print("Op_kwargs interval end:", interval_end)
    print("Op_kwargs message:", message)

with DAG(
    "multi_template_dag",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # PythonOperator with templates_dict
    task1 = PythonOperator(
        task_id="task_templates_dict",
        python_callable=print_params,
        templates_dict={
            "interval_start": "{{ data_interval_start }}",
            "interval_end": "{{ data_interval_end }}",
            "report_filename": "report_{{ ds }}.csv",
            "message": "Hello from DAG run {{ run_id }}",
        },
    )

    # PythonOperator with op_kwargs templated
    task2 = PythonOperator(
        task_id="task_op_kwargs",
        python_callable=print_op_kwargs,
        op_kwargs={
            "interval_start": "{{ data_interval_start }}",
            "interval_end": "{{ data_interval_end }}",
            "message": "Another message for {{ ds }}",
        },
    )

    # BashOperator using templated command
    task3 = BashOperator(
        task_id="task_bash",
        bash_command="echo 'Processing {{ ds }} from {{ data_interval_start }} to {{ data_interval_end }}'",
    )

    # Task dependencies
    task1 >> task2 >> task3
