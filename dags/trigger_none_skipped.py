from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

def skip_task(**kwargs):
    from airflow.exceptions import AirflowSkipException
    raise AirflowSkipException("⏭ Skipping this task")

with DAG("trigger_none_skipped", start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:
    start = EmptyOperator(task_id="start")

    t1 = PythonOperator(task_id="ok", python_callable=lambda: print("✅ Success"))
    t2 = PythonOperator(task_id="skip", python_callable=skip_task)

    final = PythonOperator(
        task_id="final_task",
        python_callable=lambda: print("🚫 This runs only if nothing is skipped"),
        trigger_rule=TriggerRule.NONE_SKIPPED
    )

    start >> [t1, t2] >> final
