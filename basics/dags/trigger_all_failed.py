from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def fail():
    raise Exception("❌ Intentionally failing")

with DAG("trigger_all_failed", start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:
    start = EmptyOperator(task_id="start")

    t1 = PythonOperator(task_id="fail_1", python_callable=fail)
    t2 = PythonOperator(task_id="fail_2", python_callable=fail)

    final = PythonOperator(
        task_id="final_task",
        python_callable=lambda: print("🔥 All previous tasks failed"),
        trigger_rule=TriggerRule.ALL_FAILED
    )

    start >> [t1, t2] >> final
