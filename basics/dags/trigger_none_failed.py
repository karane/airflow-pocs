from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

with DAG("trigger_none_failed", start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:
    start = EmptyOperator(task_id="start")

    t1 = PythonOperator(task_id="ok_1", python_callable=lambda: print("✅ Success"))
    t2 = PythonOperator(task_id="ok_2", python_callable=lambda: print("✅ Success"))

    final = PythonOperator(
        task_id="final_task",
        python_callable=lambda: print("🎉 No upstream tasks failed"),
        trigger_rule=TriggerRule.NONE_FAILED
    )

    start >> [t1, t2] >> final
