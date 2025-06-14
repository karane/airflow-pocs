from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

with DAG("trigger_all_done", start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:
    start = EmptyOperator(task_id="start")

    t1 = PythonOperator(task_id="ok", python_callable=lambda: print("✅ Success"))
    t2 = PythonOperator(task_id="fail", python_callable=lambda: (_ for _ in ()).throw(Exception("💥")))

    final = PythonOperator(
        task_id="final_task",
        python_callable=lambda: print("🏁 All upstream tasks are done (even if failed/skipped)"),
        trigger_rule=TriggerRule.ALL_DONE
    )

    start >> [t1, t2] >> final
