from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def succeed():
    logger.info("✅ This task will succeed.")

def fail():
    raise Exception("❌ This task fails on purpose.")

with DAG(
    dag_id="trigger_one_success",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start")

    task_success = PythonOperator(
        task_id="task_success",
        python_callable=succeed,
    )

    task_fail = PythonOperator(
        task_id="task_fail",
        python_callable=fail,
    )

    join = PythonOperator(
        task_id="final_task",
        python_callable=lambda: logger.info("🎯 Final task ran due to one_success rule."),
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    start >> [task_success, task_fail] >> join
