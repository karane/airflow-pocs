from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import random
import logging

# Create logger with __name__
logger = logging.getLogger(__name__)

def choose_branch():
    choice = random.choice(['run_this_first', 'run_this_second'])
    logger.info(f"Branching to: {choice}")
    return choice

def print_message(msg):
    def _inner():
        logger.info(msg)
    return _inner

with DAG(
    dag_id="branching_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start")

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_branch,
    )

    task_a = PythonOperator(
        task_id="run_this_first",
        python_callable=print_message("🟢 Running first branch")
    )

    task_b = PythonOperator(
        task_id="run_this_second",
        python_callable=print_message("🔵 Running second branch")
    )

    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success"
    )

    start >> branching >> [task_a, task_b] >> join
