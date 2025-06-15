from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

default_args = {
    "start_date": datetime(2023, 1, 1),
    "retries": 0
}

with DAG(
    dag_id="bash_operator_examples",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Demonstration of BashOperator use cases",
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Hello World
    with TaskGroup("hello_world") as hello_group:
        BashOperator(
            task_id="say_hello",
            bash_command='echo "[INFO] Hello from Bash!"'
        )

    # 2. Date and Uptime
    with TaskGroup("date_uptime") as date_uptime_group:
        log_date = BashOperator(
            task_id="print_date",
            bash_command='echo "[INFO] Current date is:" && date'
        )
        log_uptime = BashOperator(
            task_id="print_uptime",
            bash_command='echo "[INFO] Uptime is:" && uptime'
        )
        log_date >> log_uptime

    # 3. Create and List Directory
    with TaskGroup("create_and_list_dir") as dir_group:
        mkdir = BashOperator(
            task_id="create_directory",
            bash_command='mkdir -p /tmp/airflow_demo && echo "[INFO] Directory created: /tmp/airflow_demo"'
        )
        listdir = BashOperator(
            task_id="list_directory",
            bash_command='echo "[INFO] Listing directory:" && ls -la /tmp/airflow_demo'
        )
        mkdir >> listdir

    # 4. Export Environment Variable
    with TaskGroup("env_var") as env_group:
        export_var = BashOperator(
            task_id="export_and_print",
            bash_command='MY_VAR="Airflow Rocks!" && echo "[INFO] Value of MY_VAR: $MY_VAR"'
        )

    # 5. Run a Bash Script
    class NoTemplateBashOperator(BashOperator):
        template_fields = []  # Disable jinja templating

    with TaskGroup("run_script") as script_group:
        run_script = NoTemplateBashOperator(
            task_id="run_shell_script",
            bash_command='bash /opt/airflow/scripts/run_me.sh'
        )

    # 6. Retry on Failure
    with TaskGroup("retry_flaky") as retry_group:
        flaky = BashOperator(
            task_id="might_fail",
            bash_command='if [[ $RANDOM -gt 20000 ]]; then echo "[INFO] Passed"; else echo "[ERROR] Failed"; exit 1; fi',
            retries=3,
            retry_delay=timedelta(seconds=5),
        )

    start >> [hello_group, date_uptime_group, dir_group, env_group, script_group, retry_group]
