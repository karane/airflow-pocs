# dags/csv_to_postgres_dag.py
from datetime import datetime
from airflow import DAG
from operators.csv_to_postgres_operator import CSVToPostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="csv_to_postgres_example",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    # Task 1: Load CSV into Postgres
    load_users = CSVToPostgresOperator(
        task_id="load_users_csv",
        csv_path="/opt/airflow/data/users.csv",
        postgres_conn_id="airflow_db",
        table="users",
        replace=True,
    )

    # Task 2: Check if users were loaded
    check_users = PostgresOperator(
        task_id="check_users",
        postgres_conn_id="airflow_db",
        sql="SELECT COUNT(*) AS total_users FROM users;",
    )

    # Set dependency
    load_users >> check_users
