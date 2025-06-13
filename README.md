# Airflow POC

This project sets up a basic Apache Airflow environment with Docker Compose.

## Quick Start

1. Initialize Airflow metadata DB:

    ```bash
    docker compose run airflow-webserver airflow db init
    ```

2. Create the admin user:

    ```bash
    docker compose run airflow-webserver airflow users create \
        --username admin --password admin \
        --firstname Admin --lastname User \
        --role Admin --email admin@example.com
    ```

4. Start the environment:

    ```bash
    docker compose up
    ```

5. Visit the web UI:  
    a. In your browser enter: `http://localhost:8080`  
    b. Login with admin / admin

You should see a DAG called hello_airflow.