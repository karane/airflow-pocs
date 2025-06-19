# Airflow API to PostgreSQL ETL

This project demonstrates a simple **ETL pipeline using Apache Airflow**. It extracts user data from a public API (`https://jsonplaceholder.typicode.com/users`), transforms it, and loads it into a **PostgreSQL** database.

The project uses **Docker Compose** to run:
- Apache Airflow (`webserver`, `scheduler`)
- PostgreSQL
- Airflow initialization container

---

##  Requirements

- Docker + Docker Compose
- Internet access to reach the API
  
---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/karane/airflow_api_to_postgres.git
cd airflow_api_to_postgres
```

### 2. Generate a secret key (optional)

Run this to generate a secure key:

```bash
echo "AIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -hex 32)" > .env
```
---

### 3. Build and initialize the project

First-time only:

```bash
docker compose run --rm airflow-init
```

This will:
- Install dependencies
- Initialize Airflow DB
- Create an admin user

---

### 4. Start Airflow

```bash
docker compose up
```

Open Airflow UI in your browser at: [http://localhost:8080](http://localhost:8080)

- **Username**: `admin`
- **Password**: `admin`

---

### 5. Trigger the ETL DAG

- In Airflow UI, look for the DAG named: `api_to_postgres_etl`
- Toggle it **on**
- Click the "Play" ▶️ button to trigger a manual run

---

## 🧪 Check the Data

After the DAG runs, inspect the PostgreSQL database.

### Connect to PostgreSQL:

```bash
docker exec -it $(docker ps -qf "ancestor=postgres:13") psql -U airflow -d airflow
```

### Query the table:

```sql
SELECT * FROM users;
```


---

## Customization Ideas

- Replace the API endpoint with your own REST API
- Add DBT for transformations
- Use Airflow Connections UI for managing credentials
- Store data in S3 or BigQuery

---

## Cleanup

To stop and remove containers:

```bash
docker compose down
```

To remove volumes/data as well:

```bash
docker compose down -v
```
