import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class CSVToPostgresOperator(BaseOperator):
    """
    Custom operator to load a local CSV file into a Postgres table.
    """

    @apply_defaults
    def __init__(
        self,
        csv_path: str,
        postgres_conn_id: str,
        table: str,
        replace: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.csv_path = csv_path
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.replace = replace

    def execute(self, context):
        self.log.info(f"Reading CSV from {self.csv_path}")
        df = pd.read_csv(self.csv_path)

        self.log.info(f"Loaded {len(df)} rows, writing to {self.table}")

        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        engine = pg_hook.get_sqlalchemy_engine()

        if_exists = "replace" if self.replace else "append"
        df.to_sql(self.table, engine, if_exists=if_exists, index=False)

        self.log.info(f"Inserted {len(df)} rows into {self.table}")
        return f"Inserted {len(df)} rows into {self.table}"
