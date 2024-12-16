from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json


# Define the DAG
with DAG(
    dag_id="nasa_apod",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Step 1: Create table in Postgres if it doesn't exist
    @task
    def create_table():
        # Define the create table query
        create_table_query = """
        CREATE TABLE IF NOT EXISTS nasa_apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        # Initialize PostgresHook and run the query
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        pg_hook.run(create_table_query)

    # Step 2: Extract data from NASA API (APOD) [Extract Pipeline]

    # Step 3: Transform data (Save Relevant data) [Transform Pipeline]

    # Step 4: Load data into Postgres [Load Pipeline]

    # Step 5: Verify data in Postgres

    # Step 6: Define the task dependencies
