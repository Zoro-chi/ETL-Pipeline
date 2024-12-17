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
        pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
        pg_hook.run(create_table_query)

    # Step 2: Extract data from NASA API (APOD) [Extract Pipeline]
    extract_api = SimpleHttpOperator(
        task_id="extract_api",
        http_conn_id="nasa_api",  # Connection ID defined in Airflow
        endpoint="planetary/apod",
        method="GET",
        data={
            "api_key": "{{  conn.nasa_api.extra_dejson.api_key }}"
        },  # Use the API key from the connection
        response_filter=lambda response: json.loads(
            response.text
        ),  # Convert the response to JSON
    )

    # Step 3: Transform data (Save Relevant data) [Transform Pipeline]
    @task
    def transform_data(response):
        # Extract relevant data from the response
        transformed_data = {
            "title": response["title"] if "title" in response else "",
            "explanation": response["explanation"] if "explanation" in response else "",
            "url": response["url"] if "url" in response else "",
            "date": response["date"] if "date" in response else "",
            "media_type": response["media_type"] if "media_type" in response else "",
        }
        return transformed_data

    # Step 4: Load data into Postgres [Load Pipeline]
    @task
    def load_data_to_postgres(transformed_data):
        # Initialize PostgresHook
        pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
        # Define the insert query
        insert_query = """
        INSERT INTO nasa_apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        # Run the insert query
        pg_hook.run(
            insert_query,
            parameters=(
                transformed_data["title"],
                transformed_data["explanation"],
                transformed_data["url"],
                transformed_data["date"],
                transformed_data["media_type"],
            ),
        )

    # Step 5: Verify data in Postgres

    # Step 6: Define the task dependencies
    create_table() >> extract_api
    api_response = extract_api.output
    transformed_data = transform_data(api_response)
    load_data_to_postgres(transformed_data)
