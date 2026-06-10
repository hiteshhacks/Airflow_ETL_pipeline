from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

from datetime import datetime
from airflow.decorators import task 
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
 

# define DAG
with DAG(
    dag_id="ETL_pipeline",
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False
)as dag:
    
    # step 1: Create table if it does not exist
    @task
    def create_table():
        # initialize Postgres hook
        postgres_hook =  PostgresHook(postgres_conn_id="postgres_default")


        # create table query
        create_table_query= """
        CREATE TABLE IF NOT EXISTS nasa_apod (
           id SERIAL PRIMARY KEY,
           title VARCHAR(255),
           explanation TEXT,
           url TEXT,
           date DATE,
           media_type VARCHAR(50)  
           );
"""

        # execute query
        postgres_hook.run(create_table_query)


    # step 2: Extract data from NASA API

    extract_apod=HttpOperator(
        task_id="extract_apod",
        http_conn_id="nasa_api",  ## connection id defined in Airflow UI for nasa api
        endpoint="/planetary/apod", ## nasa api endpoint for astronomy picture of the day
        method="GET",
        data={"api_key":"{{ conn.nasa_api.extra_dejson.api_key }}"}, ## use the api key from the connection configuration in Airflow UI
        response_filter=lambda response: response.json()
    )



    # step 3: Transform data (pick the information we need to save )

    @task
    def transform_data(response):
        apod_data ={
            "title":response.get("title",""),
            "explanation":response.get("explanation",""),
            "url":response.get("url",""),
            "date":response.get("date",""),
            "media_type":response.get("media_type","")
        }
        return apod_data
    



    # step 4: Load data into Postgres database
    @task
    def load_data_to_postgres(apod_data):
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")

        insert_query= """
        INSERT INTO nasa_apod (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);   

"""
        postgres_hook.run(insert_query, parameters=(
            apod_data["title"],
            apod_data["explanation"],
            apod_data["url"],
            apod_data["date"],
            apod_data["media_type"]
        ))
    # step 5: verify data in DBviewer

    # step6 : set dependencies between tasks
    # Extract
    create_table() >> extract_apod 
    api_response = extract_apod.output

    # Transform
    transformed_data= transform_data(api_response)

    # Load
    load_data_to_postgres(transformed_data)
