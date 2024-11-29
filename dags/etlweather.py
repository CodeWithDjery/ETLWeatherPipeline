from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

# Latitude and longitude of (Gabon/Libreville in this case)
LATITUDE = '0.3901'
LONGITUDE = '9.4544'
POSTGRES_CONN_ID = 'postgres_conn_id'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': 60,
}


# Create the DAG
with DAG(
    dag_id='etl_weather_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    @task
    def extract_weather_info():
        """
        Extracts weather information from the Open Meteo API.
        """
        
        # Use the HttpsHook to make the API request
        hook = HttpHook(
            http_conn_id=API_CONN_ID,
            method='GET',
        )
        
        # API endpoint for weather data
        # https://api.open-meteo.com/v1/forecast?latitude=0.3901&longitude=9.4544&current_weather=true
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        
        # Make the API request
        response = hook.run(endpoint)
        
        if response.status_code == 200:
            return response.json()
        raise Exception(f"API request failed with status code {response.status_code}")
        
    @task()
    def transform_weather_info(weather_data):
        """Transform the extracted weather data."""
        current_weather = weather_data['current_weather']
        process_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return process_data
    
    @task()
    def load_weather_info(processed_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            processed_data['latitude'],
            processed_data['longitude'],
            processed_data['temperature'],
            processed_data['windspeed'],
            processed_data['winddirection'],
            processed_data['weathercode']
        ))

        conn.commit()
        cursor.close()
        
    ## DAG workflow
    weather_data = extract_weather_info()
    transformed_data = transform_weather_info(weather_data)
    load_weather_info(transformed_data)
        