import json
import pandas as pd
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Converts temperature from Kelvin to Fahrenheit
def kelvin_to_fahrenheit(kelvin):
    celcius = (kelvin - 273.15) * (9/5) + 32
    celcius = round(celcius, 3)  
    return celcius

# Transforms the extracted weather data from the API into a specific format
def transform_extracted_kumasi_weather_data(task_instance):
    # Pulling weather data from a previous task via XCom
    data = task_instance.xcom_pull(task_ids="group_a.tsk_extract_kumasi_weather_data")

    # Creating a dictionary to hold the transformed weather data
    weather_data = {}

    # Extracting and transforming weather data
    weather_data["City"] = data['name']
    weather_data["Weather_description"] = data["weather"][0]["description"]
    weather_data["Temp_fahrenheit"] = kelvin_to_fahrenheit(data["main"]["temp"])
    weather_data["Feels_like_fahrenheit"] = kelvin_to_fahrenheit(data["main"]["feels_like"])
    weather_data["Min_temp_fahrenheit"] = kelvin_to_fahrenheit(data["main"]["temp_min"])
    weather_data["Max_temp_fahrenheit"] = kelvin_to_fahrenheit(data["main"]["temp_max"])
    weather_data["Pressure"] = data["main"]["pressure"]
    weather_data["Humidity"] = data["main"]["humidity"]
    weather_data["Wind_speed"] = data["wind"]["speed"]
    weather_data["Time_of_record"] = datetime.fromtimestamp(data["dt"])  # Convert timestamp to datetime
    weather_data["Sunrise_time"] = datetime.fromtimestamp(data["sys"]["sunrise"])
    weather_data["Sunset_time"] = datetime.fromtimestamp(data["sys"]["sunset"])

    # Creating a DataFrame and saving it as CSV without the header
    weather_data_df = pd.DataFrame([weather_data])
    weather_data_df.to_csv("daily_weather_update_kumasi.csv", index=False, header=False)

# Loads weather data into the Postgres table using the COPY command
def load_weather_data_into_postgres():
    hook = PostgresHook(postgres_conn_id= 'postgres_conn')
    hook.copy_expert(
        sql = "COPY kumasi_weather_table FROM stdin WITH DELIMITER as ','"
        , filename= 'daily_weather_update_kumasi.csv' 
    )

# Joins data from two Postgres tables, converts it to a DataFrame, and saves it to an S3 bucket
def save_joined_data_to_s3(task_instance=None):
    # Establish connection to Postgres
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Execute the SQL query to join the weather data and lookup data
    query = """
    SELECT
        w.city,
        Weather_description,
        Temp_fahrenheit,
        Feels_like_fahrenheit,
        Min_temp_fahrenheit,
        Max_temp_fahrenheit,
        Pressure,
        Humidity,
        Wind_speed,
        Time_of_record,
        Sunrise_time,
        Sunset_time,
        region,
        census,
        land_area
    FROM
        kumasi_weather_table w
    INNER JOIN
        kumasi_lookup_table klt
    ON
        w.city = klt.city;
    """
    cursor.execute(query)
    data = cursor.fetchall()

    # Convert the result set into a Pandas DataFrame
    df = pd.DataFrame(data, columns=[
        'city', 'description', 'Temp_fahrenheit', 'Feels_like_fahrenheit',
        'Min_temp_fahrenheit', 'Max_temp_fahrenheit', 'Pressure', 'Humidity',
        'Wind_speed', 'Time_of_record', 'Sunrise_time', 'Sunset_time', 'region',
        'census', 'land_area'
    ])

    # Create a unique timestamp for the file
    dt_string = datetime.now().strftime("%d%m%Y%H%M%S")

    # Write the DataFrame to a CSV file in an S3 bucket
    df.to_csv(f"s3://parallel-airflow-bucket-bok/joined_weather_data_{dt_string}.csv", index=False)

    # Close cursor and connection
    cursor.close()
    conn.close()

# Default arguments for the DAG, specifying retries and start date
default_args = {
    "owner": "bright",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 17),
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

# DAG definition
with DAG("weather_dag_2", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:

    # Starting point of the pipeline
    start_pipeline = DummyOperator(task_id="tsk_start_pipeline")

    # Group of tasks related to extracting and loading data from S3 and Weather API
    with TaskGroup(group_id="group_a", tooltip="Extract_from_s3_and_weatherapi") as group_A:

        # Create a lookup table in Postgres
        create_table_1 = PostgresOperator(
            task_id="tsk_create_table_1",
            postgres_conn_id="postgres_conn",
            sql="""
                CREATE TABLE IF NOT EXISTS kumasi_lookup_table(
                    city VARCHAR(50), region VARCHAR(50), census BIGINT, land_area INT
                );
            """
        )

        # Truncate the lookup table to ensure it's empty before loading new data
        truncate_table = PostgresOperator(
            task_id="tsk_truncate_table",
            postgres_conn_id="postgres_conn",
            sql="TRUNCATE TABLE kumasi_lookup_table;"
        )

        # Load lookup data from S3 to Postgres
        uploadS3_to_postgres = PostgresOperator(
            task_id="tsk_upload_to_postgres",
            postgres_conn_id='postgres_conn',
            sql="""
                SELECT aws_s3.table_import_from_s3(
                    'kumasi_lookup_table', '', 
                    '(format csv, DELIMITER '','', HEADER true)', 
                    'parallel-airflow-bucket-bok', 'kumasi_city_lookup.csv', 'eu-west-2'
                );
            """
        )

        # Create weather data table in Postgres
        create_table_2 = PostgresOperator(
            task_id="tsk_create_table_2",
            postgres_conn_id="postgres_conn",
            sql="""
                CREATE TABLE IF NOT EXISTS kumasi_weather_table(
                    City VARCHAR(50), Weather_description VARCHAR(50), Temp_fahrenheit FLOAT,
                    Feels_like_fahrenheit FLOAT, Min_temp_fahrenheit FLOAT, Max_temp_fahrenheit FLOAT,
                    Pressure INT, Humidity INT, Wind_speed FLOAT, Time_of_record TIMESTAMP,
                    Sunrise_time TIMESTAMP, Sunset_time TIMESTAMP
                );
            """
        )

        # HttpSensor to check if the Weather API is ready
        kumasi_weather_api_ready = HttpSensor(
            task_id="tsk_kumasi_weather_api_ready",
            http_conn_id="weathermap_id",
            endpoint="/data/2.5/weather?q=Kumasi&appid=d1d125186b2bb28c3c2c4828f0cb7375"
        )

        # SimpleHttpOperator to extract weather data from the Weather API
        extract_kumasi_weather_data = SimpleHttpOperator(
            task_id="tsk_extract_kumasi_weather_data",
            http_conn_id="weathermap_id",
            endpoint="/data/2.5/weather?q=Kumasi&appid=d1d125186b2bb28c3c2c4828f0cb7375",
            method='GET',
            response_filter=lambda r: json.loads(r.text),
            log_response=True
        )

        # PythonOperator to transform and save the weather data into CSV
        transform_extracted_kumasi_weather_data = PythonOperator(
            task_id='tsk_transform_load_kumasi_weather_data',
            python_callable=transform_extracted_kumasi_weather_data
        )

        # PythonOperator to load the transformed weather data into Postgres
        load_weather_data_to_postgres = PythonOperator(
            task_id="tsk_load_weather_data_to_postgres",
            python_callable=load_weather_data_into_postgres
        )

        # Defining the task sequence for TaskGroup A
        create_table_1 >> truncate_table >> uploadS3_to_postgres
        create_table_2 >> kumasi_weather_api_ready >> extract_kumasi_weather_data >> transform_extracted_kumasi_weather_data >> load_weather_data_to_postgres

    # Postgres operator to join weather data with the lookup table
    join_data = PostgresOperator(
        task_id="tsk_join_data",
        postgres_conn_id="postgres_conn",
        sql="""
            SELECT
                w.city, Weather_description, Temp_fahrenheit, Feels_like_fahrenheit,
                Min_temp_fahrenheit, Max_temp_fahrenheit, Pressure, Humidity, Wind_speed,
                Time_of_record, Sunrise_time, Sunset_time, region, census, land_area
            FROM kumasi_weather_table w
            INNER JOIN kumasi_lookup_table klt ON w.city = klt.city
        """
    )

    # PythonOperator to save joined data to S3
    load_joined_data_to_s3 = PythonOperator(
        task_id="tsk_load_joined_data_to_s3",
        python_callable=save_joined_data_to_s3
    )

    # DummyOperator to mark the end of the pipeline
    end_pipeline = DummyOperator(task_id="tsk_end_pipeline")


    # Defining the overall DAG task sequence
    start_pipeline >> group_A >> join_data >> load_joined_data_to_s3 >> end_pipeline
