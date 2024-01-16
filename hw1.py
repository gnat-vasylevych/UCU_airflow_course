from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import json
from datetime import datetime
import requests


WEATHER_API_KEY = "a3577eddb7354e317df888f2858f5255"
cities = {
    "Lviv": {
        "lat": "49.83826",
        "lon": "24.02324"
    },
    "Kyiv": {
        "lat": "50.450001",
        "lon": "30.523333"
    },
    "Kharkiv": {
        "lat": "49.988358",
        "lon": "	36.232845"
    },
    "Odesa": {
        "lat": "46.482952",
        "lon": "30.712481"
    },
    "Zhmerynka": {
        "lat": "49.03705",
        "lon": "28.11201"
    }
}


with DAG(dag_id="hw1", schedule_interval="@daily", start_date=datetime(2024, 1, 16, 0, 0, 0, 0), catchup=False) as dag:
    print("Airflow sucks")
    db_create = SqliteOperator(task_id="create_table_sqlite", sqlite_conn_id="airflow_conn",
                                       sql="""
     CREATE TABLE IF NOT EXISTS measures
     (
    timestamp TEXT,
    temp FLOAT,
    humidity float,
    clouds float,
    wind_speed float,
    city TEXT
     );"""
)
    print("Airflow sucks again")

    def _check_api(ti):
        base_url = "https://api.openweathermap.org/data/3.0/"
        endpoint = "onecall"

        params = {"appid": WEATHER_API_KEY, "date": "2023-12-11",
                      "lat": cities["Lviv"]["lat"],
                      "lon": cities["Lviv"]["lon"]
                      }

        response = requests.get(base_url + endpoint, params=params)
        print(response.json())


    check_api = PythonOperator(task_id="check_api", python_callable=_check_api)

    # IT DOESN'T WORK SO I TRIED VIA PYTHON OPERATOR

    # check_api = HttpSensor(
    #     task_id="check_api",
    #     http_conn_id="weather_conn",
    #     endpoint="onecall",
    #     request_params={"appid": WEATHER_API_KEY,
    #                     "lat": Variable.get("LVIV_LAT"),
    #                     "lon": Variable.get("LVIV_LON")
    #           }
    # )


    def extract_process_inject_weather_all_cities(ti, **kwargs):
        for city in cities:
            extract_data = SimpleHttpOperator(
                task_id=f"extract_data_{city}",
                http_conn_id="weather_conn",
                endpoint="onecall/day_summary",
                data={"appid": WEATHER_API_KEY, "date": kwargs['execution_date'],
                      "lat": cities[city]["lat"],
                      "lon": cities[city]["lon"]
                      },
                method="GET",
                response_filter=lambda x: json.loads(x.text),
                log_response=True
            )

            def _process_weather(ti):
                info = ti.xcom_pull(f"extract_data_{city}")
                timestamp = info["data"]["dt"]
                temp = info["temperature"]["max"]
                humidity = info["humidity"]["afternoon"]
                clouds = info["cloud_cover"]["afternoon"]
                wind_speed = info["wind"]["max"]["speed"]
                return timestamp, temp, humidity, clouds, wind_speed

            process_data = PythonOperator(
                task_id="process_data",
                python_callable=_process_weather
            )

            inject_data = SqliteOperator(
                task_id="inject_data",
                sqlite_conn_id="airflow_conn",
                sql=f"""
            INSERT INTO measures (timestamp, temp, humidity, clouds, wind_speed) VALUES
            ({{ti.xcom_pull(task_ids='process_data')[0]}},
            {{ti.xcom_pull(task_ids='process_data')[1]}}),
            {{ti.xcom_pull(task_ids='process_data')[2]}}),
            {{ti.xcom_pull(task_ids='process_data')[3]}}),
            {{ti.xcom_pull(task_ids='process_data')[4]}}),
            {city};
            """,
            )
            extract_data >> process_data >> inject_data


    etl_data = PythonOperator(
        task_id="etl_data",
        python_callable=extract_process_inject_weather_all_cities
    )

    db_create >> check_api >> etl_data
