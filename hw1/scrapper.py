from datetime import datetime
import logging
import json
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.decorators.task_group import TaskGroup
from airflow.utils.dates import days_ago


cities = ["Lviv", "Kharkiv", "Kyiv", "Odesa", "Zhmerynka"]


def _process_weather(ti, city):
    geo_lat, geo_lon = ti.xcom_pull(task_ids=f'process_geo_{city}')
    weather_data = ti.xcom_pull(task_ids=f'extract_data_{city}')
    weather_data_str = json.dumps(weather_data, indent=2)
    result_str = f"Weather Data for {city}: {weather_data_str}"
    print(result_str)


def _process_geo(ti, city):
    geo_data = ti.xcom_pull(task_ids=f'get_geo_{city}')
    logging.info(f"Geo Data for {city}: {geo_data}")
    print(f"lon: {geo_data[0]['lon']} lat: {geo_data[0]['lat']}")
    return geo_data[0]["lat"], geo_data[0]["lon"]


def _get_weather_api_url(ti, city):
    geo_lat, geo_lon = ti.xcom_pull(task_ids=f'process_geo_{city}')
    api_key = Variable.get("WEATHER_API_KEY")
    return f"https://api.openweathermap.org/data/3.0/onecall?lat={geo_lat}&lon={geo_lon}&exclude=hourly,daily&appid={api_key}"


with DAG(dag_id="dag_weather_v5", schedule_interval=None, start_date=datetime(2023, 11, 22), catchup=False) as dag:
    for city in cities:
        with TaskGroup(f"weather_tasks_{city}") as weather_tasks:
            get_geo = SimpleHttpOperator(
                task_id=f"get_geo",
                http_conn_id="weather_conn",
                endpoint="geo/1.0/direct",
                data={
                    "q": city,
                    "limit": 1,
                    "appid": Variable.get("WEATHER_API_KEY")
                },
                method="GET",
                response_filter=lambda x: json.loads(x.text),
                log_response=True 
            )

            process_geo = PythonOperator(
                task_id=f"process_geo",
                python_callable=_process_geo,
                op_args=[city]
            )

            check_api = HttpSensor( 
                task_id=f"check_api",
                http_conn_id="weather_conn",
                endpoint="data/3.0/onecall",
                request_params={
                    "lat": f"{{{{ task_instance.xcom_pull(task_ids='process_geo_{city}')[0] }}}}",
                    "lon": f"{{{{ task_instance.xcom_pull(task_ids='process_geo_{city}')[1] }}}}",
                    "exclude": "hourly,daily",
                    "appid": Variable.get("WEATHER_API_KEY")
                }
            )

            extract_data = SimpleHttpOperator(
                task_id=f"extract_data",
                http_conn_id="weather_conn",
                endpoint="data/3.0/onecall",
                data={
                    "lat": f"{{{{ task_instance.xcom_pull(task_ids='process_geo_{city}')[0] }}}}",
                    "lon": f"{{{{ task_instance.xcom_pull(task_ids='process_geo_{city}')[1] }}}}",
                    "exclude": "hourly,daily",
                    "appid": Variable.get("WEATHER_API_KEY")
                }, 
                method="GET",
                response_filter=lambda x: json.loads(x.text),
                log_response=True 
            )

            process_weather = PythonOperator(
                task_id=f"process_weather",
                python_callable=_process_weather,
                op_args=[city]
            )


            get_geo >> process_geo >> check_api >> extract_data >> process_weather
    weather_tasks