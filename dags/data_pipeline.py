from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from utils import extract_data, transform_data, load_data

defaults_args = {
    'owner': 'airflow',
    'email': 'nonsoskyokpara@gmail.com',
    'email_on_failure': True,
    #'depends_on_past': True,
    'retries': 3,
    'retry delay': timedelta(minutes=5),
    'schedule_interval': '@daily'
}

with DAG(
'weather_data_pipeline',
default_args=defaults_args,
description='A simple weather data pipeline',
tags=['weather', 'etl', 'pipeline', 'data engineering'],
) as dag:
    extract_task = PythonOperator(
        task_id = 'extract_weather_data',
        python_callable = extract_data
    )

    transform_task = PythonOperator(
        task_id = 'transform_weather_data',
        python_callable = transform_data
    )

    load_task = PythonOperator(
        task_id = 'load_weather_data',
        python_callable = load_data
    )

    extract_task >> transform_task >> load_task