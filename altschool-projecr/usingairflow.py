from airflow import DAG
from datetime import datetime, timedelta
from web.operators.grouppro import WebToGCSHKOperator
from airflow.operators.dummy import DummyOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG('my_data_fetch_dag', default_args=default_args, schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')

    fetch_and_store_data = WebToGCSHKOperator(
        task_id='download_to_gcs',
        gcs_bucket_name='alt_new_bucket',
        gcs_object_name='eviction_data.csv',
        api_endpoint='https://data.cityofnewyork.us/resource/6z8x-wfk4.json',
        api_headers={
            "X-App-Token": 'H5iB0k6uYvlqi0k6yobmF0xmY',
            "X-App-Secret": 'rAP6bbNMln-aNQmnd94NlSTCiklYhOhfO3B3',
        },
        api_params={
            "$limit": 2000,
        },
    )

    end = DummyOperator(task_id='end')

    start >> fetch_and_store_data >> end
