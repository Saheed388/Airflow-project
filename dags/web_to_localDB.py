import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from utils.db_injestion import db_conn_ingestion

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")



URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv.gz'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv.gz'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ dag_run.logical_date.strftime(\'%Y_%m\') }}'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/HP/Documents/Alt_school/venv/Altschool_env/data-engineering-repo/Spotify_assignment1/poetic-now-399015-1156f00817c4.json'


DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="LoadDataWebToDB",
    description="Job to move data from website to local PostgreSQL DB",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 2 * *", # At 06:00 on day 2 of every month
    max_active_runs=1,
    catchup=True,
    tags=["Website_to_local_PostgreSQL_DB"],
) as dag:

    start = BashOperator(task_id="start", bash_command="echo 'Start'")

    download_file = BashOperator(
         task_id="download_file",
         bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}',
    )

    ingestion_data = PythonOperator(
        task_id="ingestion_data",
        python_callable=db_conn_ingestion,
        op_kwargs={
            "bucket_name": "altschool_bucket338",
            "destination_blob_name": "saeed_cloud_spotify_songs_data.csv",
        },
    )

    delete_file = BashOperator(
         task_id="delete_file",
         bash_command=f'rm {OUTPUT_FILE_TEMPLATE}',
    )

    end = BashOperator(task_id="end", bash_command="echo 'End'")

    start >> download_file >> ingestion_data >> delete_file >> end
