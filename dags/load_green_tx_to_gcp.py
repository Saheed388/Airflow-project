import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from web.operators.webToGcs_hook import WebToGCSHKOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

PROJECT_ID = "poetic-now-399015"
DESTINATION_BUCKET = "alt_new_bucket"
ENDPOINT = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
SERVICE = "green"
DESTINATION_PATH = SERVICE+'_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv.gz'

with DAG(
    dag_id="Load-Green-Tax-Data-To-GCS",
    description="Job to move data from website to local Postgresql DB",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 2 * *",
    max_active_runs=1,
    catchup=True,
    tags=["Website-to-GCS-Bucket"],
) as dag:
    start = DummyOperator(task_id="start")

    download_to_gcs = WebToGCSHKOperator(
        task_id="download_to_gcs",
        endpoint=ENDPOINT,
        destination_path=DESTINATION_PATH,
        destination_bucket=DESTINATION_BUCKET,
        service=SERVICE,
    )

    end = DummyOperator(task_id="end")

    start >> download_to_gcs >> end
