from airflow import DAG
from datetime import datetime, timedelta
from web.operators.grouppro import WebToGCSHKOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy import DummyOperator

# Define your default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Create your DAG
with DAG('data_fetch_dag', default_args=default_args, schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')

    # Fetch and store data in GCS
    fetch_and_store_data = WebToGCSHKOperator(
        task_id='download_to_gcs',
        gcs_bucket_name='alt_new_bucket',
        gcs_object_name='eviction_data.csv',
        api_endpoint='https://data.cityofnewyork.us/resource/6z8x-wfk4.json',
        api_headers={
            "X-App-Token": '********************',
            "X-App-Secret": '************************',
        },
        api_params={
            "$limit": 2000,
        },
    )

    # Push data from GCS to BigQuery
    upload_to_bigquery = GCSToBigQueryOperator(
        task_id='upload_to_bigquery',
        source_objects=['eviction_data.csv'],
        destination_project_dataset_table='poetic-now-399015.eviction_dataset.eviction_table111',
        schema_fields=[],  # Define schema fields if needed
        skip_leading_rows=1,
        source_format='CSV',
        field_delimiter=',',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',  # Change to your desired write disposition
        autodetect=True, 
        bucket ="alt_new_bucket",
    )


    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> fetch_and_store_data >> upload_to_bigquery >> end
