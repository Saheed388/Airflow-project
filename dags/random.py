from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GoogleCloudStorageToLocaLFilesystemOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import random
import csv
# Define your DAG
dag = DAG(
    'push_random_data_to_gcs',
    schedule_interval=None,  # You can specify your schedule interval here
    start_date=days_ago(1),  # Adjust the start date as needed
    catchup=False,
)

# Define your GCS bucket name and file paths
bucket_name = 'random-bucket'
local_file_path = '/c/Users/HP/Documents/Airflow-gcp/airflowrandom_data.csv'
gcs_file_path = 'random_data.csv'

# Create a GCS bucket if it doesn't exist
create_bucket = GCSCreateBucketOperator(
    task_id='create_bucket',
    bucket_name=bucket_name,
    storage_class='STANDARD',
    location='US',  # Specify the location
    project_id='poetic-now-399015',
    dag=dag,
)

# Task to generate and save random data
def generate_random_data(file_path):
    num_rows = 10  # You can adjust the number of rows as needed
    # Create a list to store the random data
    data = []

    # Generate and store random data in the list
    for _ in range(num_rows):
        random_integer = random.randint(1, 100)
        random_float = random.uniform(0.0, 1.0)
        random_element = random.choice(['A', 'B', 'C', 'D', 'E'])

        # Append the random data to the list as a tuple
        data.append((random_integer, random_float, random_element))

    # Write the data to the CSV file
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write the header row
        writer.writerow(["Random Integer", "Random Float", "Random Element"])
        # Write the data rows
        writer.writerows(data)

    return f"Random data saved to {file_path}"

# Generate random data and save it locally
generate_data_task = PythonOperator(
    task_id='generate_data',
    python_callable=generate_random_data,
    op_args=[local_file_path],
    dag=dag,
)

# Transfer the local CSV file to GCS using the GoogleCloudStorageToGoogleCloudStorageOperator
upload_to_gcs = GoogleCloudStorageToLocaLFilesystemOperator(
    task_id='upload_to_gcs',
    source_bucket=bucket_name,
    source_objects=[local_file_path],
    destination_bucket=bucket_name,
    destination_object=gcs_file_path,
    project_id='poetic-now-399015',
    google_cloud_storage_conn_id='google_cloud_default',  # Specify your GCS connection ID here
    mime_type='application/csv',  # Set the appropriate MIME type
    move_object=True,
    dag=dag,
)

# Define the task dependencies
create_bucket >> generate_data_task >> upload_to_gcs
