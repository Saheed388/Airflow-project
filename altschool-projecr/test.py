import requests
import zipfile
import os
import pandas as pd
from io import BytesIO
import time

# URL of the ZIP file you want to extract
zip_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/2022-10-12/green_tripdata_2023-01.csv.zip"

# Number of retries
max_retries = 3

for attempt in range(1, max_retries + 1):
    try:
        # Send an HTTP GET request to the URL
        response = requests.get(zip_url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Create a BytesIO object from the response content
            zip_data = BytesIO(response.content)

            # Specify the extraction directory
            extraction_path = "/c/Users/HP/Documents/Airflow-gcp/altschool-projecr"  # Change to your desired extraction directory

            try:
                # Create the extraction directory if it doesn't exist
                os.makedirs(extraction_path, exist_ok=True)

                # Open the ZIP file
                with zipfile.ZipFile(zip_data, "r") as zip_file:
                    # Extract all files in the ZIP archive to the extraction directory
                    zip_file.extractall(extraction_path)

                # Get a list of extracted files
                extracted_files = os.listdir(extraction_path)

                # Process each extracted CSV file
                for csv_file_name in extracted_files:
                    if csv_file_name.endswith(".csv"):
                        csv_file_path = os.path.join(extraction_path, csv_file_name)
                        df = pd.read_csv(csv_file_path)

                        # Now you can work with the DataFrame or save it as a new CSV file
                        df.to_csv("//c/Users/HP/Documents/Airflow-gcp/altschool-projecr/green_file.csv", index=False)  # Change the path as needed

                print("Extraction and CSV conversion completed.")
                break  # Successful, exit retry loop
            except Exception as e:
                print(f"Error during extraction: {str(e)}")
        else:
            print(f"Failed to retrieve the ZIP file. Status code: {response.status_code}")

    except requests.exceptions.ConnectionError:
        if attempt < max_retries:
            print(f"Retry attempt {attempt} - Connection error. Retrying in 10 seconds...")
            time.sleep(10)
        else:
            print(f"Max retries reached. Could not establish a connection.")
