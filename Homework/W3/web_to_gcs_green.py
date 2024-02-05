import io
import requests
import os
import pandas as pd
from google.cloud import storage
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# services = ['fhv','green','yellow']
init_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/dts-de-course-2024-dad522388eea.json"

@data_loader
def upload_from_url_to_gcs(url, bucket_name, destination_blob_name):
    # Initialize a Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Initialize a blob (GCS file) in the bucket
    blob = bucket.blob(destination_blob_name)

    # Stream the Parquet file from the URL
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        # Create an in-memory bytes buffer
        buffer = io.BytesIO(response.content)
        # Upload the buffer to GCS
        blob.upload_from_file(buffer, content_type='application/octet-stream')

@data_loader
def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.parquet"

        # download it using requests via a pandas df
        request_url = f"{init_url}/{file_name}"

        # switch out the bucketname
        BUCKET = os.environ.get("GCP_GCS_BUCKET", "trips_data_parquet")
        upload_from_url_to_gcs(request_url, BUCKET, f"parquet/{service}/{file_name}")
    
        # upload it to gcs 
        print(f"GCS: parquet/{service}/{file_name}")

web_to_gcs('2022', 'green')