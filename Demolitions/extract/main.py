from dotenv import load_dotenv
import os
import requests
from google.cloud import storage
import functions_framework
load_dotenv()

@functions_framework.http
def extract_demo_data(request):
    # Constants
    URL = 'https://phl.carto.com/api/v2/sql?q=SELECT+*,+ST_Y(the_geom)+AS+lat,+ST_X(the_geom)+AS+lng+FROM+demolitions&filename=demolitions&format=csv&skipfields=cartodb_id'
    BUCKET_NAME = os.getenv('DATA_LAKE_BUCKET')
    BLOB_NAME = 'raw/demolitions/demolitions.csv' 

    # Initialize Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    try:
        # Send a request to download the data
        response = requests.get(URL, stream=True)
        response.raise_for_status()  

        # Get a blob object
        blob = bucket.blob(BLOB_NAME)

        # Upload the content directly to Google Cloud Storage
        blob.upload_from_string(response.content)
        print(f'Uploaded data to {BUCKET_NAME}/{BLOB_NAME}')

        return f'Successfully uploaded to {BUCKET_NAME}/{BLOB_NAME}', 200

    except requests.RequestException as e:
        print(f'Failed to download the file: {e}')
        return f'Failed to download the file: {e}', 500
    except Exception as e:
        print(f'An error occurred: {e}')
        return f'An error occurred: {e}', 500
