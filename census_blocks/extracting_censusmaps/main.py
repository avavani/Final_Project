from dotenv import load_dotenv
import os
import requests
import pathlib
import functions_framework
import pandas as pd
import io
import zipfile
from google.cloud import storage

Raw_DIR = pathlib.Path(__file__).parent


load_dotenv()

# Constants
URL = 'https://www2.census.gov/geo/tiger/TIGER2023/BG/tl_2023_06_bg.zip'
BUCKET_NAME = os.getenv('DATA_LAKE_BUCKET')
BASE_DIR = pathlib.Path(__file__).resolve().parent
EXTRACT_PATH = BASE_DIR / 'raw' / 'blockgroup'

# Initialize the Google Cloud Storage client
storage_client = storage.Client()

@functions_framework.http
def extract_block_group(request):
    try:
        
        response = requests.get(URL)
        response.raise_for_status()  
        print(f'Received {response.status_code} response...')

        # Handling the zip file in memory
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            zip_file.extractall(EXTRACT_PATH)
            print(f'Extracted all files into {EXTRACT_PATH}...')

        # Upload files to Google Cloud Storage
        bucket = storage_client.bucket(BUCKET_NAME)
        for file_path in EXTRACT_PATH.iterdir():
            if file_path.is_file():  
                blob_name = f'raw/blockgroup/{file_path.name}'
                blob = bucket.blob(blob_name)
                blob.upload_from_filename(str(file_path))
                print(f'Uploaded {file_path} to {BUCKET_NAME}/{blob_name}')

    except requests.RequestException as e:
        print(f'Failed to download or process the file: {e}')
    except Exception as e:
        print(f'An error occurred: {e}')

    return "Process completed", 200