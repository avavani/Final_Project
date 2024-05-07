from dotenv import load_dotenv
import os
import pathlib
import pandas as pd
import functions_framework
import logging
from google.cloud import storage

load_dotenv()

# Constants
BUCKET_NAME = os.getenv('DATA_LAKE_BUCKET')
DOWNLOAD_PREFIX = 'raw/acs_data/'
UPLOAD_PREFIX = 'prepared/acs_data/'

storage_client = storage.Client()

@functions_framework.http
def prepare_census_data(request):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def download_files(year):
        bucket = storage_client.bucket(BUCKET_NAME)
        file_path = f'{DOWNLOAD_PREFIX}{year}/philly{year}.csv'
        local_path = pathlib.Path(f'/tmp/philly{year}.csv')
        blob = bucket.blob(file_path)
        blob.download_to_filename(str(local_path))
        logging.info(f'Downloaded {file_path} to {local_path}')
        return local_path

    def save_to_jsonl(df, year):
        jsonl_file_path = pathlib.Path(f'/tmp/philly{year}.jsonl')
        df.to_json(str(jsonl_file_path), orient='records', lines=True)
        logging.info(f'Converted CSV to JSONL at {jsonl_file_path}')
        return jsonl_file_path

    def upload_to_gcs(jsonl_file_path, year):
        blob_name = f'{UPLOAD_PREFIX}{year}/philly.jsonl'
        blob = storage_client.bucket(BUCKET_NAME).blob(blob_name)
        blob.upload_from_filename(str(jsonl_file_path))
        logging.info(f'Uploaded {jsonl_file_path} to {BUCKET_NAME}/{blob_name}')

    try:
        years = [2021, 2016]
        for year in years:
            local_csv_path = download_files(year)
            df = pd.read_csv(local_csv_path)
            df = rename_columns(df)
            jsonl_file_path = save_to_jsonl(df, year)
            upload_to_gcs(jsonl_file_path, year)
        return "Data extraction and upload completed successfully.", 200
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return f"An error occurred: {e}", 500
