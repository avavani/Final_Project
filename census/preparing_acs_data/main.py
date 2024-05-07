from dotenv import load_dotenv
import os
import pandas as pd
import pathlib
import functions_framework
import logging
from google.cloud import storage


load_dotenv()


BUCKET_NAME = os.getenv('DATA_LAKE_BUCKET')
DOWNLOAD_PREFIX = 'raw/acs_data/'
UPLOAD_PREFIX = 'prepared/acs_data/'


storage_client = storage.Client()

@functions_framework.http
def prepare_census_data(request):
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Function to download files from GCS
    def download_files(year):
        bucket = storage_client.bucket(BUCKET_NAME)
        file_path = f'{DOWNLOAD_PREFIX}{year}/philly{year}.csv'
        local_path = pathlib.Path(f'/tmp/philly{year}.csv')
        blob = bucket.blob(file_path)
        blob.download_to_filename(str(local_path))
        logging.info(f'Downloaded {file_path} to {local_path}')
        return local_path

    # Function to rename columns
    def rename_columns(df):
        column_mapping = {
            'B02001_001E': 'Total_Population',
            'B02001_002E': 'White_Alone',
            'B02001_003E': 'Black_or_African_American_Alone',
            'B02001_005E': 'Asian_Alone',
            'B07003_003E': 'Moved_Same_County',
            'B07003_007E': 'Moved_Different_County_Same_State',
            'B07003_016E': 'Moved_From_Abroad',
            'B19001_001E': 'Total_Household_Income',
            'B15003_001E': 'Educational_Attainment',
            'B01002_001E': 'Median_Age',
            'B11005_001E': 'Number_of_Children',
            'B11001_001E': 'Family_Type',
            'B12001_001E': 'Marital_Status',
            'B25024_001E': 'Building_Type',
            'B25003_001E': 'Tenure_Status',
            'B25044_001E': 'Number_of_Vehicles',
            'B25018_001E': 'Number_of_Bedrooms',
            'B25009_001E': 'Condominium_Status',
            'B25034_001E': 'Year_Built',
            'B25040_001E': 'Heating_Fuel',
            'B25001_001E': 'Unit_Size',
            'state': 'State',
            'county': 'County',
            'tract': 'Census_Tract'
        }
        df.rename(columns=column_mapping, inplace=True)
        return df

    # Function to save DataFrame to JSONL
    def save_to_jsonl(df, year):
        jsonl_file_path = pathlib.Path(f'/tmp/philly{year}.jsonl')
        df.to_json(str(jsonl_file_path), orient='records', lines=True)
        logging.info(f'Converted CSV to JSONL at {jsonl_file_path}')
        return jsonl_file_path

    # Function to upload JSONL file to GCS
    def upload_to_gcs(jsonl_file_path, year):
        blob_name = f'{UPLOAD_PREFIX}{year}/philly.jsonl'
        blob = storage_client.bucket(BUCKET_NAME).blob(blob_name)
        blob.upload_from_filename(str(jsonl_file_path))
        logging.info(f'Uploaded {jsonl_file_path} to {BUCKET_NAME}/{blob_name}')

    try:
        years = [2021]
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
