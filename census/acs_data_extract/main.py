from dotenv import load_dotenv
import os
import requests
import pathlib
import functions_framework
import pandas as pd
from google.cloud import storage

Raw_DIR = pathlib.Path(__file__).parent
load_dotenv()

@functions_framework.http
def extract_census_data(request):
    # Extracting the census data
    def extract_census_data(api_key, year):
        base_url = f"https://api.census.gov/data/{year}/acs/acs5"
        params = {
            'get': 'B02001_001E,B02001_002E,B02001_003E,B02001_005E,B07003_003E,B07003_007E,B07003_016E,B19001_001E,B15003_001E,B01002_001E,B11005_001E,B11001_001E,B12001_001E,B25024_001E,B25003_001E,B25044_001E,B25018_001E,B25009_001E,B25034_001E,B25040_001E,B25001_001E',
            'for': 'tract:*',
            'in': 'state: 42 county:101', #For Philly County
            'key': api_key
        }
        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            data = response.json()
            headers = data.pop(0)
            df = pd.DataFrame(data, columns=headers)
            return df
        else:
            raise Exception(f"API Request Failed: {response.status_code} {response.text}")

    # Save DataFrame to CSV
    def save_to_csv(df, filename):
        df.to_csv(filename, index=False)

    # Upload file to Google Cloud Storage
    def upload_to_gcs(blobname, filename):
        bucket_name = os.getenv('DATA_LAKE_BUCKET')
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blobname)
        blob.upload_from_filename(filename)
        print(f'Uploaded {filename} to {bucket_name}')

    
    years = [2021]

    # Extract data for each year
    for year in years:
        try:
            df = extract_census_data(api_key='60e7725695270ae74e6285ce993d2e8d4c222198', year=year)
            filename = Raw_DIR / f'philly{year}.csv'
            save_to_csv(df, filename)
            print(f"Data for {year} saved successfully!")
            blobname = f'raw/acs_data/{year}/philly{year}.csv'
            upload_to_gcs(blobname, filename)
            return "Data extraction and upload completed successfully.", 200
        except Exception as e:
            return f"An error occurred: {str(e)}", 500
        

       git rm --cached <Users/avani/Documents/cloud_comp/final_project/census_blocks/preparing_blockgroup/prepared/blockgroup/blockgroups.jsonl>