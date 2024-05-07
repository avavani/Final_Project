from dotenv import load_dotenv
import os
from google.cloud import bigquery
import pathlib
import functions_framework

# Load environment variables from .env file
load_dotenv()

# Set up paths
SCRIPT_DIR = pathlib.Path(__file__).parent
RAW_DATA_DIR = SCRIPT_DIR / 'raw_data'
PREPARED_DATA_DIR = SCRIPT_DIR / 'prepared_data'

# Define function to load census data into BigQuery
@functions_framework.http
def load_census_data(request):

    # Define dataset name
    dataset_name = os.getenv('DATASET_NAME')

    # Create BigQuery client
    bigquery_client = bigquery.Client()

    # Define blob name and table name
    prepared_blobname = f'prepared/acs_data/2021/philly.jsonl'
    tablename = f'acs2021'
    table_uri = f'gs://{os.getenv("DATA_LAKE_BUCKET")}/{prepared_blobname}'

    # Create table creation query
    create_table_query = f'''
        CREATE OR REPLACE EXTERNAL TABLE {dataset_name}.{tablename} (
            `Total_Population` STRING,
            `White_Alone` STRING,
            `Black_or_African_American_Alone` STRING,
            `Asian_Alone` STRING,
            `Moved_Same_County` STRING,
            `Moved_Different_County_Same_State` STRING,
            `Moved_From_Abroad` STRING,
            `Total_Household_Income` STRING,
            `Educational_Attainment` STRING,
            `Median_Age` STRING,
            `Number_of_Children` STRING,
            `Family_Type` STRING,
            `Marital_Status` STRING,
            `Building_Type` STRING,
            `Tenure_Status` STRING,
            `Number_of_Vehicles` STRING,
            `Number_of_Bedrooms` STRING,
            `Condominium_Status` STRING,
            `Year_Built` STRING,
            `Heating_Fuel` STRING,
            `Unit_Size` STRING,
            `State` STRING,
            `County` STRING,
            `Census_Tract` STRING,
            `Block_Group` STRING
        )
        OPTIONS (
          format = 'JSON',
          uris = ['{table_uri}']
        )
        '''

    # Execute table creation query
    job = bigquery_client.query(create_table_query)
    job.result()  # Wait for the job to complete

    print(f'Loaded {table_uri} into {dataset_name}.{tablename}')

    return f'Downloaded to {table_uri}'
