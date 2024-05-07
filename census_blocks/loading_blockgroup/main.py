from dotenv import load_dotenv
import os
from google.cloud import bigquery
import pathlib
import functions_framework

load_dotenv()

# Set up paths
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
RAW_DATA_DIR = SCRIPT_DIR / 'raw' / 'blockgroups'
PREPARED_DATA_DIR = SCRIPT_DIR / 'prepared' / 'blockgroups'

@functions_framework.http
def load_block_group(request):
    dataset_name = os.getenv('DATASET_NAME')
    bigquery_client = bigquery.Client()
    prepared_blobname = 'prepared/blockgroup/blockgroups.jsonl'
    tablename = 'bg'
    table_uri = f'gs://{os.getenv("DATA_LAKE_BUCKET")}/{prepared_blobname}'

    
    create_table_query = f'''
    CREATE OR REPLACE EXTERNAL TABLE {dataset_name}.{tablename} (
        `id` STRING,
        `type` STRING,
        `STATEFP` STRING,
        `COUNTYFP` STRING,
        `TRACTCE` STRING,
        `BLKGRPCE` STRING,
        `GEOID` STRING,
        `GEOIDFQ` STRING,
        `NAMELSAD` STRING,
        `MTFCC` STRING,
        `FUNCSTAT` STRING,
        `ALAND` INT64,
        `AWATER` INT64,
        `INTPTLAT` STRING,
        `INTPTLON` STRING,
        `wkt` STRING
    ) OPTIONS (
        format = 'JSON',
        uris = ['{table_uri}']
    )
    '''

    job = bigquery_client.query(create_table_query)
    job.result() 
    print(f'Loaded {table_uri} into {dataset_name}.{tablename}')

    return f'Loaded {table_uri} into {dataset_name}.{tablename}'