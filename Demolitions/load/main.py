from dotenv import load_dotenv
import os
from google.cloud import bigquery
import functions_framework

load_dotenv()

@functions_framework.http
def load_demo_data(request):
    dataset_name = os.getenv('DATASET_NAME')
    bucket_name = os.getenv('DATA_LAKE_BUCKET')

    bigquery_client = bigquery.Client()

    prepared_blobname = 'prepared/demolitions/demolitions.jsonl'
    tablename = 'demos'
    table_uri = f'gs://{bucket_name}/{prepared_blobname}'

    # Create table creation query
    create_table_query = '''
    CREATE OR REPLACE TABLE `{0}.{1}` (
        `the_geom` GEOGRAPHY,
        `the_geom_webmercator` STRING,
        `objectid` STRING,
        `addressobjectid` STRING,
        `parcel_id_num` STRING,
        `opa_account_num` STRING,
        `address` STRING,
        `unit_type` STRING,
        `unit_num` STRING,
        `zip` STRING,
        `censustract` STRING,
        `opa_owner` STRING,
        `caseorpermitnumber` STRING,
        `record_type` STRING,
        `typeofwork` STRING,
        `typeofworkdescription` STRING,
        `city_demo` STRING,
        `start_date` STRING,
        `completed_date` STRING,
        `status` STRING,
        `applicanttype` STRING,
        `applicantname` STRING,
        `contractorname` STRING,
        `contractortype` STRING,
        `contractoraddress1` STRING,
        `contractoraddress2` STRING,
        `contractorcity` STRING,
        `contractorstate` STRING,
        `contractorzip` STRING,
        `mostrecentinsp` STRING,
        `systemofrecord` STRING,
        `geocode_x` STRING,
        `geocode_y` STRING,
        `council_district` STRING,
        `posse_jobid` STRING,
        `lat` STRING,
        `lng` STRING
    )
    '''.format(dataset_name, tablename)

    job = bigquery_client.query(create_table_query)
    job.result() 

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    load_job = bigquery_client.load_table_from_uri(
        table_uri,
        f'{dataset_name}.{tablename}',
        job_config=job_config
    )
    load_job.result() 

    if load_job.errors:
        return f'Errors occurred: {load_job.errors}'
    else:
        return f'Successfully loaded {table_uri} into {dataset_name}.{tablename}'

