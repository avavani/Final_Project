from dotenv import load_dotenv
import os
import csv
import json
from google.cloud import storage
import functions_framework

load_dotenv()

@functions_framework.http
def prepare_demo_data(request):
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()
    bucket_name = os.getenv('DATA_LAKE_BUCKET')
    bucket = storage_client.bucket(bucket_name)

    # File paths in GCS
    csv_blob_name = 'raw/demolitions/demolitions.csv'
    jsonl_blob_name = 'prepared/demolitions/demolitions.jsonl'

    # Download CSV file from GCS
    csv_blob = bucket.blob(csv_blob_name)
    csv_content = csv_blob.download_as_text()

    # Convert CSV content to JSONL format
    csv_reader = csv.DictReader(csv_content.splitlines())
    jsonl_string = ""
    for row in csv_reader:
        json_string = json.dumps(row)
        jsonl_string += json_string + '\n'

    # Upload JSONL to Google Cloud Storage
    jsonl_blob = bucket.blob(jsonl_blob_name)
    jsonl_blob.upload_from_string(jsonl_string)

    return f"Successfully converted and uploaded JSONL to {bucket_name}/{jsonl_blob_name}", 200

