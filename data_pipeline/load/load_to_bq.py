# data_pipeline/load/load_to_bq.py

import os
from xmlrpc import client
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

def run(ndjson_file_path: str):
    
    """
    Uploads NDJSON file to BigQuery using a single insert job.
    Assumes table and dataset already exist.
    """
    project_id = os.getenv("PROJECT_ID")
    dataset_id = os.getenv("DATASET_NAME")
    table_id = os.getenv("TABLE_NAME")
    print("File path:",ndjson_file_path)
    if not all([project_id, dataset_id, table_id]):
        raise ValueError("Missing required BigQuery environment variables.")

    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
   
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    with open(ndjson_file_path, "rb") as source_file:
        print("'File",source_file)
        load_job = client.load_table_from_file(
            source_file,
            table_ref,
            job_config=job_config
        )
    print(f"🔄 Starting load job for {ndjson_file_path} to {dataset_id}.{table_id}")
    load_job.result()  # Wait for completion
    
    print(f"✅ Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}")
