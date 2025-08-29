import os

from dotenv import load_dotenv
from google.cloud import bigquery
from loguru import logger


def load_data_from_gcs_to_bigquery(gcs_uri, project_id, dataset_id, table_id):
    """
    Loads a CSV file from GCS into a BigQuery table.
    The table is created if it does not exist.
    """
    # Initialize clients
    bigquery_client = bigquery.Client()

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip the header row
        autodetect=True,  # Automatically detect the schema
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite table
    )

    # Define the destination table
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    logger.info(gcs_uri)

    # Start the load job
    load_job = bigquery_client.load_table_from_uri(
        gcs_uri, table_ref, job_config=job_config
    )

    # Wait for the job to complete
    load_job.result()

    print(
        f"Loaded {load_job.output_rows} rows into {project_id}.{dataset_id}.{table_id}"
    )


# --- Example Usage ---
if __name__ == "__main__":
    # Replace with your specific details
    load_dotenv()

    GCP_PROJECT_ID = os.getenv("PROJECT_ID")
    GCS_BUCKET_NAME = os.getenv("GCS_STORAGE_BUCKET_NAME")
    GCS_FILE_NAME = os.getenv("GCS_STORAGE_BUCKET_FILE_NAME")
    BIGQUERY_DATASET = os.getenv("BQ_DATASET")
    BIGQUERY_TABLE = "staging"

    gcs_file_uri = f"gs://{GCS_BUCKET_NAME}/{GCS_FILE_NAME}"

    load_data_from_gcs_to_bigquery(
        gcs_file_uri, GCP_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLE
    )
