import os

from google.cloud import bigquery


def load_data_from_gcs_to_bigquery(event, context):
    """
    Background Cloud Function to load data from GCS to BigQuery.
    Triggered by a Cloud Storage event.

    Args:
        event (dict): The dictionary with data relevant to the event.
                      Contains the bucket and file name.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    gcs_bucket_name = event["bucket"]
    gcs_file_name = event["name"]

    # Construct the URI for the file
    gcs_uri = f"gs://{gcs_bucket_name}/{gcs_file_name}"

    # Get environment variables set in Terraform
    project_id = os.environ.get("GCP_PROJECT")
    dataset_id = os.environ.get("BQ_DATASET")
    table_id = os.environ.get("BQ_TABLE")

    if not all([project_id, dataset_id, table_id]) and not all(
        isinstance(var, str) for var in [project_id, dataset_id, table_id]
    ):
        raise ValueError(
            f"One or more required env vars are missing or not strings: "
            f"PROJECT_ID={project_id}, BQ_DATASET={dataset_id}, BQ_TABLE={table_id}"
        )

    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        # Append new data to the table
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = bigquery_client.load_table_from_uri(
        gcs_uri, table_ref, job_config=job_config
    )

    load_job.result()  # Wait for the job to complete

    print(
        f"Successfully loaded {load_job.output_rows} rows from {gcs_file_name} into {project_id}.{dataset_id}.{table_id}"
    )
