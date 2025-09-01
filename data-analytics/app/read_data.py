import os

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from loguru import logger


def read_data_from_bigquery(dataset_id, table_id):
    """
    Reads the contents of a file from GCS given its URI.
    Returns the file content as bytes.
    """

    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    try:
        table = bigquery_client.get_table(table_ref)
        rows = bigquery_client.list_rows(table)
        dataframe = rows.to_dataframe()
        logger.info(f"Successfully read {len(dataframe)} rows from BigQuery table.")
        print(dataframe.head())  # Display the first few rows of the dataframe
        return dataframe
    except Exception as e:
        logger.error(f"Error reading data from BigQuery: {e}")
        raise


# --- Example Usage ---
if __name__ == "__main__":
    # Replace with your specific details
    load_dotenv()

    dataset_id = os.getenv("BQ_DATASET")
    table_id = "staging"

    if not all([dataset_id, table_id]) and not all(
        isinstance(var, str) for var in [dataset_id, table_id]
    ):
        raise ValueError(
            f"One or more required env vars are missing or not strings: "
            f"dataset_id={dataset_id}, table_id={table_id}"
        )

    df = read_data_from_bigquery(dataset_id, table_id)
