import os

import polars as pl
import utils
from dotenv import load_dotenv
from google.cloud import bigquery
from loguru import logger

utils.setup_logging("INFO")


def read_data_from_file(
    file_path: str,
) -> pl.DataFrame:
    """Reads data from a CSV file

    :param file_path: Path to the CSV file
    :return: DataFrame containing the data
    """

    logger.info(f"Reading data from file: {file_path}")
    df = pl.read_csv(file_path)

    logger.success(f"Data read successfully with {len(df)} records after filtering.")

    return df


def read_data_from_bigquery(
    table_env_var: str = "GCP_BQ_BRONZE_TABLE",
) -> pl.DataFrame:
    """Reads data from a BigQuery table

    :return: Filtered data
    """

    load_dotenv()  # Load environment variables from .env file

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("GCP_BQ_DATASET")
    table_id = os.getenv(table_env_var)
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    if not all([project_id, dataset_id, table_id]):
        logger.error(
            f"Missing one or more required env vars: GCP_PROJECT_ID={project_id}, GCP_BQ_DATASET={dataset_id}, {table_env_var}={table_id}"
        )
        raise ValueError("Missing required environment variables for BigQuery upload.")

    logger.info(f"Reading data from BigQuery table: {full_table_id}")

    client = bigquery.Client(location="europe-west4")

    query = f"""
    SELECT
        *
    FROM (
        SELECT
        *,
        FROM
        `{full_table_id}`
    )
    """

    logger.debug(f"Executing query: {query}")

    df = pl.from_pandas(client.query(query).to_dataframe())

    # Convert all columns to string to avoid dtype issues
    df = df.with_columns([pl.col(col).cast(pl.Utf8) for col in df.columns])

    logger.success(f"Data read successfully with {len(df)} records.")

    return df


def upload_dataframe_to_bigquery(df: pl.DataFrame, table_env_var: str, if_exists: str = "replace"):
    """
    Uploads a DataFrame to a BigQuery table specified by an environment variable.

    :param df: DataFrame to upload
    :param table_env_var: Name of the environment variable containing the table name (e.g., 'GCP_BQ_GOLD_RIDES_TABLE')
    :param if_exists: 'append' (default) or 'replace' for table write disposition
    """
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset_id = os.getenv("GCP_BQ_DATASET")
    table_id = os.getenv(table_env_var)

    if not all([project_id, dataset_id, table_id]):
        logger.error(
            f"Missing one or more required env vars: GCP_PROJECT_ID={project_id}, GCP_BQ_DATASET={dataset_id}, {table_env_var}={table_id}"
        )
        raise ValueError("Missing required environment variables for BigQuery upload.")

    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    logger.info(f"Uploading DataFrame to BigQuery table: {full_table_id}")

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        if if_exists == "append"
        else bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df.to_pandas(), full_table_id, job_config=job_config)
    job.result()
    logger.success(f"Uploaded {df.shape[0]} rows to {full_table_id}.")


if __name__ == "__main__":
    df_from_file = read_data_from_file(
        "data/bronze/ncr_ride_bookings.csv",
    )
    upload_dataframe_to_bigquery(df_from_file, table_env_var="GCP_BQ_BRONZE_TABLE", if_exists="replace")

    df_from_bq = read_data_from_bigquery(table_env_var="GCP_BQ_BRONZE_TABLE")

    if df_from_file.sort(by=df_from_file.columns).equals(df_from_bq.sort(by=df_from_bq.columns)):
        logger.info("Data from file and BigQuery are equal.")
    else:
        logger.warning("Data from file and BigQuery are NOT equal.")
