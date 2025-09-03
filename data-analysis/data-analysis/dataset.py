import datetime
import os
from typing import Optional

import pandas as pd
import utils
from dotenv import load_dotenv
from google.cloud import bigquery
from loguru import logger

utils.setup_logging("INFO")


def read_data_from_file(
    file_path: str,
    start_datetime: Optional[datetime.datetime] = None,
    end_datetime: Optional[datetime.datetime] = None,
    datetime_format: str = "%Y-%m-%d %H:%M:%S",
):
    """Reads data from a CSV file and filters it based on the provided time range.

    :param file_path: Path to the CSV file
    :param start_time: start_time, defaults to None
    :param end_time: end_time, defaults to None
    :return: Filtered data
    """

    logger.info(f"Reading data from file: {file_path}")
    df = pd.read_csv(file_path)

    df["datetime"] = pd.to_datetime(df["Date"] + " " + df["Time"], format=datetime_format)

    if start_datetime:
        df = df[df["datetime"] >= start_datetime]
    if end_datetime:
        df = df[df["datetime"] <= end_datetime]

    df.drop(columns=["datetime"], inplace=True)

    logger.success(f"Data read successfully with {len(df)} records after filtering.")

    return df


def read_raw_data_from_bigquery(
    start_datetime: Optional[datetime.datetime] = None,
    end_datetime: Optional[datetime.datetime] = None,
):
    """Reads data from a BigQuery table and filters it based on the provided time range.

    :param start_datetime: Start datetime for filtering, defaults to None
    :param end_datetime: End datetime for filtering, defaults to None
    :return: Filtered data
    """

    load_dotenv()  # Load environment variables from .env file

    bigquery_staging_data_table = os.getenv("BQ_STAGING_DATA_TABLE")

    if not bigquery_staging_data_table:
        logger.error("Environment variable BQ_STAGING_DATA_TABLE is not set.")
        raise ValueError("Environment variable BQ_STAGING_DATA_TABLE is not set.")

    logger.info(f"Reading data from BigQuery table: {bigquery_staging_data_table}")

    client = bigquery.Client()

    query = f"""
    SELECT
        *
    FROM (
        SELECT
        *,
        PARSE_DATETIME('%Y-%m-%d %H:%M:%S', CONCAT(Date, ' ', Time)) AS datetime
        FROM
        `{bigquery_staging_data_table}`
    )
    WHERE
        (datetime >= @start_datetime OR @start_datetime IS NULL)
        AND (datetime <= @end_datetime OR @end_datetime IS NULL)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_datetime", "DATETIME", start_datetime),
            bigquery.ScalarQueryParameter("end_datetime", "DATETIME", end_datetime),
        ]
    )

    logger.debug(f"Executing query: {query}")

    df = client.query(query, job_config=job_config).to_dataframe()

    logger.success(f"Data read successfully with {len(df)} records after filtering.")

    return df


if __name__ == "__main__":
    df = read_data_from_file(
        "data/raw/ncr_ride_bookings.csv",
    )

    df = read_raw_data_from_bigquery()
