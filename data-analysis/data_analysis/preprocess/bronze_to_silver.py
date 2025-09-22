import os

import fire
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

from data_analysis.data_handling.data_handler import (
    read_data_from_bigquery,
    read_data_from_file,
    upload_dataframe_to_bigquery,
)
from data_analysis.data_handling.validation import validate_bronze_df
from data_analysis.utils import setup_logging


def run(log_level: str = "INFO", bigquery_upload: bool = False):
    setup_logging(log_level)
    load_dotenv()

    BRONZE_DATA_FILE = os.getenv("BRONZE_DATA_FILE")
    SILVER_DATA_FILE = os.getenv("SILVER_DATA_FILE")

    if not BRONZE_DATA_FILE:
        raise ValueError("BRONZE_DATA_FILE environment variable is not set.")

    if not SILVER_DATA_FILE:
        raise ValueError("SILVER_DATA_FILE environment variable is not set.")

    if bigquery_upload:
        logger.info("Extracting BRONZE data from BigQuery...")
        dataframe = read_data_from_bigquery(table_env_var="GCP_BQ_BRONZE_TABLE")
    else:
        logger.info("Extracting BRONZE data from file...")
        dataframe = read_data_from_file(BRONZE_DATA_FILE)

    logger.success("Successfully extracted BRONZE data from file.")

    logger.info("Validating data")
    validate_bronze_df(dataframe)
    logger.success("Data validation complete")

    logger.info("Transforming data")
    dataframe = transform_to_silver(dataframe)
    logger.success("Data transformation complete")

    logger.info("Data processing complete. Uploading data to SILVER csv...")
    dataframe.to_csv(SILVER_DATA_FILE, index=False)
    logger.success("Data saved to SILVER csv successfully.")

    # Create to bigquery
    if bigquery_upload:
        logger.info("Uploading data to SILVER BigQuery table...")
        upload_dataframe_to_bigquery(dataframe, table_env_var="GCP_BQ_SILVER_TABLE")
        logger.success("Data uploaded to SILVER BigQuery table successfully.")


def transform_to_silver(bronze_df: pd.DataFrame) -> pd.DataFrame:
    # Perform data transformation here

    silver_df = rename_columns(bronze_df)
    silver_df = cast_to_dtypes(silver_df)
    silver_df = extract_temporal_features(silver_df)
    silver_df = cancelled_to_flag(silver_df)
    silver_df = missing_to_flag(silver_df)

    return silver_df


def rename_columns(df):
    """Apply function to lowercase all columns and replace spaces with underscores"""
    new_df = df.copy()

    new_df.columns = new_df.columns.str.lower().str.replace(" ", "_")
    return new_df


def cast_to_dtypes(df):
    """Cast columns to appropriate data types.

    :param df: Input DataFrame
    :return: DataFrame with casted columns
    """
    new_df = df.copy()
    new_df["date"] = pd.to_datetime(new_df["date"], errors="coerce")
    new_df["time"] = pd.to_datetime(new_df["time"], format="%H:%M:%S", errors="coerce").dt.time
    # combine date and time columns to datetime
    new_df["datetime"] = pd.to_datetime(new_df["date"].astype(str) + " " + new_df["time"].astype(str), errors="coerce")

    new_df["booking_id"] = new_df["booking_id"].astype(pd.StringDtype())
    new_df["booking_status"] = new_df["booking_status"].astype("category")
    new_df["customer_id"] = new_df["customer_id"].astype(pd.StringDtype())
    new_df["vehicle_type"] = new_df["vehicle_type"].astype("category")
    new_df["pickup_location"] = new_df["pickup_location"].astype(pd.StringDtype())
    new_df["drop_location"] = new_df["drop_location"].astype("category")
    new_df["avg_vtat"] = pd.to_numeric(new_df["avg_vtat"], errors="coerce")
    new_df["avg_ctat"] = pd.to_numeric(new_df["avg_ctat"], errors="coerce")

    # Add these columns and their types
    new_df["reason_for_cancelling_by_customer"] = new_df["reason_for_cancelling_by_customer"].astype("category")
    new_df["driver_cancellation_reason"] = new_df["driver_cancellation_reason"].astype("category")
    new_df["cancelled_rides_by_driver"] = pd.to_numeric(new_df["cancelled_rides_by_driver"], errors="coerce")
    new_df["cancelled_rides_by_customer"] = pd.to_numeric(new_df["cancelled_rides_by_customer"], errors="coerce")
    new_df["incomplete_rides"] = pd.to_numeric(new_df["incomplete_rides"], errors="coerce")
    new_df["incomplete_rides_reason"] = new_df["incomplete_rides_reason"].astype("category")
    new_df["booking_value"] = pd.to_numeric(new_df["booking_value"], errors="coerce")
    new_df["ride_distance"] = pd.to_numeric(new_df["ride_distance"], errors="coerce")
    new_df["driver_ratings"] = pd.to_numeric(new_df["driver_ratings"], errors="coerce")
    new_df["customer_rating"] = pd.to_numeric(new_df["customer_rating"], errors="coerce")
    new_df["payment_method"] = new_df["payment_method"].astype("category")

    return new_df


def extract_temporal_features(df):
    new_df = df.copy()
    new_df["hour"] = new_df["datetime"].dt.hour
    new_df["day"] = new_df["datetime"].dt.day
    new_df["month"] = new_df["datetime"].dt.month
    new_df["weekday"] = new_df["datetime"].dt.dayofweek  # Monday=0, Sunday=6
    new_df["is_weekend"] = new_df["weekday"].isin([5, 6])

    def segment_time_of_day(hour):
        if 6 <= hour <= 11:
            return "morning"
        elif 12 <= hour <= 16:
            return "afternoon"
        elif 17 <= hour <= 22:
            return "evening"
        else:
            return "night"

    new_df["time_of_day"] = new_df["hour"].apply(lambda h: segment_time_of_day(h) if pd.notnull(h) else None)

    return new_df


def cancelled_to_flag(df):
    new_df = df.copy()

    columns_to_flag = [
        "cancelled_rides_by_driver",
        "cancelled_rides_by_customer",
        "incomplete_rides",
    ]

    for col in columns_to_flag:
        flag_col = f"{col}_flag"
        new_df[flag_col] = new_df[col].notnull()

    return new_df


def missing_to_flag(df):
    new_df = df.copy()

    columns_to_flag = [
        "driver_ratings",
        "customer_rating",
        "booking_value",
        "payment_method",
    ]

    for col in columns_to_flag:
        flag_col = f"{col}_missing_flag"
        new_df[flag_col] = new_df[col].isnull()

    return new_df


if __name__ == "__main__":
    fire.Fire(run)
