import os

import fire
import polars as pl
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
    dataframe.write_csv(SILVER_DATA_FILE)
    logger.success("Data saved to SILVER csv successfully.")

    # Create to bigquery
    if bigquery_upload:
        logger.info("Uploading data to SILVER BigQuery table...")
        upload_dataframe_to_bigquery(dataframe, table_env_var="GCP_BQ_SILVER_TABLE")
        logger.success("Data uploaded to SILVER BigQuery table successfully.")


def transform_to_silver(bronze_df: pl.DataFrame) -> pl.DataFrame:
    # Perform data transformation here

    silver_df = rename_columns(bronze_df)

    silver_df = cast_to_dtypes(silver_df)
    silver_df = extract_temporal_features(silver_df)
    silver_df = cancelled_to_flag(silver_df)
    silver_df = missing_to_flag(silver_df)

    return silver_df


def rename_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Apply function to lowercase all columns and replace spaces with underscores"""
    new_df = df.clone()

    new_df = new_df.rename({col: col.lower().replace(" ", "_") for col in new_df.columns})

    return new_df


def cast_to_dtypes(df: pl.DataFrame) -> pl.DataFrame:
    new_df = df.clone()

    # Parse date column
    new_df = new_df.with_columns([pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False)])
    # Parse time column (as string or pl.Time if format known)
    new_df = new_df.with_columns([pl.col("time").str.strptime(pl.Time, "%H:%M:%S", strict=False)])
    # Combine date and time into datetime
    new_df = new_df.with_columns(
        [
            (pl.col("date").cast(pl.Utf8) + " " + pl.col("time").cast(pl.Utf8))
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
            .alias("datetime")
        ]
    )
    # Cast columns to appropriate types
    new_df = new_df.with_columns(
        [
            pl.col("booking_id").cast(pl.Utf8, strict=False),
            pl.col("booking_status").cast(pl.Categorical, strict=False),
            pl.col("customer_id").cast(pl.Utf8, strict=False),
            pl.col("vehicle_type").cast(pl.Categorical, strict=False),
            pl.col("pickup_location").cast(pl.Utf8, strict=False),
            pl.col("drop_location").cast(pl.Categorical, strict=False),
            pl.col("avg_vtat").cast(pl.Float64, strict=False),
            pl.col("avg_ctat").cast(pl.Float64, strict=False),
            pl.col("reason_for_cancelling_by_customer").cast(pl.Categorical, strict=False),
            pl.col("driver_cancellation_reason").cast(pl.Categorical, strict=False),
            pl.col("cancelled_rides_by_driver").cast(pl.Float64, strict=False),
            pl.col("cancelled_rides_by_customer").cast(pl.Float64, strict=False),
            pl.col("incomplete_rides").cast(pl.Float64, strict=False),
            pl.col("incomplete_rides_reason").cast(pl.Categorical, strict=False),
            pl.col("booking_value").cast(pl.Float64, strict=False),
            pl.col("ride_distance").cast(pl.Float64, strict=False),
            pl.col("driver_ratings").cast(pl.Float64, strict=False),
            pl.col("customer_rating").cast(pl.Float64, strict=False),
            pl.col("payment_method").cast(pl.Categorical, strict=False),
        ]
    )

    return new_df


def extract_temporal_features(df: pl.DataFrame) -> pl.DataFrame:
    new_df = df.clone()

    new_df = new_df.with_columns(
        [
            pl.col("datetime").dt.hour().alias("hour"),
            pl.col("datetime").dt.day().alias("day"),
            pl.col("datetime").dt.month().alias("month"),
            pl.col("datetime").dt.weekday().alias("weekday"),
        ]
    )

    new_df = new_df.with_columns(
        [
            pl.col("weekday").is_in([5, 6]).alias("is_weekend"),
            pl.when(pl.col("hour").is_between(6, 11))
            .then(pl.lit("morning"))
            .when(pl.col("hour").is_between(12, 16))
            .then(pl.lit("afternoon"))
            .when(pl.col("hour").is_between(17, 22))
            .then(pl.lit("evening"))
            .otherwise(pl.lit("night"))
            .alias("time_of_day"),
        ]
    )

    return new_df


def cancelled_to_flag(df: pl.DataFrame) -> pl.DataFrame:
    new_df = df.clone()

    columns_to_flag = [
        "cancelled_rides_by_driver",
        "cancelled_rides_by_customer",
        "incomplete_rides",
    ]

    for col in columns_to_flag:
        flag_col = f"{col}_flag"
        new_df = new_df.with_columns(
            (pl.col(col).is_not_null()).alias(flag_col),
        )

    return new_df


def missing_to_flag(df: pl.DataFrame) -> pl.DataFrame:
    new_df = df.clone()

    columns_to_flag = [
        "driver_ratings",
        "customer_rating",
        "booking_value",
        "payment_method",
    ]

    for col in columns_to_flag:
        flag_col = f"{col}_missing_flag"
        new_df = new_df.with_columns(
            (pl.col(col).is_null()).alias(flag_col),
        )

    return new_df


if __name__ == "__main__":
    fire.Fire(run)
