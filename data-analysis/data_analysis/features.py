import fire
import pandas as pd
from loguru import logger

from data_analysis.dataset import read_data_from_file  # , read_staging_data_from_bigquery
from data_analysis.utils import setup_logging
from data_analysis.validation import validate_staging_data


def run(log_level: str = "INFO"):
    setup_logging(log_level)

    logger.info("Extracting data from file...")
    dataframe = read_data_from_file("data/staging/ncr_ride_bookings.csv")
    logger.success("Successfully extracted data from file.")

    logger.info("Validating data")
    validate_staging_data(dataframe)
    logger.success("Data validation complete")

    logger.info("Transforming data")
    dataframe = transform(dataframe)
    logger.success("Data transformation complete")

    logger.info("Data processing complete. Uploading data to processed csv...")
    dataframe.to_csv("data/processed/processed_data.csv", index=False)
    logger.success("Data uploaded to processed csv successfully.")

    # Create to bigquery and to feature store


def transform(staging_df: pd.DataFrame) -> pd.DataFrame:
    # Perform data transformation here

    processed_df = rename_columns(staging_df)
    processed_df = cast_to_dtypes(processed_df)
    processed_df = extract_temporal_features(processed_df)
    processed_df = cancelled_to_flag(processed_df)
    processed_df = missing_to_flag(processed_df)

    return processed_df


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
