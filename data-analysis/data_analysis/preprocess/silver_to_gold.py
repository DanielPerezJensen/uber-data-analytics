import os
import time
from datetime import datetime

import fire
import openmeteo_requests
import polars as pl
import requests_cache
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from loguru import logger
from retry_requests import retry
from tqdm import tqdm

import data_analysis.utils as utils
from data_analysis.data_handling.data_handler import (
    read_data_from_bigquery,
    read_data_from_file,
    upload_dataframe_to_bigquery,
)

utils.setup_logging("INFO")


def run(log_level: str = "INFO", bigquery_upload: bool = False):
    """Run the silver to gold transformation process.

    :param log_level: Logging level, defaults to "INFO"
    :param bigquery_upload: Flag indicating whether to upload to BigQuery, defaults to False
    """
    logger.info(" Starting silver to gold transformation")
    utils.setup_logging(log_level)
    load_dotenv()

    SILVER_DATA_FILE = os.getenv("SILVER_DATA_FILE")
    GOLD_LOCATIONS_FILE = os.getenv("GOLD_LOCATIONS_FILE")
    GOLD_WEATHER_FILE = os.getenv("GOLD_WEATHER_FILE")
    GOLD_RIDES_FILE = os.getenv("GOLD_RIDES_FILE")

    if not SILVER_DATA_FILE:
        raise ValueError("SILVER_DATA_FILE environment variable is not set.")
    if not GOLD_LOCATIONS_FILE:
        raise ValueError("GOLD_LOCATIONS_FILE environment variable is not set.")
    if not GOLD_WEATHER_FILE:
        raise ValueError("GOLD_WEATHER_FILE environment variable is not set.")
    if not GOLD_RIDES_FILE:
        raise ValueError("GOLD_RIDES_FILE environment variable is not set.")

    logger.info("Grabbing silver data from file...")

    if bigquery_upload:
        logger.info("Extracting SILVER data from BigQuery...")
        silver_df = read_data_from_bigquery(table_env_var="GCP_BQ_SILVER_TABLE")
    else:
        silver_df = read_data_from_file(SILVER_DATA_FILE)

    # For local runs this is necesarry, future work could think about how to grab from cloud
    if os.path.exists(GOLD_LOCATIONS_FILE):
        loc_df = pl.read_csv(GOLD_LOCATIONS_FILE)
    else:
        loc_df = create_location_table(silver_df)
        store_or_upload(loc_df, GOLD_LOCATIONS_FILE, bigquery_upload, "GCP_BQ_GOLD_LOCATIONS_TABLE", "Location data")

    if os.path.exists(GOLD_WEATHER_FILE):
        weather_df = pl.read_csv(GOLD_WEATHER_FILE)
    else:
        weather_df = create_weather_table(silver_df, loc_df)
        store_or_upload(weather_df, GOLD_WEATHER_FILE, bigquery_upload, "GCP_BQ_GOLD_WEATHER_TABLE", "Weather data")

    logger.info("Creating gold table...")
    gold_rides_df = create_gold_table(silver_df, loc_df, weather_df)
    store_or_upload(gold_rides_df, GOLD_RIDES_FILE, bigquery_upload, "GCP_BQ_GOLD_RIDES_TABLE", "Gold data")

    logger.success("Finished silver to gold transformation")


def store_or_upload(
    df: pl.DataFrame, local_path: str, bigquery_flag: bool, table_env_var: str, table_name: str
) -> None:
    """Stores or uploads a DataFrame to a specified location.

    :param df: DataFrame to be stored or uploaded
    :param local_path: Local file path to save the DataFrame if not uploading
    :param bigquery_flag: Flag indicating whether to upload to BigQuery
    :param table_env_var: Environment variable for the BigQuery table
    :param table_name: Name of the table being processed
    """
    if bigquery_flag:
        logger.info(f"Uploading {table_name} to BigQuery...")
        upload_dataframe_to_bigquery(df, table_env_var=table_env_var, if_exists="replace")
        logger.success(f"{table_name} uploaded to BigQuery successfully.")
    else:
        df.write_csv(local_path)
        logger.success(f"{table_name} saved to {local_path} successfully.")


def get_loc(location_string: str, max_retries: int = 3, delay: float = 1.5):
    """Gets the latitude and longitude of a location string using the Nominatim geocoding service.
    Retries the request up to max_retries times in case of failure, with a delay between attempts.

    :param location_string: The location string to geocode.
    :param max_retries: The maximum number of retry attempts, defaults to 3.
    :param delay: The delay between retry attempts in seconds, defaults to 1.5.
    :return: A tuple containing the latitude and longitude, or (None, None) if not found.
    """
    loc = Nominatim(user_agent="GetLoc", timeout=10)

    for attempt in range(max_retries):
        try:
            get_location = loc.geocode(location_string)
            if get_location is None:
                return None, None
            return get_location.latitude, get_location.longitude

        except Exception as e:
            print(f"Attempt {attempt + 1} failed for '{location_string}': {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                return None, None


def create_location_table(silver_df: pl.DataFrame):
    """Create a location table with unique locations and their corresponding latitude and longitude.

    :param silver_df: DataFrame containing silver data with location information.
    :return: DataFrame containing unique locations and their latitude and longitude.
    """
    pickup_locations = silver_df["pickup_location"]
    drop_locations = silver_df["drop_location"]
    unique_locations = set(pickup_locations) | set(drop_locations)

    data = [
        {"location": loc, "latitude": get_loc(loc)[0], "longitude": get_loc(loc)[1]}
        for loc in tqdm(unique_locations, desc="Processing locations")
    ]

    loc_df = pl.DataFrame(data)

    return loc_df


def get_weather_info(long, lat, start_date="2024-01-01", end_date="2024-12-30", max_retries=3, delay=60):
    """Fetches weather information from the Open-Meteo API for a given latitude and longitude
    over a specified date range. Retries the request up to max_retries times in case of failure,
    with a delay between attempts.

    :param long: Longitude of the location.
    :param lat: Latitude of the location.
    :param start_date: Start date for the weather data, defaults to "2024-01-01".
    :param end_date: End date for the weather data, defaults to "2024-12-30".
    :param max_retries: Maximum number of retry attempts, defaults to 3.
    :param delay: Delay between retry attempts in seconds, defaults to 60.
    :return: Weather data for the specified location and date range, or None if not found.
    """
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": long,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ["temperature_2m", "rain", "snowfall", "wind_speed_10m", "wind_speed_100m"],
    }

    for attempt in range(max_retries):
        try:
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]

            hourly = response.Hourly()

            hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
            hourly_rain = hourly.Variables(1).ValuesAsNumpy()
            hourly_snowfall = hourly.Variables(2).ValuesAsNumpy()
            hourly_wind_speed_10m = hourly.Variables(3).ValuesAsNumpy()
            hourly_wind_speed_100m = hourly.Variables(4).ValuesAsNumpy()

            hourly_data = {
                "date": pl.datetime_range(
                    start=datetime.fromtimestamp(hourly.Time()),
                    end=datetime.fromtimestamp(hourly.TimeEnd()),
                    interval="1h",
                    closed="left",
                    eager=True,
                )
            }

            hourly_data["temperature_2m"] = hourly_temperature_2m
            hourly_data["rain"] = hourly_rain
            hourly_data["snowfall"] = hourly_snowfall
            hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
            hourly_data["wind_speed_100m"] = hourly_wind_speed_100m

            return hourly_data

        except Exception as e:
            print(f"Attempt {attempt + 1} failed for weather API at ({lat}, {long}): {e}")
            if attempt < max_retries - 1:
                print("Waiting 1 minute before retrying...")
                time.sleep(delay)
            else:
                print("All retries failed.")
                return None


def create_weather_table(
    silver_df: pl.DataFrame,
    loc_df: pl.DataFrame,
) -> pl.DataFrame:
    """Create a weather table by fetching weather data for each location."""

    min_date = silver_df["date"].min()
    max_date = silver_df["date"].max()

    weather_dfs = []

    for row in tqdm(
        loc_df.drop_nulls().iter_rows(named=True), total=loc_df.drop_nulls().height, desc="Processing weather"
    ):
        location = row["location"]
        if row["latitude"] is None or row["longitude"] is None:
            print(f"Skipping location {location} due to missing coordinates.")
            continue
        weather_data = get_weather_info(
            long=row["longitude"],
            lat=row["latitude"],
            start_date=min_date,
            end_date=max_date,
        )
        weather_df = pl.DataFrame(weather_data)
        weather_df = weather_df.with_columns(pl.lit(location).alias("location"))
        weather_dfs.append(weather_df)

    all_weather_df = pl.concat(weather_dfs)

    return all_weather_df


def ensure_string_column(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Ensure a column is of type String, casting if necessary."""
    if df.schema.get(col) != pl.String:
        df = df.with_columns(pl.col(col).cast(pl.String).alias(col))
    return df


def create_gold_table(silver_df: pl.DataFrame, loc_df: pl.DataFrame, weather_df: pl.DataFrame) -> pl.DataFrame:
    """Create a gold table by merging silver data with location and weather data.

    :param silver_df: DataFrame containing silver data.
    :param loc_df: DataFrame containing location information (latitude and longitude).
    :param weather_df: DataFrame containing weather data for each location.
    :return: DataFrame containing the gold table.
    """
    # Rename columns for pickup join
    pickup_loc_df = loc_df.rename(
        {"location": "pickup_location", "latitude": "pickup_latitude", "longitude": "pickup_longitude"}
    )

    # Join on pickup_location
    gold_df = silver_df.join(pickup_loc_df, left_on="pickup_location", right_on="pickup_location", how="left")

    # Rename columns for drop join
    drop_loc_df = loc_df.rename(
        {"location": "drop_location", "latitude": "drop_latitude", "longitude": "drop_longitude"}
    )

    # Join on drop_location
    gold_df = gold_df.join(drop_loc_df, left_on="drop_location", right_on="drop_location", how="left")

    # Ensure datetime columns are parsed
    gold_df = gold_df.with_columns(
        pl.concat_str([pl.col("date"), pl.col("time")], separator=" ").str.strptime(pl.Datetime).alias("datetime")
    )

    # Merge for pickup location: find nearest weather record by hour
    pickup_weather = weather_df.rename(
        {
            "location": "pickup_location",
            "date": "pickup_weather_date",
            "temperature_2m": "pickup_temperature_2m",
            "rain": "pickup_rain",
            "snowfall": "pickup_snowfall",
            "wind_speed_10m": "pickup_wind_speed_10m",
            "wind_speed_100m": "pickup_wind_speed_100m",
        }
    )
    pickup_weather = ensure_string_column(pickup_weather, "pickup_weather_date")
    pickup_weather = pickup_weather.with_columns(pl.col("pickup_weather_date").str.strptime(pl.Datetime))

    gold_df = gold_df.sort(by="datetime").join_asof(
        pickup_weather.sort(by="pickup_weather_date"),
        left_on="datetime",
        right_on="pickup_weather_date",
        by="pickup_location",
        strategy="nearest",
        tolerance="1h",
        suffix="_pickup",
    )

    # Merge for drop location: find nearest weather record by hour
    drop_weather = weather_df.rename(
        {
            "location": "drop_location",
            "date": "drop_weather_date",
            "temperature_2m": "drop_temperature_2m",
            "rain": "drop_rain",
            "snowfall": "drop_snowfall",
            "wind_speed_10m": "drop_wind_speed_10m",
            "wind_speed_100m": "drop_wind_speed_100m",
        }
    )
    drop_weather = ensure_string_column(drop_weather, "drop_weather_date")
    drop_weather = drop_weather.with_columns(pl.col("drop_weather_date").str.strptime(pl.Datetime))

    gold_df = gold_df.sort("datetime").join_asof(
        drop_weather.sort(by="drop_weather_date"),
        left_on="datetime",
        right_on="drop_weather_date",
        by="drop_location",
        strategy="nearest",
        tolerance="1h",
        suffix="_drop",
    )

    gold_df = gold_df.drop(
        ["pickup_weather_date", "drop_weather_date"],
    )

    logger.info(f"Gold DataFrame created with {gold_df.height} rows and {gold_df.width} columns.")

    return gold_df


if __name__ == "__main__":
    fire.Fire(run)
