import os
import time

import fire
import numpy as np
import openmeteo_requests
import pandas as pd
import requests_cache
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from loguru import logger
from retry_requests import retry
from tqdm import tqdm

import data_analysis.utils as utils

utils.setup_logging("INFO")


def run(log_level: str = "INFO"):
    logger.info(" Starting silver to gold transformation")
    utils.setup_logging(log_level)
    load_dotenv()

    SILVER_DATA_FILE = os.getenv("SILVER_DATA_FILE")
    GOLD_LOC_FILE = os.getenv("GOLD_LOC_FILE")
    GOLD_WEATHER_FILE = os.getenv("GOLD_WEATHER_FILE")
    GOLD_RIDES_FILE = os.getenv("GOLD_RIDES_FILE")

    if not SILVER_DATA_FILE:
        raise ValueError("SILVER_DATA_FILE environment variable is not set.")
    if not GOLD_LOC_FILE:
        raise ValueError("GOLD_LOC_FILE environment variable is not set.")
    if not GOLD_WEATHER_FILE:
        raise ValueError("GOLD_WEATHER_FILE environment variable is not set.")
    if not GOLD_RIDES_FILE:
        raise ValueError("GOLD_RIDES_FILE environment variable is not set.")

    logger.info("Grabbing silver data from file...")
    silver_df = pd.read_csv(SILVER_DATA_FILE)

    logger.info("Creating location table...")
    if not os.path.exists(GOLD_LOC_FILE):
        loc_df = create_location_table(silver_df)
        loc_df.to_csv(GOLD_LOC_FILE, index=False)
        logger.success(f"Location data uploaded to {GOLD_LOC_FILE} successfully.")
    else:
        loc_df = pd.read_csv(GOLD_LOC_FILE)
        logger.info(f"Location file {GOLD_LOC_FILE} already exists. Skipping creation.")

    logger.info("Creating weather table...")
    if not os.path.exists(GOLD_WEATHER_FILE):
        weather_df = create_weather_table(silver_df, loc_df)
        weather_df.to_csv(GOLD_WEATHER_FILE, index=False)
        logger.success(f"Weather data uploaded to {GOLD_WEATHER_FILE} successfully.")
    else:
        weather_df = pd.read_csv(GOLD_WEATHER_FILE)
        logger.info(f"Weather file {GOLD_WEATHER_FILE} already exists. Skipping creation.")

    logger.info("Creating gold table...")
    gold_df = create_gold_table(silver_df, loc_df, weather_df)
    gold_df.to_csv(GOLD_RIDES_FILE, index=False)
    logger.success(f"Gold data uploaded to {GOLD_RIDES_FILE} successfully.")

    logger.success("Finished silver to gold transformation")


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


def create_location_table(silver_df: pd.DataFrame):
    """Create a location table with unique locations and their corresponding latitude and longitude.

    :param silver_df: DataFrame containing silver data with location information.
    :return: DataFrame containing unique locations and their latitude and longitude.
    """
    unique_locations = list(silver_df["pickup_location"].unique()) + list(silver_df["drop_location"].unique())

    loc_dict = {unique_location: None for unique_location in unique_locations}

    for location in tqdm(loc_dict):
        if loc_dict[location] is None:
            loc_dict[location] = get_loc(location)

    loc_df = (
        pd.DataFrame.from_dict(loc_dict, orient="index", columns=["latitude", "longitude"])
        .reset_index()
        .rename(columns={"index": "location"})
    )

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
                "date": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left",
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
    silver_df: pd.DataFrame,
    loc_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create a weather table by fetching weather data for each location.

    :param silver_df: DataFrame containing silver data with date information.
    :param loc_df: DataFrame containing location information (latitude and longitude).
    :return: DataFrame containing weather data for each location.
    """
    min_date = silver_df["date"].min()
    max_date = silver_df["date"].max()

    weather_dfs = []

    for idx, row in tqdm(loc_df.dropna().iterrows(), total=loc_df.dropna().shape[0]):
        location = row["location"]
        if row["latitude"] == np.nan or row["longitude"] == np.nan:
            print(f"Skipping location {location} due to missing coordinates.")
            continue
        weather_data = get_weather_info(
            long=row["longitude"],
            lat=row["latitude"],
            start_date=min_date,
            end_date=max_date,
        )
        weather_df = pd.DataFrame(weather_data)
        weather_df["location"] = location
        weather_dfs.append(weather_df)

    all_weather_df = pd.concat(weather_dfs, ignore_index=True)

    # Convert date to format 2024-03-23 12:29:38
    all_weather_df["date"] = pd.to_datetime(all_weather_df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    return all_weather_df


def create_gold_table(silver_df: pd.DataFrame, loc_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    """Create a gold table by merging silver data with location and weather data.

    :param silver_df: DataFrame containing silver data.
    :param loc_df: DataFrame containing location information (latitude and longitude).
    :param weather_df: DataFrame containing weather data for each location.
    :return: DataFrame containing the gold table.
    """
    gold_df = silver_df.merge(
        loc_df.rename(
            columns={"location": "pickup_location", "latitude": "pickup_latitude", "longitude": "pickup_longitude"}
        ),
        on="pickup_location",
        how="left",
    ).merge(
        loc_df.rename(
            columns={"location": "drop_location", "latitude": "drop_latitude", "longitude": "drop_longitude"}
        ),
        on="drop_location",
        how="left",
    )

    # Convert datetime columns to pandas datetime
    gold_df["datetime"] = pd.to_datetime(gold_df["datetime"])
    weather_df["date"] = pd.to_datetime(weather_df["date"])

    # Merge for pickup location: find nearest weather record by hour
    pickup_weather = weather_df.rename(
        columns={
            "location": "pickup_location",
            "date": "pickup_weather_date",
            "temperature_2m": "pickup_temperature_2m",
            "rain": "pickup_rain",
            "snowfall": "pickup_snowfall",
            "wind_speed_10m": "pickup_wind_speed_10m",
            "wind_speed_100m": "pickup_wind_speed_100m",
        }
    )
    gold_df = pd.merge_asof(
        gold_df.sort_values("datetime"),
        pickup_weather.sort_values("pickup_weather_date"),
        left_on="datetime",
        right_on="pickup_weather_date",
        by="pickup_location",
        direction="nearest",
        tolerance=pd.Timedelta("1h"),
    )

    # Merge for drop location: find nearest weather record by hour
    drop_weather = weather_df.rename(
        columns={
            "location": "drop_location",
            "date": "drop_weather_date",
            "temperature_2m": "drop_temperature_2m",
            "rain": "drop_rain",
            "snowfall": "drop_snowfall",
            "wind_speed_10m": "drop_wind_speed_10m",
            "wind_speed_100m": "drop_wind_speed_100m",
        }
    )
    gold_df = pd.merge_asof(
        gold_df.sort_values("datetime"),
        drop_weather.sort_values("drop_weather_date"),
        left_on="datetime",
        right_on="drop_weather_date",
        by="drop_location",
        direction="nearest",
        tolerance=pd.Timedelta("1h"),
        suffixes=("", "_drop"),
    )

    gold_df.drop(
        ["pickup_weather_date", "drop_weather_date"],
        axis=1,
        inplace=True,
    )

    return gold_df


if __name__ == "__main__":
    fire.Fire(run)
