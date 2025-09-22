import pandera.polars as pa
import polars as pl
from loguru import logger

import data_analysis.utils as utils

raw_data_schema = pa.DataFrameSchema(
    {
        "Date": pa.Column(pa.String, nullable=False),
    }
)


def validate_bronze_df(bronze_df: pl.DataFrame) -> pl.DataFrame:
    """Validates the BRONZE data using the defined schema. For now a fake schema is used, to be improved later.

    :param bronze_df: Polars DataFrame containing the BRONZE data
    :return: Validated Polars DataFrame
    """
    logger.info("Validating raw data with Pandera (polars)...")
    try:
        # Use the `validate` method to check the DataFrame.
        # `lazy=True` ensures all validation errors are collected before raising an exception.
        validated_df = raw_data_schema.validate(bronze_df, lazy=True)
        logger.success("Validation successful! BRONZE data is clean.")
        return validated_df
    except pa.errors.SchemaErrors as err:
        logger.error("Validation failed!")
        logger.error("Schema Errors:")
        logger.error(err.failure_cases)
        raise err


if __name__ == "__main__":
    utils.setup_logging()

    df = pl.read_csv("data/BRONZE/ncr_ride_bookings.csv")
    validate_bronze_df(df)
