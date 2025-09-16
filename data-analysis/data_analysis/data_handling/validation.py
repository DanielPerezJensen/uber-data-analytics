import pandas as pd
import pandera.pandas as pa
from loguru import logger

import data_analysis.utils as utils

raw_data_schema = pa.DataFrameSchema(
    {
        "Date": pa.Column(pa.String, nullable=False),
    }
)


def validate_bronze_df(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """Validates the BRONZE data using the defined schema. For now a fake schema is used, to be improved later.

    :param bronze: DataFrame containing the BRONZE data
    :return: Validated DataFrame
    """
    logger.info("Validating raw data with Pandera...")
    try:
        # Use the `validate` method to check the DataFrame.
        # `lazy=True` ensures all validation errors are collected before raising an exception.
        validated_df = raw_data_schema.validate(bronze_df, lazy=True)
        logger.success("Validation successful! BRONZE data is clean.")
        return validated_df
    except pa.errors.SchemaErrors as err:
        logger.error("Validation failed!")
        # The `failure_cases` and `check` properties of the exception
        # provide detailed information about what went wrong.
        logger.error("Schema Errors:")
        logger.error(err.failure_cases)
        raise err


if __name__ == "__main__":
    utils.setup_logging()

    df = pd.read_csv("data/BRONZE/ncr_ride_bookings.csv")

    validate_bronze_df(df)
