import pandas as pd
import pandera.pandas as pa
import utils
from loguru import logger

raw_data_schema = pa.DataFrameSchema(
    {
        "Date": pa.Column(pa.String, nullable=False),
    }
)


def validate_staging_data(staging_df: pd.DataFrame) -> pd.DataFrame:
    """Validates the staging data using the defined schema. For now a fake schema is used, to be improved later.

    :param staging_df: DataFrame containing the staging data
    :return: Validated DataFrame
    """
    logger.info("Validating raw data with Pandera...")
    try:
        # Use the `validate` method to check the DataFrame.
        # `lazy=True` ensures all validation errors are collected before raising an exception.
        validated_df = raw_data_schema.validate(staging_df, lazy=True)
        logger.success("Validation successful! Staging data is clean.")
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

    df = pd.read_csv("data/staging/ncr_ride_bookings.csv")

    validate_staging_data(df)
