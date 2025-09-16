import fire
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

import data_analysis.utils as utils

utils.setup_logging("INFO")


def run(log_level: str = "INFO"):
    logger.info(" Starting silver to gold transformation")
    utils.setup_logging(log_level)
    load_dotenv()


def transform_to_gold(silver_df: pd.DataFrame) -> pd.DataFrame:
    return silver_df


if __name__ == "__main__":
    fire.Fire(run)
