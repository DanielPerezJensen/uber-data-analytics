import sys

import loguru


def setup_logging(level: str):
    logger = loguru.logger
    logger.remove()  # Remove default logger

    logger.add(
        sink=sys.stdout,  # Print to stdout
        level=level.upper(),
        colorize=True,
    )

    return logger
