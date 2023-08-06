import os

from laktory._logger import get_logger


def test_get_logger():
    # Get logger
    logger = get_logger(__name__)

    # Log info
    logger.info("This is an info log")


if __name__ == "__main__":
    test_get_logger()
