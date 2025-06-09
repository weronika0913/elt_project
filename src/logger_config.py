"""
This module configures a logger for recording application activity.
"""

import logging
import os


def setup_logger(name):
    """
    Set up and return a logger with the given name.

    The logger writes INFO level and above messages to a log file
    located in the 'logs' directory under 'app.log'.
    Log messages include timestamp, logger name, level, and message.

    Args:
        name (str): The name of the logger.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logging.basicConfig(
        level=logging.INFO,
        filename=os.path.join("logs", "app.log"),
        encoding="utf-8",
        filemode="a",
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    return logger
