import os
import logging
from logging.handlers import RotatingFileHandler

LOG_DIR = "logs"


def setup_logging(
    name="TravelQuest-ETL",
    filename=None,
    max_bytes=5 * 1024 * 1024,
    backup_count=10,
    console_output=False,  # New parameter to toggle console logging
):
    """Configures logging for the ETL pipeline with rotating file handling."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    log_file = os.path.join(LOG_DIR, f"{filename if filename else name}.log")

    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    fh = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    fh.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fh.setFormatter(formatter)

    logger.addHandler(fh)  # File logging

    if console_output:  # Only add console logging if requested
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    logger.info("Logging initialized.")
    return logger
