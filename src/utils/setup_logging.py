import logging
import sys
import os
from logging.handlers import RotatingFileHandler


def setup_logging(
    log_filename="pipeline.log",
    log_level=logging.INFO,
    log_to_console=True,
    max_bytes=10 * 1024 * 1024,
    backup_count=5,
):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    log_path = os.path.join(log_dir, log_filename)

    file_handler = RotatingFileHandler(
        log_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )

    handlers = [file_handler]
    if log_to_console:
        handlers.append(logging.StreamHandler(sys.stdout))

    logging.basicConfig(level=log_level, format=log_format, handlers=handlers)

    logging.info("Logging is set up successfully.")
