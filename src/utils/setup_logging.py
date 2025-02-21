import logging
import sys
import os


def setup_logging(
    log_filename="pipeline.log", log_level=logging.INFO, log_to_console=True
):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    handlers = [
        logging.FileHandler(os.path.join(log_dir, log_filename), encoding="utf-8")
    ]
    if log_to_console:
        handlers.append(logging.StreamHandler(sys.stdout))
    logging.basicConfig(level=log_level, format=log_format, handlers=handlers)

    logging.info("Logging is set up successfully.")
