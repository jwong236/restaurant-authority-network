import os
import logging
from logging.handlers import RotatingFileHandler

def get_logger(name, filename=None, max_bytes=5*1024*1024, backup_count=10):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    log_dir = "Logs"
    log_file = f"{filename if filename else name}.log"
    full_log_path = os.path.join(log_dir, log_file)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    else:
        if os.path.exists(full_log_path):
            os.remove(full_log_path)

    fh = RotatingFileHandler(
        full_log_path,
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    fh.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    fh.setFormatter(formatter)

    logger.addHandler(fh)
    return logger
