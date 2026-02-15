import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

DIR = 'logs'

'''
For Configuring any type of Log file
'''
def configure_logging(filename='openaq.log', level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", max_bytes=5*1024*1024, backup_count=5):
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_path = log_dir/filename

    log_formatter = logging.Formatter(
        fmt=format
    )

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    file_handler.setFormatter(log_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)

    logger = logging.getLogger()
    logger.setLevel(level)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger

