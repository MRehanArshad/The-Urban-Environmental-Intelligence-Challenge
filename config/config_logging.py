import logging
from logging.handlers import RotatingFileHandler
from config.config import LOGSDIR

'''
For Configuring any type of Log file
'''
def configure_logging(filename='openaq.log', level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", max_bytes=5*1024*1024, backup_count=5):
    log_path = LOGSDIR/filename

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

