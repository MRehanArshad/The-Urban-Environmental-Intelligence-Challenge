from scripts.config_logging import configure_logging
import logging

configure_logging()

logger = logging.getLogger(__name__)

def main():
    logger.info("Application Started")

if __name__ == "__main__":
    main()