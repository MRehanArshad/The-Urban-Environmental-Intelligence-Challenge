import argparse
import logging
import os
from dotenv import load_dotenv

from pipelines.data_extractor import DataExtractor
from config.config_logging import configure_logging
from pipelines.data_cleaning import clean_data
from pipelines.dimensionality_reduction import reduce
from pipelines.heat_density_analysis import analyze

configure_logging()

logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(
        description="OpenAQ Data Pipeline"
    )

    parser.add_argument(
        '--skip-download',
        action='store_true',
        help="Skip data extraction pipeline"
    )

    return parser.parse_args()

def main():
    args = parse_args()

    load_dotenv()
    api_key = os.getenv("OPENAQ_API_KEY")

    if not api_key:
        raise RuntimeError("OPENAQ_API_KEY not found")

    location_ids = None
    
    if args.skip_download:
        logger.info("Skipping data extraction pipeline (--skip-download used)")
    else:
        extractor = DataExtractor(api_key=api_key)
        extractor.run(location_ids)
    
    df = clean_data()

    wide_df = reduce(df)

    analyze(df)

    logger.info("Main Executed Successfully")

if __name__ == "__main__":
    main()