from pathlib import Path

BASE_URL="https://api.openaq.org/v3"

YEAR=2025

STATION_LIMIT=100


PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATADIR = PROJECT_ROOT / "data"
LOGSDIR = PROJECT_ROOT / "logs"

OUTPUT_FILE = DATADIR / "openaq_2025.csv"

REQUEST_TIMEOUT = 30
MAX_RETRIES = 5

DATADIR.mkdir(exist_ok=True)
LOGSDIR.mkdir(exist_ok=True)
