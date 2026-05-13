import os
from pathlib import Path
from datetime import datetime, timezone

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")

TMDB_BASE_URL = "https://api.themoviedb.org/3"
OMDB_BASE_URL = "https://www.omdbapi.com/"

PROJECT_ROOT = Path("/opt/airflow")
DATA_ROOT = PROJECT_ROOT / "data"

MAX_TMDB_PAGES = 2
MAX_MOVIES_FOR_DETAILS = 50

def get_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")

def build_path(layer: str, group: str, table: str) -> Path:
    """
    /{layer}/{group}/{TableName}/{date}/
    """
    return DATA_ROOT / layer / group / table / get_date_str()