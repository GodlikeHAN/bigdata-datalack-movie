import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


def _default_project_root() -> Path:
    local_root = Path(__file__).resolve().parents[2]
    airflow_root = Path("/opt/airflow")
    return airflow_root if airflow_root.exists() else local_root


PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", _default_project_root()))
DATA_ROOT = Path(os.getenv("DATA_ROOT", PROJECT_ROOT / "data"))
ARTIFACTS_ROOT = Path(os.getenv("ARTIFACTS_ROOT", PROJECT_ROOT / "artifacts"))
REPORTS_ROOT = Path(os.getenv("REPORTS_ROOT", PROJECT_ROOT / "reports"))
DOCS_ROOT = Path(os.getenv("DOCS_ROOT", PROJECT_ROOT / "docs"))

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
OMDB_API_KEY = os.getenv("OMDB_API_KEY")

TMDB_BASE_URL = "https://api.themoviedb.org/3"
OMDB_BASE_URL = "https://www.omdbapi.com/"

MAX_TMDB_PAGES = int(os.getenv("MAX_TMDB_PAGES", "2"))
MAX_MOVIES_FOR_DETAILS = int(os.getenv("MAX_MOVIES_FOR_DETAILS", "50"))

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
ELASTICSEARCH_USERNAME = os.getenv("ELASTICSEARCH_USERNAME", "")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD", "")
ELASTICSEARCH_VERIFY_CERTS = os.getenv("ELASTICSEARCH_VERIFY_CERTS", "false").lower() == "true"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TRENDING_TOPIC = os.getenv("KAFKA_TRENDING_TOPIC", "movie_trending_events")
KAFKA_TRENDING_CONSUMER_GROUP = os.getenv("KAFKA_TRENDING_CONSUMER_GROUP", "movie_trending_realtime_indexer_v1")
REALTIME_TRENDING_LIMIT = int(os.getenv("REALTIME_TRENDING_LIMIT", "100"))

MOVIE_PERFORMANCE_INDEX = os.getenv("MOVIE_PERFORMANCE_INDEX", "movie_performance_gap_v1")
REALTIME_TRENDING_INDEX = os.getenv("REALTIME_TRENDING_INDEX", "movie_trending_realtime_v1")


def get_date_str(value: Optional[datetime] = None) -> str:
    base_value = value or datetime.now(timezone.utc)
    return base_value.strftime("%Y%m%d")


def build_path(layer: str, group: str, table: str, date_str: Optional[str] = None) -> Path:
    """
    /{layer}/{group}/{TableName}/{date}/
    """
    return DATA_ROOT / layer / group / table / (date_str or get_date_str())
