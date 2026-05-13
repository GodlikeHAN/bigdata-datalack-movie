from __future__ import annotations

from pathlib import Path

from src.config.settings import DATA_ROOT
from src.utils.file_utils import latest_partition_dir, read_json


RAW_TABLES = [
    ("tmdb", "tmdb_trending"),
    ("tmdb", "tmdb_popular"),
    ("tmdb", "tmdb_movie_details"),
    ("tmdb", "tmdb_external_ids"),
    ("omdb", "omdb_movie_details"),
]


def _table_root(group: str, table: str) -> Path:
    return DATA_ROOT / "raw" / group / table


def validate_raw_data(run_date: str | None = None) -> dict:
    table_results: dict[str, dict] = {}

    for group, table in RAW_TABLES:
        root = _table_root(group, table)
        partition = root / run_date if run_date else latest_partition_dir(root)
        files = sorted(partition.glob("*.json")) if partition and partition.exists() else []

        result = {
            "partition": partition.name if partition else None,
            "file_count": len(files),
            "status": "ok" if files else "missing",
        }

        if table == "tmdb_external_ids" and files:
            missing_imdb = 0
            for file in files:
                payload = read_json(file)
                if not payload.get("data", {}).get("imdb_id"):
                    missing_imdb += 1
            result["missing_imdb_ids"] = missing_imdb

        if table == "omdb_movie_details" and files:
            api_errors = 0
            for file in files:
                payload = read_json(file)
                if payload.get("data", {}).get("Response") == "False":
                    api_errors += 1
            result["api_error_count"] = api_errors

        table_results[f"{group}.{table}"] = result

    all_ok = all(item["status"] == "ok" for item in table_results.values())
    return {"status": "ok" if all_ok else "warning", "tables": table_results}
