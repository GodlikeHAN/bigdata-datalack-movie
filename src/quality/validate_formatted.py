from __future__ import annotations

import pandas as pd

from src.config.settings import DATA_ROOT
from src.utils.file_utils import latest_partition_dir


FORMATTED_TABLES = [
    ("formatted", "tmdb", "tmdb_movie_details"),
    ("formatted", "omdb", "omdb_movie_ratings"),
    ("usage", "ratings_boxoffice_analysis", "movie_performance_gap"),
]


def _read_parquet_metrics(path) -> dict:
    dataframe = pd.read_parquet(path)
    return {
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "null_counts": dataframe.isna().sum().to_dict(),
    }


def validate_formatted_data(run_date: str | None = None) -> dict:
    results: dict[str, dict] = {}

    for layer, group, table in FORMATTED_TABLES:
        root = DATA_ROOT / layer / group / table
        partition = root / run_date if run_date else latest_partition_dir(root)
        key = f"{layer}.{group}.{table}"

        if not partition or not partition.exists():
            results[key] = {"status": "missing", "partition": None}
            continue

        try:
            metrics = _read_parquet_metrics(partition)
            metrics["status"] = "ok"
            metrics["partition"] = partition.name
            results[key] = metrics
        except Exception as exc:
            results[key] = {
                "status": "error",
                "partition": partition.name,
                "error": str(exc),
            }

    return {"status": "ok", "tables": results}
