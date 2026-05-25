from __future__ import annotations

import pandas as pd

from src.config.settings import DATA_ROOT, MOVIE_PERFORMANCE_INDEX
from src.indexing.bulk_indexer import bulk_upsert_changed_dataframe
from src.utils.file_utils import latest_partition_dir


def index_usage_to_elasticsearch(run_date: str | None = None) -> dict:
    performance_root = DATA_ROOT / "usage" / "ratings_boxoffice_analysis" / "movie_performance_gap"

    performance_partition = performance_root / run_date if run_date else latest_partition_dir(performance_root)

    if not performance_partition or not performance_partition.exists():
        raise FileNotFoundError("Movie performance usage parquet not found.")

    performance_df = pd.read_parquet(performance_partition)
    movie_result = bulk_upsert_changed_dataframe(
        MOVIE_PERFORMANCE_INDEX,
        performance_df,
        id_column="document_id",
        hash_column="data_hash",
    )

    return {
        "movie_documents_inserted": movie_result["inserted"],
        "movie_documents_updated": movie_result["updated"],
        "movie_documents_skipped": movie_result["skipped"],
    }
