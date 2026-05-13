from __future__ import annotations

import pandas as pd

from src.config.settings import DATA_ROOT, GENRE_YEAR_INDEX, MOVIE_PERFORMANCE_INDEX
from src.indexing.bulk_indexer import bulk_index_dataframe
from src.utils.file_utils import latest_partition_dir


def index_usage_to_elasticsearch(run_date: str | None = None) -> dict:
    performance_root = DATA_ROOT / "usage" / "ratings_boxoffice_analysis" / "movie_performance_gap"
    genre_root = DATA_ROOT / "usage" / "ratings_boxoffice_analysis" / "genre_year_performance"

    performance_partition = performance_root / run_date if run_date else latest_partition_dir(performance_root)
    genre_partition = genre_root / run_date if run_date else latest_partition_dir(genre_root)

    if not performance_partition or not performance_partition.exists():
        raise FileNotFoundError("Movie performance usage parquet not found.")
    if not genre_partition or not genre_partition.exists():
        raise FileNotFoundError("Genre year usage parquet not found.")

    performance_df = pd.read_parquet(performance_partition)
    genre_df = pd.read_parquet(genre_partition)
    genre_df = genre_df.copy()
    genre_df["document_id"] = genre_df["release_year"].astype(str) + "-" + genre_df["main_genre"].fillna("unknown")

    movie_docs = bulk_index_dataframe(MOVIE_PERFORMANCE_INDEX, performance_df, id_column="tmdb_id")
    genre_docs = bulk_index_dataframe(GENRE_YEAR_INDEX, genre_df, id_column="document_id")

    return {
        "movie_documents_indexed": movie_docs,
        "genre_year_documents_indexed": genre_docs,
    }
