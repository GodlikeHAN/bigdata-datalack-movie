from src.config.settings import MOVIE_PERFORMANCE_INDEX, REALTIME_TRENDING_INDEX


INDEX_MAPPINGS = {
    MOVIE_PERFORMANCE_INDEX: {
        "mappings": {
            "properties": {
                "document_id": {"type": "keyword"},
                "data_hash": {"type": "keyword"},
                "source_data_hash": {"type": "keyword"},
                "tmdb_id": {"type": "keyword"},
                "imdb_id": {"type": "keyword"},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "original_title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "release_date": {"type": "date"},
                "release_year": {"type": "integer"},
                "release_age_months": {"type": "float"},
                "movie_lifecycle": {"type": "keyword"},
                "main_genre": {"type": "keyword"},
                "main_production_country": {"type": "keyword"},
                "rating_consensus_score": {"type": "float"},
                "performance_category": {"type": "keyword"},
                "revenue": {"type": "long"},
                "actual_final_revenue": {"type": "double"},
                "predicted_final_revenue": {"type": "double"},
                "budget": {"type": "long"},
                "roi": {"type": "float"},
                "ml_revenue_gap": {"type": "float"},
                "ml_gap_ratio": {"type": "float"},
                "poster_path": {"type": "keyword"},
                "poster_url": {"type": "keyword", "index": False},
                "youtube_trailer_key": {"type": "keyword"},
                "youtube_trailer_url": {"type": "keyword", "index": False},
                "ingestion_time_utc": {"type": "date"},
            }
        }
    },
    REALTIME_TRENDING_INDEX: {
        "mappings": {
            "properties": {
                "event_time_utc": {"type": "date"},
                "source": {"type": "keyword"},
                "event_type": {"type": "keyword"},
                "tmdb_id": {"type": "keyword"},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "rank": {"type": "integer"},
                "popularity": {"type": "float"},
                "vote_average": {"type": "float"},
                "vote_count": {"type": "integer"},
            }
        }
    },
}
