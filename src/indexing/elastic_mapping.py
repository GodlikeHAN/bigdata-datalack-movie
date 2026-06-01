from src.config.settings import MOVIE_PERFORMANCE_INDEX, REALTIME_TRENDING_INDEX


INDEX_MAPPINGS = {
    MOVIE_PERFORMANCE_INDEX: {
        "mappings": {
            "properties": {
                "document_id": {"type": "keyword"},
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
                "event_id": {"type": "keyword"},
                "event_time_utc": {"type": "date"},
                "source": {"type": "keyword"},
                "event_type": {"type": "keyword"},
                "tmdb_id": {"type": "keyword"},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "poster_path": {"type": "keyword"},
                "poster_url": {"type": "keyword", "index": False},
                "changed_fields": {"type": "keyword"},
                "has_change": {"type": "boolean"},
                "rank": {"type": "integer"},
                "rank_previous_value": {"type": "integer"},
                "rank_previous_changed_utc": {"type": "date"},
                "rank_last_changed_utc": {"type": "date"},
                "rank_change_direction": {"type": "keyword"},
                "popularity": {"type": "float"},
                "popularity_previous_value": {"type": "float"},
                "popularity_previous_changed_utc": {"type": "date"},
                "popularity_last_changed_utc": {"type": "date"},
                "popularity_change_direction": {"type": "keyword"},
                "vote_average": {"type": "float"},
                "vote_average_previous_value": {"type": "float"},
                "vote_average_previous_changed_utc": {"type": "date"},
                "vote_average_last_changed_utc": {"type": "date"},
                "vote_average_change_direction": {"type": "keyword"},
                "vote_count": {"type": "integer"},
                "vote_count_previous_value": {"type": "integer"},
                "vote_count_previous_changed_utc": {"type": "date"},
                "vote_count_last_changed_utc": {"type": "date"},
                "vote_count_change_direction": {"type": "keyword"},
            }
        }
    },
}
