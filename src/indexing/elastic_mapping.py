from src.config.settings import GENRE_YEAR_INDEX, MOVIE_PERFORMANCE_INDEX, REALTIME_TRENDING_INDEX


INDEX_MAPPINGS = {
    MOVIE_PERFORMANCE_INDEX: {
        "mappings": {
            "properties": {
                "tmdb_id": {"type": "keyword"},
                "imdb_id": {"type": "keyword"},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "release_date": {"type": "date"},
                "release_year": {"type": "integer"},
                "main_genre": {"type": "keyword"},
                "rating_consensus_score": {"type": "float"},
                "commercial_score": {"type": "float"},
                "performance_gap": {"type": "float"},
                "performance_category": {"type": "keyword"},
                "revenue": {"type": "long"},
                "budget": {"type": "long"},
                "roi": {"type": "float"},
                "ml_expected_revenue": {"type": "float"},
                "ml_revenue_gap": {"type": "float"},
                "ingestion_time_utc": {"type": "date"},
            }
        }
    },
    GENRE_YEAR_INDEX: {
        "mappings": {
            "properties": {
                "release_year": {"type": "integer"},
                "main_genre": {"type": "keyword"},
                "movie_count": {"type": "integer"},
                "avg_rating_consensus_score": {"type": "float"},
                "avg_revenue": {"type": "float"},
                "avg_roi": {"type": "float"},
                "avg_performance_gap": {"type": "float"},
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
