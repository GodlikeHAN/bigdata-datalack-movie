# Data Quality Report

## Raw Layer

- `tmdb.tmdb_trending`: status=ok, files=1, partition=20260525
- `tmdb.tmdb_popular`: status=ok, files=2, partition=20260525
- `tmdb.tmdb_movie_details`: status=ok, files=49, partition=20260525
- `tmdb.tmdb_external_ids`: status=ok, files=49, partition=20260525
- `omdb.omdb_movie_details`: status=ok, files=49, partition=20260525

## Formatted / Usage Layer

- `formatted.tmdb.tmdb_movie_details`: status=ok, rows=49, partition=20260525
- `formatted.omdb.omdb_movie_ratings`: status=ok, rows=48, partition=20260525
- `usage.ratings_boxoffice_analysis.movie_performance_gap`: status=ok, rows=49, partition=20260525