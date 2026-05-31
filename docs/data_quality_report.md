# Data Quality Report

## Raw Layer

- `tmdb.tmdb_trending`: status=ok, files=1, partition=20260530
- `tmdb.tmdb_popular`: status=ok, files=2, partition=20260530
- `tmdb.tmdb_movie_details`: status=ok, files=47, partition=20260530
- `tmdb.tmdb_external_ids`: status=ok, files=47, partition=20260530
- `omdb.omdb_movie_details`: status=ok, files=47, partition=20260530

## Formatted / Usage Layer

- `formatted.tmdb.tmdb_movie_details`: status=ok, rows=47, partition=20260530
- `formatted.omdb.omdb_movie_ratings`: status=ok, rows=47, partition=20260530
- `usage.ratings_boxoffice_analysis.movie_performance_gap`: status=ok, rows=47, partition=20260530