# Data Quality Report

## Raw Layer

- `tmdb.tmdb_trending`: status=ok, files=1, partition=20260513
- `tmdb.tmdb_popular`: status=ok, files=2, partition=20260513
- `tmdb.tmdb_movie_details`: status=ok, files=49, partition=20260513
- `tmdb.tmdb_external_ids`: status=ok, files=49, partition=20260513
- `omdb.omdb_movie_details`: status=ok, files=48, partition=20260513

## Formatted / Usage Layer

- `formatted.tmdb.tmdb_movie_details`: status=ok, rows=49, partition=20260513
- `formatted.omdb.omdb_movie_ratings`: status=ok, rows=47, partition=20260513
- `usage.ratings_boxoffice_analysis.movie_performance_gap`: status=ok, rows=49, partition=20260513
- `usage.ratings_boxoffice_analysis.genre_year_performance`: status=ok, rows=23, partition=20260513