{% set run_date = var('run_date', none) %}
{% if run_date %}
  {% set parquet_path = '/opt/airflow/data/formatted/tmdb/tmdb_movie_details/' ~ run_date ~ '/*.parquet' %}
{% else %}
  {% set parquet_path = '/opt/airflow/data/formatted/tmdb/tmdb_movie_details/*/*.parquet' %}
{% endif %}

select
  tmdb_id,
  imdb_id,
  title,
  original_title,
  release_date,
  year(release_date) as release_year,
  genres,
  production_countries,
  runtime,
  budget,
  revenue,
  vote_average as tmdb_vote_average,
  vote_average * 10 as tmdb_score_100,
  vote_count,
  popularity,
  ingestion_time_utc
from read_parquet('{{ parquet_path }}')
