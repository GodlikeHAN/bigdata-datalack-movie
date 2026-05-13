{% set run_date = var('run_date', none) %}
{% if run_date %}
  {% set parquet_path = '/opt/airflow/data/formatted/omdb/omdb_movie_ratings/' ~ run_date ~ '/*.parquet' %}
{% else %}
  {% set parquet_path = '/opt/airflow/data/formatted/omdb/omdb_movie_ratings/*/*.parquet' %}
{% endif %}

select
  imdb_id,
  title as omdb_title,
  year,
  imdb_rating,
  imdb_score_100,
  imdb_votes,
  rt_score_100,
  metacritic_score_100,
  omdb_boxoffice_usd,
  genres as omdb_genres,
  countries as omdb_countries
from read_parquet('{{ parquet_path }}')
