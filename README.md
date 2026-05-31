# Movie Ratings vs Box Office Performance

End-to-end Big Data project for analyzing whether movie commercial performance matches machine-learning revenue expectations.

The current implementation uses TMDB and OMDb as REST sources, Airflow for orchestration, Spark for formatting/combination, Spark ML for revenue prediction, and Elasticsearch/Kibana for exposition.

## Current Architecture

```text
TMDB API + OMDb API
        -> Airflow batch DAG
        -> raw JSON
        -> Spark formatting -> formatted parquet
        -> Spark combination -> movie-level usage parquet
        -> Spark ML revenue model
        -> Elasticsearch full refresh per batch run
        -> Kibana

TMDB trending/day
        -> Airflow realtime DAG
        -> Kafka
        -> realtime lake files + Elasticsearch realtime index
```

## Pipeline Rules

- No dbt step in the current version. dbt models will be added later.
- No movie grouping or genre/year aggregate output. The usage layer is one document per movie.
- Stable Elasticsearch `_id` is `document_id = tmdb-{tmdb_id}`.
- Each batch run fully replaces the `movie_performance_gap_v1` Elasticsearch index with that run's final movie dataset.
- TMDB poster and YouTube trailer fields are nullable fallbacks: missing poster or trailer does not fail the pipeline.

## Spark ML Logic

Movie lifecycle:

- `historical`: release date is at least 12 months old
- `active`: release date is less than 12 months old or missing

ML flow:

```text
historical_movies -> train Spark ML RandomForestRegressor
active_movies -> predict predicted_final_revenue
all_movies -> write Elasticsearch
```

Business label:

```text
ml_gap_ratio = (actual_final_revenue - predicted_final_revenue) / predicted_final_revenue
```

- `Commercial Overperformer`: `ml_gap_ratio > 0.20`
- `Commercial Underperformer`: `ml_gap_ratio < -0.20`
- `As Expected`: `-0.20 <= ml_gap_ratio <= 0.20`

## Data Lake Layout

```text
data/
  raw/
    tmdb/
    omdb/
  formatted/
    tmdb/tmdb_movie_details/YYYYMMDD/
    omdb/omdb_movie_ratings/YYYYMMDD/
  usage/
    ratings_boxoffice_analysis/movie_performance_gap/YYYYMMDD/
  realtime/
    movie/tmdb_trending_events/YYYYMMDD/
```

## Main Files

- `dags/movie_batch_pipeline_dag.py`: batch orchestration
- `dags/movie_realtime_pipeline_dag.py`: realtime Kafka orchestration
- `spark_jobs/format_tmdb_movies.py`: TMDB normalization, poster URL, YouTube trailer URL
- `spark_jobs/format_omdb_movies.py`: OMDb rating and box office normalization
- `spark_jobs/combine_ratings_boxoffice.py`: movie-level TMDB/OMDb join
- `spark_jobs/train_ml_revenue_gap.py`: Spark ML revenue prediction and final labels
- `src/indexing/index_to_elastic.py`: movie-level Elasticsearch full refresh indexing

## Run

1. Fill `.env` with `TMDB_API_KEY` and `OMDB_API_KEY`.
2. Start the stack:

```bash
make up
```

3. Trigger the batch DAG:

```bash
make run-all
```

4. Trigger the realtime DAG:

```bash
make run-realtime
```

## Services

- Airflow UI: `http://localhost:8080`
- Elasticsearch: `http://localhost:9200`
- Kibana: `http://localhost:5601`
- Kafka external listener: `localhost:9094`

Default Airflow credentials:

- username: `admin`
- password: `admin`
