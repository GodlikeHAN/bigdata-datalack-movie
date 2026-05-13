# Movie Ratings vs Box Office Performance

Big Data end-to-end project for the question: do highly rated movies really perform well commercially?

The project ingests TMDB and OMDb data, formats it into a datalake-friendly layout, combines both sources into a `Movie Performance Gap Index`, enriches the result with an ML revenue expectation model, and exposes the output in Elasticsearch/Kibana.

## Architecture

```text
TMDB API + OMDb API
        -> Airflow batch DAG
        -> raw JSON
        -> Spark formatting -> formatted parquet
        -> Spark combination + ML -> usage parquet
        -> Elasticsearch
        -> Kibana

TMDB trending/day
        -> Airflow realtime DAG
        -> Kafka
        -> realtime lake files + Elasticsearch realtime index
        -> Kibana auto-refresh dashboard
```

## Main Deliverables

- `dags/movie_batch_pipeline_dag.py`: full batch orchestration
- `dags/movie_realtime_pipeline_dag.py`: near realtime Kafka pipeline
- `dags/movie_airbyte_sync_dag.py`: Airbyte bonus orchestration
- `spark_jobs/`: formatting, combination, KPI aggregation, ML enrichment
- `src/indexing/`: Elasticsearch bulk indexing
- `dbt_movie_project/`: bonus dbt path using DuckDB
- `kibana/`: Kibana import placeholders and dashboard structure
- `docs/`: blog post draft, video script, data quality report

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
    ratings_boxoffice_analysis/genre_year_performance/YYYYMMDD/
  realtime/
    movie/tmdb_trending_events/YYYYMMDD/
```

## Scoring Coverage

- Ingestion from 2 sources: TMDB + OMDb
- Distributed filesystem bonus: LocalStack S3 mirror enabled through `ENABLE_S3_MIRROR=true`
- Formatting: Spark jobs output parquet
- Field normalization: dates, runtime, box office, ratings, UTC timestamps
- Combination: TMDB + OMDb join with KPI generation
- Machine learning bonus: `train_ml_revenue_gap.py`
- Indexing: Elasticsearch bulk indexing for usage and realtime indices
- Dashboarding: Kibana import files and dashboard plan
- Realtime bonus: Kafka producer/consumer + 1-minute DAG
- DBT bonus: `dbt_movie_project/`
- Run-all bonus: `Makefile`
- Airbyte bonus path: `movie_airbyte_sync_dag.py` plus Airbyte API trigger module

## Run

1. Copy `.env.example` to `.env`.
2. Fill `TMDB_API_KEY` and `OMDB_API_KEY`.
3. Optional: set Airbyte credentials if you want the Airbyte bonus DAG to run against a real instance.
4. Start the stack:

```bash
make up
```

5. Trigger the main batch DAG:

```bash
make run-all
```

6. Trigger the realtime DAG:

```bash
make run-realtime
```

7. Run the dbt bonus path:

```bash
make dbt
```

## Services

- Airflow UI: `http://localhost:8080`
- Elasticsearch: `http://localhost:9200`
- Kibana: `http://localhost:5601`
- LocalStack S3 API: `http://localhost:4566`
- Kafka external listener: `localhost:9094`

Default Airflow credentials:

- username: `admin`
- password: `admin`

## Kibana Dashboards To Build

- Executive Overview
- Ratings vs Revenue
- Performance Gap Analysis
- Genre / Year Analysis
- Realtime Trending

Import base objects from:

- `kibana/index_patterns.ndjson`
- `kibana/dashboard.ndjson`

## Notes

- The batch DAG keeps your current compliant raw folder structure untouched.
- `spark_compute_kpis` runs before `spark_train_ml_model` to preserve your requested step order.
  The ML step then rewrites the enriched usage table and refreshes the KPI aggregate so the final outputs still contain ML-based flags.
- Airbyte cloud/self-hosted setup is environment-specific, so the repo includes the orchestration hook and env contract rather than bundling the full Airbyte OSS stack.
- Cloud deployment and publishing the final blog article remain external actions after the local stack is validated.
