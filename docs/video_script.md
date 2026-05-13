# Video Script

## 00:00 - 00:45

Introduce the project question: do highly rated movies always perform well commercially?

## 00:45 - 01:30

Show the end-to-end architecture:

- TMDB + OMDb
- Airflow
- raw / formatted / usage / realtime
- Spark
- Kafka
- Elasticsearch
- Kibana

## 01:30 - 02:30

Show raw API files inside `data/raw/`.

## 02:30 - 03:30

Open Airflow and show:

- `movie_batch_pipeline_dag`
- `movie_realtime_pipeline_dag`
- `movie_airbyte_sync_dag`

## 03:30 - 04:30

Show the datalake directory naming convention.

## 04:30 - 05:30

Show Spark parquet outputs in `data/formatted/`.

## 05:30 - 06:30

Show joined results in `data/usage/ratings_boxoffice_analysis/movie_performance_gap/`.

## 06:30 - 07:30

Explain:

- `rating_consensus_score`
- `commercial_score`
- `performance_gap`
- ML revenue expectation

## 07:30 - 08:30

Show Elasticsearch indices and Kibana dashboards.

## 08:30 - 09:15

Show the realtime pipeline:

- Kafka events
- realtime storage
- Kibana auto-refresh

## 09:15 - 09:45

Run:

- `dbt run`
- `dbt test`
- Airbyte DAG trigger

## 09:45 - 10:00

Conclude with:

- key insights
- scoring coverage
- next step for cloud deployment and public sharing
