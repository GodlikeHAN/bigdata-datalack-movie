# When Ratings Do Not Match Revenue

## Project Motivation

Movie quality and movie business success are often treated as the same story, but they are not. Some films are critically appreciated and commercially ignored. Others dominate the box office with average ratings. This project studies that gap with a full big data pipeline.

## Data Sources

- TMDB API for popularity, votes, budget, revenue, genres, production metadata, and IMDb IDs
- OMDb API for IMDb, Rotten Tomatoes, Metacritic, and extra box office information

## Data Lake Architecture

The pipeline follows a datalake structure with `raw`, `formatted`, `usage`, and `realtime` layers. Data is stored locally for simplicity and mirrored to LocalStack S3 for the distributed filesystem bonus.

## Airflow Orchestration

Three DAGs are used:

- `movie_batch_pipeline_dag`
- `movie_realtime_pipeline_dag`
- `movie_airbyte_sync_dag`

## Spark Transformation

Spark formatting jobs normalize dates, currencies, runtimes, nested arrays, and null-handling. The formatted layer is written as parquet and becomes the source for downstream analytics.

## Rating and Commercial Scores

The project builds two core metrics:

- `rating_consensus_score`
- `commercial_score`

The difference becomes the `performance_gap`.

## Machine Learning Layer

A regression model predicts expected revenue using budget, runtime, popularity, genre, and external ratings. Comparing expected and actual revenue highlights commercial overperformers and underperformers.

## Elasticsearch and Kibana

The final usage tables are indexed into Elasticsearch and visualized in Kibana using scatter plots, KPI tiles, genre-year views, and realtime trending panels.

## Main Insights

- Hidden Gems: strong rating consensus, weak commercial score
- Blockbuster Paradox: weak rating consensus, strong commercial score
- Commercial Overperformer: revenue far above model expectation
- Commercial Underperformer: revenue far below model expectation

## Difficulties and Improvements

- Missing or delayed IMDb IDs
- OMDb responses with `Response=False`
- Movies with zero or missing financial fields
- Airbyte environment variability

Future improvements could include title-year fuzzy matching, richer features, and cloud deployment with a public read-only Kibana link.
