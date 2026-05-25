# When Ratings Do Not Match Revenue

## Project Motivation

Movie quality and movie business success are often treated as the same story, but they are not. Some films are critically appreciated and commercially ignored. Others dominate the box office with average ratings. This project studies that gap with a full big data pipeline.

## Data Sources

- TMDB API for popularity, votes, budget, revenue, genres, production metadata, and IMDb IDs
- OMDb API for IMDb, Rotten Tomatoes, Metacritic, and extra box office information

## Data Lake Architecture

The pipeline follows a datalake structure with `raw`, `formatted`, `usage`, and `realtime` layers. Data is stored locally in the current version; distributed storage will be added later.

## Airflow Orchestration

Three DAGs are used:

- `movie_batch_pipeline_dag`
- `movie_realtime_pipeline_dag`
- `movie_airbyte_sync_dag`

## Spark Transformation

Spark formatting jobs normalize dates, currencies, runtimes, nested arrays, and null-handling. The formatted layer is written as parquet and becomes the source for downstream analytics.

## Rating and Commercial Scores

The project builds a movie-level ML comparison:

- `actual_final_revenue`
- `predicted_final_revenue`
- `ml_gap_ratio`

The output label is generated only from the ML revenue gap.

## Machine Learning Layer

A Spark ML regression model is trained only on historical movies, then predicts final revenue for active movies. Comparing expected and actual revenue highlights commercial overperformers, underperformers, and movies that perform as expected.

## Elasticsearch and Kibana

The final usage tables are indexed into Elasticsearch and visualized in Kibana using scatter plots, KPI tiles, genre-year views, and realtime trending panels.

## Main Insights

- Commercial Overperformer: revenue is more than 20% above model expectation
- Commercial Underperformer: revenue is more than 20% below model expectation
- As Expected: revenue is within plus or minus 20% of model expectation

## Difficulties and Improvements

- Missing or delayed IMDb IDs
- OMDb responses with `Response=False`
- Movies with zero or missing financial fields
- Airbyte environment variability

Future improvements could include title-year fuzzy matching, richer features, and cloud deployment with a public read-only Kibana link.
