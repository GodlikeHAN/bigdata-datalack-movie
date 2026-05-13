from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.extract_tmdb import (
    create_raw_directories,
    extract_tmdb_trending,
    extract_tmdb_popular,
    extract_tmdb_movie_details,
    extract_tmdb_external_ids,
)
from src.ingestion.extract_omdb import extract_omdb_movie_details


default_args = {
    "owner": "wendi",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="movie_batch_pipeline_dag",
    description="Batch pipeline for Movie Ratings vs Box Office Performance project",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["big-data", "movies", "tmdb", "omdb"],
) as dag:

    create_directories_task = PythonOperator(
        task_id="create_raw_directories",
        python_callable=create_raw_directories,
    )

    extract_tmdb_trending_task = PythonOperator(
        task_id="extract_tmdb_trending",
        python_callable=extract_tmdb_trending,
    )

    extract_tmdb_popular_task = PythonOperator(
        task_id="extract_tmdb_popular",
        python_callable=extract_tmdb_popular,
    )

    extract_tmdb_movie_details_task = PythonOperator(
        task_id="extract_tmdb_movie_details",
        python_callable=extract_tmdb_movie_details,
    )

    extract_tmdb_external_ids_task = PythonOperator(
        task_id="extract_tmdb_external_ids",
        python_callable=extract_tmdb_external_ids,
    )

    extract_omdb_movie_details_task = PythonOperator(
        task_id="extract_omdb_movie_details",
        python_callable=extract_omdb_movie_details,
    )

    create_directories_task >> extract_tmdb_trending_task

    create_directories_task >> extract_tmdb_popular_task

    extract_tmdb_popular_task >> extract_tmdb_movie_details_task

    extract_tmdb_popular_task >> extract_tmdb_external_ids_task

    extract_tmdb_external_ids_task >> extract_omdb_movie_details_task