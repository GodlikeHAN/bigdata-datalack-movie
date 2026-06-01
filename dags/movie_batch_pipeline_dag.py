from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.ingestion.extract_tmdb import (
    extract_tmdb_popular,
    extract_tmdb_movie_details,
    extract_tmdb_external_ids,
)
from src.indexing.index_to_elastic import index_usage_to_elasticsearch
from src.ingestion.extract_omdb import extract_omdb_movie_details
from spark_jobs.combine_ratings_boxoffice import run as spark_combine_sources
from spark_jobs.format_omdb_movies import run as spark_format_omdb
from spark_jobs.format_tmdb_movies import run as spark_format_tmdb
from spark_jobs.train_ml_revenue_gap import run as spark_train_ml_model

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
    schedule="@daily",
    catchup=False,
    tags=["big-data", "movies", "tmdb", "omdb"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_tmdb_popular_task = PythonOperator(
        task_id="extract_tmdb_popular",
        python_callable=extract_tmdb_popular,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    extract_tmdb_movie_details_task = PythonOperator(
        task_id="extract_tmdb_movie_details",
        python_callable=extract_tmdb_movie_details,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    extract_tmdb_external_ids_task = PythonOperator(
        task_id="extract_tmdb_external_ids",
        python_callable=extract_tmdb_external_ids,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    extract_omdb_movie_details_task = PythonOperator(
        task_id="extract_omdb_movie_details",
        python_callable=extract_omdb_movie_details,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    spark_format_tmdb_task = PythonOperator(
        task_id="spark_format_tmdb",
        python_callable=spark_format_tmdb,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    spark_format_omdb_task = PythonOperator(
        task_id="spark_format_omdb",
        python_callable=spark_format_omdb,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    spark_combine_sources_task = PythonOperator(
        task_id="spark_combine_sources",
        python_callable=spark_combine_sources,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    spark_train_ml_model_task = PythonOperator(
        task_id="spark_train_ml_model",
        python_callable=spark_train_ml_model,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    index_usage_to_elasticsearch_task = PythonOperator(
        task_id="index_usage_to_elasticsearch",
        python_callable=index_usage_to_elasticsearch,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    end = EmptyOperator(task_id="end")

    start >> extract_tmdb_popular_task >> extract_tmdb_movie_details_task
    extract_tmdb_movie_details_task >> extract_tmdb_external_ids_task >> extract_omdb_movie_details_task
    extract_omdb_movie_details_task >> spark_format_tmdb_task
    spark_format_tmdb_task >> spark_format_omdb_task >> spark_combine_sources_task
    spark_combine_sources_task >> spark_train_ml_model_task >> index_usage_to_elasticsearch_task
    index_usage_to_elasticsearch_task >> end
