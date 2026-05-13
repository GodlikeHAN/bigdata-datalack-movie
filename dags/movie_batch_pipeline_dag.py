from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.ingestion.extract_tmdb import (
    create_raw_directories,
    extract_tmdb_trending,
    extract_tmdb_popular,
    extract_tmdb_movie_details,
    extract_tmdb_external_ids,
)
from src.indexing.index_to_elastic import index_usage_to_elasticsearch
from src.quality.data_quality_report import generate_data_quality_report
from src.quality.validate_raw import validate_raw_data
from src.utils.dbt_runner import run_dbt_bonus_models
from src.utils.s3_utils import ensure_data_lake_bucket
from src.ingestion.extract_omdb import extract_omdb_movie_details
from spark_jobs.combine_ratings_boxoffice import run as spark_combine_sources
from spark_jobs.compute_kpis import run as spark_compute_kpis
from spark_jobs.format_omdb_movies import run as spark_format_omdb
from spark_jobs.format_tmdb_movies import run as spark_format_tmdb
from spark_jobs.train_ml_revenue_gap import run as spark_train_ml_model

logger = logging.getLogger(__name__)

default_args = {
    "owner": "wendi",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def bootstrap_storage() -> str:
    try:
        ensure_data_lake_bucket()
    except Exception as exc:
        logger.warning("S3 mirror bootstrap skipped: %s", exc)
    create_raw_directories()
    return "Storage initialized"


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

    create_s3_buckets_task = PythonOperator(
        task_id="create_s3_buckets",
        python_callable=bootstrap_storage,
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

    validate_raw_data_task = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data,
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

    spark_compute_kpis_task = PythonOperator(
        task_id="spark_compute_kpis",
        python_callable=spark_compute_kpis,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    spark_train_ml_model_task = PythonOperator(
        task_id="spark_train_ml_model",
        python_callable=spark_train_ml_model,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    dbt_run_bonus_models_task = PythonOperator(
        task_id="dbt_run_bonus_models",
        python_callable=run_dbt_bonus_models,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    index_usage_to_elasticsearch_task = PythonOperator(
        task_id="index_usage_to_elasticsearch",
        python_callable=index_usage_to_elasticsearch,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    generate_data_quality_report_task = PythonOperator(
        task_id="generate_data_quality_report",
        python_callable=generate_data_quality_report,
        op_kwargs={"run_date": "{{ ds_nodash }}"},
    )

    end = EmptyOperator(task_id="end")

    start >> create_s3_buckets_task
    create_s3_buckets_task >> extract_tmdb_trending_task >> extract_tmdb_popular_task >> extract_tmdb_movie_details_task
    extract_tmdb_movie_details_task >> extract_tmdb_external_ids_task >> extract_omdb_movie_details_task
    extract_omdb_movie_details_task >> validate_raw_data_task
    validate_raw_data_task >> spark_format_tmdb_task
    spark_format_tmdb_task >> spark_format_omdb_task >> spark_combine_sources_task
    spark_combine_sources_task >> spark_compute_kpis_task >> spark_train_ml_model_task
    spark_train_ml_model_task >> dbt_run_bonus_models_task >> index_usage_to_elasticsearch_task
    index_usage_to_elasticsearch_task >> generate_data_quality_report_task >> end
