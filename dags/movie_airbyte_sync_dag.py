from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.ingestion.extract_airbyte import trigger_airbyte_sync


default_args = {
    "owner": "wendi",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="movie_airbyte_sync_dag",
    description="Trigger the Airbyte TMDB/OMDb sync bonus pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["big-data", "movies", "airbyte"],
) as dag:
    start = EmptyOperator(task_id="start")

    trigger_sync = PythonOperator(
        task_id="trigger_airbyte_sync",
        python_callable=trigger_airbyte_sync,
    )

    end = EmptyOperator(task_id="end")

    start >> trigger_sync >> end
