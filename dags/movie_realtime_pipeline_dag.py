from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from streaming.kafka_movie_consumer import consume_trending_events
from streaming.kafka_tmdb_producer import produce_trending_events


default_args = {
    "owner": "wendi",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id="movie_realtime_pipeline_dag",
    description="Realtime TMDB trending pipeline backed by Kafka and Elasticsearch",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    tags=["big-data", "movies", "realtime", "kafka"],
) as dag:
    start = EmptyOperator(task_id="start")

    produce_events = PythonOperator(
        task_id="produce_tmdb_trending_events",
        python_callable=produce_trending_events,
        op_kwargs={"iterations": 1, "interval_seconds": 0, "emit_heartbeat": True},
    )

    consume_events = PythonOperator(
        task_id="consume_and_index_trending_events",
        python_callable=consume_trending_events,
        op_kwargs={"max_messages": 100, "timeout_ms": 10000},
    )

    end = EmptyOperator(task_id="end")

    start >> produce_events >> consume_events >> end
