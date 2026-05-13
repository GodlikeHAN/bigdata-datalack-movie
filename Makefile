up:
	docker compose up -d --build

run-all:
	docker compose exec airflow-webserver airflow dags trigger movie_batch_pipeline_dag

run-realtime:
	docker compose exec airflow-webserver airflow dags trigger movie_realtime_pipeline_dag

run-airbyte:
	docker compose exec airflow-webserver airflow dags trigger movie_airbyte_sync_dag

dbt:
	docker compose exec airflow-webserver bash -lc "cd /opt/airflow/dbt_movie_project && export DBT_PROFILES_DIR=/opt/airflow/dbt_movie_project && dbt deps && dbt run && dbt test"

down:
	docker compose down -v
