DBT = docker compose exec airflow-worker dbt
DBT_ARGS = --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

dbt-build:
	$(DBT) build $(DBT_ARGS)

dbt-debug:
	$(DBT) debug $(DBT_ARGS)
