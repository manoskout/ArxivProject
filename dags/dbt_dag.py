import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

DBT_CMD = "dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"


@dag(
    dag_id="dbt_transform",
    schedule="0 8 * * *",           # 08:00 UTC — after ingestion (06:00) and its downstream tasks
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,                  # no point backfilling transforms; each run rebuilds everything
    default_args={"retries": 1},
    tags=["dbt", "analytics"],
)
def dbt_transform():
    BashOperator(
        task_id="dbt_build",
        bash_command=DBT_CMD,
    )


dbt_transform()