from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id='get_postgres_tables',
    start_date=datetime(2026, 7, 2),
    schedule=None, # Run manually
    catchup=False,
    tags=['database', 'postgres'],
) as dag:

    get_tables_task = SQLExecuteQueryOperator(
        task_id='list_all_tables',
        conn_id='papers_db', # The connection ID you created in the UI
        sql="""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
              AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """,
        show_return_value_in_logs=True # This forces Airflow to print the SQL results to the task log
    )