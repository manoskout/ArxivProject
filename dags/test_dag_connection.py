from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.quality_checks import verify_minio_connection, QUALITY_CHECKS

def process_extracted_tables(ti):
    tables = ti.xcom_pull(task_ids='list_all_tables')
    if not tables:
        print("No tables found in the database.")
        return
    print(f"successfully extracted {len(tables)} tables")
    for t in tables:
        print(t)  # Assuming each table name is in the first column of the result
    pass


with DAG(
    dag_id='quality_checks_dag',
    start_date=datetime(2026, 7, 2),
    schedule="@daily", # Run manually
    catchup=False,
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

    process_tables_task = PythonOperator(
        task_id='process_tables',
        python_callable=process_extracted_tables
    )
    minio_connection = PythonOperator(
        task_id='verify_minio_connection',
        python_callable=verify_minio_connection
    )


    #  -- Quality checks table (for DAG testing)
    # CREATE TABLE quality_checks (
    #     id              BIGSERIAL PRIMARY KEY,
    #     check_name      TEXT NOT NULL,
    #     violations      INT NOT NULL DEFAULT 0,
    #     run_at          TIMESTAMPTZ NOT NULL DEFAULT now()
    # );   
    @task
    def run_quality_checks():
        hook = PostgresHook(postgres_conn_id='papers_db')
        failures = []
        for name, query in QUALITY_CHECKS.items():
            violations = hook.get_first(query)[0]
            # Insert the result into the quality_checks table
            insert_query = """
                INSERT INTO quality_checks (check_name, violations, run_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP);
            """
            hook.run(insert_query, parameters=(name, violations))
            if violations:
                failures.append(f"{name}: {violations}")
        if failures:
            raise ValueError("Quality checks failed: " + "; ".join(failures))

    run_quality_checks_task = run_quality_checks()
    # Execution order
    get_tables_task >> process_tables_task >> minio_connection >> run_quality_checks_task


