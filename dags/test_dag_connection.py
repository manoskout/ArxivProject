from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator 
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def process_extracted_tables(ti):
    tables = ti.xcom_pull(task_ids='list_all_tables')
    if not tables:
        print("No tables found in the database.")
        return
    print(f"successfully extracted {len(tables)} tables")
    for t in tables:
        print(t)  # Assuming each table name is in the first column of the result
    pass

def verify_minio_connection():
    s3_hook = S3Hook(aws_conn_id='minio')
    test_bucket = 'arxiv-raw-pdfs'
    
    if not s3_hook.check_for_bucket(test_bucket):
        print(f"Bucket '{test_bucket}' not found. Creating it now...")
        s3_hook.create_bucket(bucket_name=test_bucket)
        print("Bucket created successfully!")
    else:
        print(f"Bucket '{test_bucket}' already exists.")
        
    client = s3_hook.get_conn()
    response = client.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    
    print(f"Successfully connected! Current buckets in MinIO: {buckets}")

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

    process_tables_task = PythonOperator(
        task_id='process_tables',
        python_callable=process_extracted_tables
    )
    verify_minio_connection = PythonOperator(
        task_id='verify_minio_connection',
        python_callable=verify_minio_connection
    )

    

    # Execution order
    get_tables_task >> process_tables_task >> verify_minio_connection

