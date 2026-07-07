"""
Research Paper Download.
Daily pipeline:
    fetch_papers
    Validate 
    Load_to_postgres 
    Enrich_with_llm 
    Compute_embeddings 
    Record_run

"""

from __future__ import annotations
from airflow import DAG
import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from src.fetch_papers import data_fetcher
from src.minio_functions import validate_bucket
from psycopg2.extras import execute_values
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import ShortCircuitOperator
import json
import pandas as pd
import requests
import time

def _has_papers(ti):
    papers = ti.xcom_pull(task_ids='get_papers_to_download')
    if not papers:
        print("No papers found to download.")
        return False
    print(f"Found {len(papers)} papers to download.")
    return True

with DAG(
    dag_id='arxiv_paper_pdf_download',
    start_date= pendulum.datetime(2026, 6, 1, tz="UTC"),
    schedule='@hourly',
    catchup=True,
    max_active_runs=1,
    default_args={
        'retries': 3,
        'retry_delay': pendulum.duration(minutes=5),
        'retry_exponential_backoff': True,
    },
    tags=['arxiv', 'download'],
) as dag:

    get_papers_task = SQLExecuteQueryOperator(
        task_id='get_papers_to_download',
        conn_id='papers_db',
        sql="""
            SELECT arxiv_id, pdf_url
            FROM papers
            WHERE pdf_object_path IS NULL;  -- Only fetch papers that haven't been downloaded yet
        """,
        show_return_value_in_logs=True
    )
    # check if there are papers to download, we can use ShortCircuitOperator
    check_papers_task = ShortCircuitOperator(
        task_id="check_papers_exist",
        python_callable=_has_papers,
    )

    @task
    def download_pdfs(papers):
        if not papers:
            print("No papers to download.")
            return
        
        s3_hook = S3Hook(aws_conn_id='minio')
        client = s3_hook.get_conn()
        
        for paper in papers:
            arxiv_id, pdf_url = paper
            response = requests.get(pdf_url)
            time.sleep(3)  
            if response.status_code == 200:
                object_path = f"arxiv_pdfs/{arxiv_id}.pdf"
                client.put_object(Bucket='arxiv-papers', Key=object_path, Body=response.content)
                print(f"Downloaded and uploaded {arxiv_id} to MinIO at {object_path}")
            else:
                print(f"Failed to download {arxiv_id} from {pdf_url}")
        
    @task
    def update_pdf_paths(papers):
        if not papers:
            print("No papers to update.")
            return
        
        hook = PostgresHook(postgres_conn_id="papers_db")
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        for paper in papers:
            arxiv_id, _ = paper
            object_path = f"arxiv_pdfs/{arxiv_id}.pdf"
            cursor.execute(
                "UPDATE papers SET pdf_object_path = %s WHERE arxiv_id = %s",
                (object_path, arxiv_id)
            )
        
        conn.commit()
        cursor.close()
        conn.close()
    download_pdfs_task = download_pdfs(get_papers_task.output)
    update_pdf_paths_task = update_pdf_paths(get_papers_task.output)
get_papers_task >> check_papers_task >> download_pdfs_task >> update_pdf_paths_task
