
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
import openapi



with DAG(
    dag_id='arxiv_paper_embeddings_dag',
    start_date= pendulum.datetime(2026, 6, 1, tz="UTC"),
    schedule="0 */6 * * *", # Runs every 6 hours
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
            SELECT id, title, abstract
            FROM papers
            WHERE pdf_object_path IS NULL;  -- Only fetch papers that haven't been downloaded yet
        """,
        show_return_value_in_logs=True
    )
    
    @task
    def get_embeddings(papers: list[dict]) -> list[dict]:
        """
        Fetch embeddings for the given papers using OpenAI API.
        """
        pass
