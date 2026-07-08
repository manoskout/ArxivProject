
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
from openai import OpenAI

client = OpenAI(
    base_url="http://ollama:11434/v1",  
    api_key="ollama",                     
)


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
        task_id='get_non_embedded_papers',
        conn_id='papers_db',
        sql="""
            WITH ListedAuthors AS (
                SELECT 
                    pa.paper_id,
                    STRING_AGG(a.name, ', ') AS authors
                FROM 
                    paper_authors pa
                INNER JOIN 
                    authors a ON pa.author_id = a.id
                GROUP BY 
                    pa.paper_id
            )
            SELECT 
                p.id,
                p.title,
                p.abstract,
                la.authors
            FROM 
                papers p
            INNER JOIN 
                ListedAuthors la ON p.id = la.paper_id
            WHERE p.pdf_object_path IS NOT NULL;
        """,
        show_return_value_in_logs=True
    )
    
    @task
    def get_embeddings(papers: list[tuple]):
        """
        Fetch embeddings for the given papers using OpenAI API.
        """
        if not papers:
            print("No papers to fetch embeddings for.")
            return
        data = []
        for paper in papers:
            print(paper)
            paper_id, title, abstract, authors = paper
            
            text = f"{title}\n{authors}\n{abstract}"
            print(f"Fetching embeddings for -- {title} ...")
            response = client.embeddings.create(
                input=text,
                model="embeddinggemma"
            )
            print(response.data[0].embedding)
            data.append((paper_id, response.data[0].embedding, "embeddinggemma"))
        print(f"Embeddings fetched for {len(papers)} papers.")
        # pass

        hook = PostgresHook(postgres_conn_id="papers_db")
        conn = hook.get_conn()
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO paper_embeddings (paper_id, embedding, model_used)
            VALUES %s
            ON CONFLICT (paper_id) DO UPDATE SET
                embedding = EXCLUDED.embedding,
                model_used = EXCLUDED.model_used,
                created_at = CURRENT_TIMESTAMP;
        """
        execute_values(cursor, insert_query, data)
        conn.commit()
        cursor.close()
        conn.close()

    embeddings = get_embeddings(get_papers_task.output)

    get_papers_task >> embeddings