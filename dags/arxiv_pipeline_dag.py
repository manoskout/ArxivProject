"""
Research Paper Intelligence Pipeline.
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
import json
import pandas as pd

ARXIV_CATEGORIES = ["cs.LG", "cs.CL"]
POSTGRES_CONN_ID = "papers_db"
BUCKET = "arxiv-papers"


with DAG(
    dag_id='arxiv_paper_pipeline',
    start_date= pendulum.datetime(2026, 6, 1, tz="UTC"),
    schedule='@daily',
    catchup=True,
    max_active_runs=1,
    default_args={
        'retries': 3,
        'retry_delay': pendulum.duration(minutes=5),
        'retry_exponential_backoff': True,
    },
    tags=['arxiv'],
) as dag:

    # create staging table for the day's papers
    create_staging = SQLExecuteQueryOperator(
        task_id="create_staging_table",
        conn_id="papers_db",
        sql="""
            -- safely cleaning/dropping any existing staging table
            DROP TABLE IF EXISTS staging_papers;
            
            -- creating a new staging table
            CREATE TABLE staging_papers (
                id BIGSERIAL PRIMARY KEY,
                arxiv_id TEXT UNIQUE NOT NULL,
                title TEXT,
                authors TEXT[],        
                abstract TEXT,
                primary_category VARCHAR,
                all_categories TEXT[], 
                -- group_name VARCHAR,
                pdf_url TEXT,
                published_at TIMESTAMP,
                updated_at TIMESTAMP
            );
        """
    )
    distribute_data = SQLExecuteQueryOperator(
            task_id="distribute_data",
            conn_id="papers_db",
            sql= "src/scripts/data_distribution.sql"
    )

    @task
    def fetch_papers(data_interval_start=None, data_interval_end=None) -> list[dict]:
        """Query the arXiv API for papers in the run's date window.

        Use the data interval (not 'today') so backfills fetch the right days.
        Return a list of raw paper dicts (small enough for XCom; for bigger
        payloads, write to object storage and pass the path instead).
        """
        return data_fetcher()

    


    # Data Lake architecture (bronze/raw layer)
    #   - save the raw data to MinIO 
    #   - long-term storage 
    #   - reproducibility

    @task
    def save_raw_to_minio(raw_papers: list, ds: str = None):
        if not raw_papers:
            print("No papers fetched; skipping MinIO save.")
            return
        
        json_data = json.dumps(raw_papers, default=str)
        s3_hook = S3Hook(aws_conn_id="minio") 
        
        bucket_name = "arxiv-papers"
        object_key = f"raw_json/arxiv_papers_{ds}.json" 
        validate_bucket(bucket_name, s3_hook)

        s3_hook.load_string(
            string_data=json_data,
            key=object_key,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"Saved raw papers to MinIO at {bucket_name}/{object_key}")
    
    @task
    def clean_and_normalize(ds: str = None) -> list:
        bucket_name = BUCKET
        object_key = f"raw_json/arxiv_papers_{ds}.json"
        s3_hook = S3Hook(aws_conn_id="minio")
        if not s3_hook.check_for_key(key=object_key, bucket_name=bucket_name):
            print(f"No raw data found in MinIO at {bucket_name}/{object_key}; skipping cleaning.")
            return []
        print(f"Reading raw papers from MinIO at {bucket_name}/{object_key}")
        raw_data = s3_hook.read_key(key=object_key, bucket_name=bucket_name)
        raw_papers = json.loads(raw_data)

        if not raw_papers:
            print("No papers fetched; skipping cleaning.")
            return []
        
        df= pd.DataFrame(raw_papers)
        # remove any entire row where the arxiv_id, title, or abstract is missing
        df = df.dropna(subset=['arxiv_id', 'title', 'abstract'])
        # keeps the first from multiple rows with the exact same ID
        df = df.drop_duplicates(subset=['arxiv_id'])
        # normalize strings (ArXiv abstracts often contain hard line-breaks \n formatting the text for web display)
        df['abstract'] = df['abstract'].str.replace(r'\s+', ' ', regex=True).str.strip()
        df['title'] = df['title'].str.replace(r'\s+', ' ', regex=True).str.strip()
        ordered_columns = [
            'arxiv_id', 'title', 'authors', 'abstract', 
            'primary_category', 'all_categories', 'pdf_url',
            'published_at', 'updated_at'
        ]
        df = df[ordered_columns]
        # list of tuples for the Postgres insert hook
        return [tuple(x) for x in df.to_numpy()]
    
    

    @task
    def staging_insert(clean_papers: list):
        

        # Connect to the Postgres database using the Airflow connection
        hook = PostgresHook(postgres_conn_id="papers_db")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Define the insert query
        insert_query = """
            INSERT INTO staging_papers (
                arxiv_id,
                title,
                authors,        
                abstract,
                primary_category,
                all_categories, 
                -- group_name,
                pdf_url,
                published_at,
                updated_at 
            ) VALUES %s
        """

        # Use execute_values for efficient bulk insert
        execute_values(cursor, insert_query, clean_papers)

        # Commit the transaction and close the connection
        conn.commit()
        cursor.close()
        conn.close()

        # TODO: It stores only the successful insert count, but we should also log the total fetched count for completeness.
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def log_ingestion_success(**context):
        hook = PostgresHook(postgres_conn_id="papers_db")

        run_date = context['ds']
        started_at = context['dag_run'].start_date

        fetched_cnt = hook.get_first("SELECT COUNT(*) FROM staging_papers;")[0]
        new_cnt = hook.get_first(
            "SELECT COUNT(*) FROM papers WHERE DATE(ingested_at) = CURRENT_DATE;")[0]


        insert_sql = """
            INSERT INTO ingestion_runs (
                run_date, category_code, papers_fetched, papers_new, 
                status, error_message, started_at, finished_at
            ) VALUES (%s, %s, %s, %s, %s, NULL, %s, CURRENT_TIMESTAMP);
        """

        hook.run(insert_sql, parameters=(
            run_date, 'arxiv_batch', fetched_cnt, new_cnt, 'success', started_at
        ))
        print(f"Logged Success: {new_cnt} new papers.")

    raw = fetch_papers()
    minio_save = save_raw_to_minio(raw)
    clean = clean_and_normalize()
    load_to_stage = staging_insert(clean) 
    logging = log_ingestion_success()  


    create_staging >> raw >> minio_save >> clean >> load_to_stage >> distribute_data >> logging
  
