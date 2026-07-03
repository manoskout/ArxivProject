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

import pendulum
from airflow.decorators import dag, task
from src.fetch_papers import data_fetcher
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values   
ARXIV_CATEGORIES = ["cs.LG", "cs.CL"]
POSTGRES_CONN_ID = "papers_db"


@dag(
    dag_id="arxiv_paper_pipeline",
    schedule="0 6 * * *",                      # daily, 06:00 UTC
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=True,                              # enables backfilling history
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["arxiv", "llm", "portfolio"],
)
def arxiv_paper_pipeline():

    @task
    def fetch_papers(data_interval_start=None, data_interval_end=None) -> list[dict]:
        """Query the arXiv API for papers in the run's date window.

        Use the data interval (not 'today') so backfills fetch the right days.
        Return a list of raw paper dicts (small enough for XCom; for bigger
        payloads, write to object storage and pass the path instead).
        """
        return data_fetcher()

    @task
    def clean_and_normalize(raw_papers: list) -> list:
        import pandas as pd
        
        df = pd.DataFrame(raw_papers)

        # remove any entire row where the arxiv_id, title, or abstract is missing
        df = df.dropna(subset=['arxiv_id', 'title', 'abstract'])

        # keeps the first from multiple rows with the exact same ID
        df = df.drop_duplicates(subset=['arxiv_id'])

        # normalize strings (ArXiv abstracts often contain hard line-breaks \n formatting the text for web display)
        df['abstract'] = df['abstract'].str.replace(r'\s+', ' ', regex=True).str.strip()
        df['title'] = df['title'].str.replace(r'\s+', ' ', regex=True).str.strip()

        # Convert back to list of tuples for the Postgres insert hook
        clean_papers = [tuple(x) for x in df.to_numpy()]
        return clean_papers
    

    raw = fetch_papers()
    clean = clean_and_normalize(raw)



arxiv_paper_pipeline()
