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
        # import arxiv  # or plain requests against the Atom API
        # window: data_interval_start -> data_interval_end
        raise NotImplementedError

    @task
    def validate(raw_papers: list[dict]) -> list[dict]:
        """Drop malformed records, dedupe by arxiv_id, normalize fields.

        Use pydantic models here — clean talking point about data quality.
        """
        raise NotImplementedError

    @task
    def load_to_postgres(papers: list[dict]) -> list[str]:
        """Upsert papers, authors, categories.

        INSERT ... ON CONFLICT (arxiv_id) DO NOTHING  -> idempotency.
        Return list of *newly inserted* arxiv_ids for downstream enrichment.
        """
        # from airflow.providers.postgres.hooks.postgres import PostgresHook
        raise NotImplementedError

    @task
    def enrich_with_llm(new_arxiv_ids: list[str]) -> list[str]:
        """For each new paper: LLM call -> summary, methods, datasets, topics.

        - Ask the model for strict JSON, validate with pydantic.
        - Store model name + prompt_version so results are reproducible.
        - Batch and rate-limit; failures here must NOT lose raw data.
        """
        raise NotImplementedError

    @task
    def compute_embeddings(new_arxiv_ids: list[str]) -> None:
        """Embed title+abstract, store in paper_embeddings (pgvector)."""
        raise NotImplementedError

    @task(trigger_rule="all_done")
    def record_run(new_arxiv_ids: list[str] | None = None) -> None:
        """Write a row to ingestion_runs (observability), runs even on failure."""
        raise NotImplementedError

    raw = fetch_papers()
    clean = validate(raw)
    new_ids = load_to_postgres(clean)
    enriched = enrich_with_llm(new_ids)
    compute_embeddings(new_ids)
    record_run(new_ids)


arxiv_paper_pipeline()
