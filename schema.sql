-- Research Paper Intelligence Pipeline - Postgres Schema
-- Requires: CREATE EXTENSION IF NOT EXISTS vector; (pgvector, for semantic search)

CREATE EXTENSION IF NOT EXISTS vector;

-- Papers table from arXiv
CREATE TABLE papers (
    id              BIGSERIAL PRIMARY KEY,
    arxiv_id        TEXT UNIQUE NOT NULL,      -- e.g. '2301.12345v1'
    title           TEXT UNIQUE NOT NULL,             
    abstract        TEXT NOT NULL,
    published_at    TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ,
    pdf_url         TEXT,
    pdf_object_path TEXT,                      -- MinIO object path for the PDF 
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()

);


-- Index for faster queries on published_at
CREATE INDEX idx_papers_published_at ON papers (published_at);

-- Authors (many-to-many with papers)
CREATE TABLE authors (
    id      BIGSERIAL PRIMARY KEY,
    name    TEXT NOT NULL,
    UNIQUE (name)
);

-- Join table for papers and authors
CREATE TABLE paper_authors (
    paper_id    BIGINT REFERENCES papers(id) ON DELETE CASCADE,
    author_id   BIGINT REFERENCES authors(id) ON DELETE CASCADE,
    position    INT NOT NULL,                      -- author order matters in academia
    PRIMARY KEY (paper_id, author_id)
);

-- arXiv categories (many-to-many)
CREATE TABLE categories (
    id      BIGSERIAL PRIMARY KEY,
    code    TEXT UNIQUE NOT NULL,                  -- e.g. 'cs.LG'
    name    TEXT
);

-- Join table for papers and categories
CREATE TABLE paper_categories (
    paper_id    BIGINT REFERENCES papers(id) ON DELETE CASCADE,
    category_id BIGINT REFERENCES categories(id) ON DELETE CASCADE,
    is_primary  BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (paper_id, category_id)
);

-- LLM enrichment output (
--separate table = reprocessable without touching raw data
CREATE TABLE paper_enrichments (
    id              BIGSERIAL PRIMARY KEY,
    paper_id        BIGINT UNIQUE REFERENCES papers(id) ON DELETE CASCADE,
    summary         TEXT,                          -- LLM-generated TL;DR
    methods         JSONB,                         -- e.g. ["transformer", "RLHF"]
    datasets        JSONB,                         -- e.g. ["ImageNet", "MMLU"]
    custom_topics   JSONB,                         -- your own taxonomy
    model_used      TEXT NOT NULL,                 -- e.g. 'gemma-4'
    prompt_version  TEXT NOT NULL,                 -- track prompt iterations!
    enriched_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Embeddings for semantic search (pgvector)
CREATE TABLE paper_embeddings (
    paper_id    BIGINT PRIMARY KEY REFERENCES papers(id) ON DELETE CASCADE,
    embedding   vector(768),   -- gemma 768-dimensional float32 vector                   -- adjust dim to your model
    model_used  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for fast nearest neighbor search (HNSW)
CREATE INDEX idx_embeddings_hnsw ON paper_embeddings
    USING hnsw (embedding vector_cosine_ops);

-- Pipeline run metadata (great talking point: observability)
CREATE TABLE ingestion_runs (
    id              BIGSERIAL PRIMARY KEY,
    run_date        DATE NOT NULL,
    category_code   TEXT NOT NULL,
    papers_fetched  INT NOT NULL DEFAULT 0,
    papers_new      INT NOT NULL DEFAULT 0,
    status          TEXT NOT NULL,                 -- 'success' | 'failed'
    error_message   TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ
);

-- Quality checks table (for DAG testing)
CREATE TABLE quality_checks (
    id              BIGSERIAL PRIMARY KEY,
    check_name      TEXT NOT NULL,
    violations      INT NOT NULL DEFAULT 0,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);