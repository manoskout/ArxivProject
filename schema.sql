-- Research Paper Intelligence Pipeline - Postgres Schema
-- Requires: CREATE EXTENSION IF NOT EXISTS vector; (pgvector, for semantic search)

CREATE EXTENSION IF NOT EXISTS vector;

-- Core papers table (raw ingested data)
CREATE TABLE papers (
    id              BIGSERIAL PRIMARY KEY,
    arxiv_id        TEXT UNIQUE NOT NULL,          -- e.g. '2406.01234'
    title           TEXT NOT NULL,
    abstract        TEXT NOT NULL,
    published_at    TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ,
    pdf_url         TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_papers_published_at ON papers (published_at);

-- Authors (many-to-many with papers)
CREATE TABLE authors (
    id      BIGSERIAL PRIMARY KEY,
    name    TEXT NOT NULL,
    UNIQUE (name)
);

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

CREATE TABLE paper_categories (
    paper_id    BIGINT REFERENCES papers(id) ON DELETE CASCADE,
    category_id BIGINT REFERENCES categories(id) ON DELETE CASCADE,
    is_primary  BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (paper_id, category_id)
);

-- LLM enrichment output (separate table = reprocessable without touching raw data)
CREATE TABLE paper_enrichments (
    id              BIGSERIAL PRIMARY KEY,
    paper_id        BIGINT UNIQUE REFERENCES papers(id) ON DELETE CASCADE,
    summary         TEXT,                          -- LLM-generated TL;DR
    methods         JSONB,                         -- e.g. ["transformer", "RLHF"]
    datasets        JSONB,                         -- e.g. ["ImageNet", "MMLU"]
    custom_topics   JSONB,                         -- your own taxonomy
    model_used      TEXT NOT NULL,                 -- e.g. 'claude-sonnet-4-6'
    prompt_version  TEXT NOT NULL,                 -- track prompt iterations!
    enriched_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Embeddings for semantic search (pgvector)
CREATE TABLE paper_embeddings (
    paper_id    BIGINT PRIMARY KEY REFERENCES papers(id) ON DELETE CASCADE,
    embedding   vector(1536),                      -- adjust dim to your model
    model_used  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

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

-- ============================================================
-- Example analytical queries to practice (put these in a
-- /analytics folder in your repo — interviewers love this)
-- ============================================================

-- 1. Trending topics per week (window functions + JSONB)
-- SELECT
--     date_trunc('week', p.published_at) AS week,
--     topic,
--     COUNT(*) AS n_papers,
--     RANK() OVER (PARTITION BY date_trunc('week', p.published_at)
--                  ORDER BY COUNT(*) DESC) AS topic_rank
-- FROM papers p
-- JOIN paper_enrichments e ON e.paper_id = p.id,
--      jsonb_array_elements_text(e.custom_topics) AS topic
-- GROUP BY 1, 2;

-- 2. Most frequent co-author pairs (self-join)
-- SELECT a1.name, a2.name, COUNT(*) AS n_collabs
-- FROM paper_authors pa1
-- JOIN paper_authors pa2
--   ON pa1.paper_id = pa2.paper_id AND pa1.author_id < pa2.author_id
-- JOIN authors a1 ON a1.id = pa1.author_id
-- JOIN authors a2 ON a2.id = pa2.author_id
-- GROUP BY 1, 2
-- ORDER BY n_collabs DESC
-- LIMIT 20;
