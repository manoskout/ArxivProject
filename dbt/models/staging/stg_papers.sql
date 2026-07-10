SELECT
    id          AS paper_id,
    arxiv_id,
    title,
    abstract,
    published_at,
    ingested_at
FROM {{ source('arxiv', 'papers') }}
