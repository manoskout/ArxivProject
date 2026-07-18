WITH papers_daily AS (
    SELECT ingested_at::date AS day, COUNT(*) AS papers_ingested
    FROM {{ ref('stg_papers') }}
    GROUP BY 1
),

embeddings_daily AS (
    SELECT created_at::date AS day, COUNT(*) AS papers_embedded
    FROM {{ source('arxiv', 'paper_embeddings') }}
    GROUP BY 1
),

enrichments_daily AS (
    SELECT enriched_at::date AS day, COUNT(*) AS papers_enriched
    FROM {{ source('arxiv', 'paper_enrichments') }}
    GROUP BY 1
)

SELECT
    COALESCE(p.day, e.day, en.day)      AS day,
    COALESCE(p.papers_ingested, 0)      AS papers_ingested,
    COALESCE(e.papers_embedded, 0)      AS papers_embedded,
    COALESCE(en.papers_enriched, 0)     AS papers_enriched
FROM papers_daily p
-- full outer join keeps every fay from either side, filling the absences with nulls, which we then coalesce to 0
FULL OUTER JOIN embeddings_daily  e  ON e.day  = p.day
FULL OUTER JOIN enrichments_daily en ON en.day = COALESCE(p.day, e.day)
ORDER BY day