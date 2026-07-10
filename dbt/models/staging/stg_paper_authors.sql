SELECT
    paper_id,
    author_id,
    position
FROM {{ source('arxiv', 'paper_authors') }}